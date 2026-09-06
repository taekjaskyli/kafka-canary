//
// Copyright Strimzi authors.
// License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
//

package service_manager

import (
	"bufio"
	"bytes"
	"io"
	"log"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/taekjaskyli/kafka-canary/internal/config"
)

// Implementation of Service Manager
type ServiceManager struct {
	CanaryConfig
	Paths
	canaryCmd *exec.Cmd
}

// Configurations of canary that is manipulated in e2e tests
type CanaryConfig struct {
	ReconcileIntervalTime string
	TopicTestName         string
	KafkaBrokerAddress    string
}

// paths to services (Kafka Compose file and Canary application main method)
type Paths struct {
	pathDockerComposeKafka string
	pathToCanaryMain       string
}

const (
	canaryTestTopicName         = "kafka-canary-test"
	kafkaBrokerAddress          = "127.0.0.1:9092"
	canaryReconcileIntervalTime = "1000"

	pathToDockerComposeImage = "compose-kafka.yaml"
	pathToMainMethod         = "../cmd/main.go"
)

func (c *ServiceManager) StartKafkaBroker() {
	log.Println("Starting Kafka")

	errComposingContainers := c.dockerCompose("start Kafka broker using docker compose", "up", "-d")
	if errComposingContainers != nil {
		log.Fatal(errComposingContainers.Error())
	}
	log.Println("Kafka container created")
	c.waitForBroker()
}

func (c *ServiceManager) StopKafkaBroker() {
	log.Println("Stopping Kafka")
	errStoppingContainers := c.dockerCompose("stop Kafka broker using docker compose", "down")
	if errStoppingContainers != nil {
		log.Fatal(errStoppingContainers.Error())
	}
}

func CreateManager() *ServiceManager {
	manager := &ServiceManager{}

	manager.ReconcileIntervalTime = canaryReconcileIntervalTime
	manager.TopicTestName = canaryTestTopicName
	manager.pathToCanaryMain = pathToMainMethod
	manager.pathDockerComposeKafka = pathToDockerComposeImage
	manager.KafkaBrokerAddress = kafkaBrokerAddress
	return manager
}

func (c *ServiceManager) StartCanary() {
	log.Println("Starting Canary")
	c.setUpCanaryParamsViaEnv()
	var wg sync.WaitGroup
	wg.Add(1)

	c.canaryCmd = exec.Command("go", "run", c.pathToCanaryMain)
	c.canaryCmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	stderr, err := c.canaryCmd.StderrPipe()
	if err != nil {
		log.Fatalf("could not get stderr pipe: %v", err)
	}
	stdout, err := c.canaryCmd.StdoutPipe()
	if err != nil {
		log.Fatalf("could not get stdout pipe: %v", err)
	}

	var readyOnce sync.Once
	go func() {
		merged := io.MultiReader(stderr, stdout)
		scanner := bufio.NewScanner(merged)
		for scanner.Scan() {
			msg := scanner.Text()
			log.Println("[canary]", msg)
			if strings.Contains(msg, "Starting canary manager") {
				readyOnce.Do(wg.Done)
			}
		}
	}()

	if err := c.canaryCmd.Start(); err != nil {
		log.Fatal(err.Error())
	}

	if waitTimeout(&wg, time.Second*30) {
		c.StopCanary()
		log.Fatal("canary failed to start within allowed time")
	}
	log.Println("Canary is ready")
}

func (c *ServiceManager) StopCanary() {
	if c.canaryCmd == nil || c.canaryCmd.Process == nil {
		return
	}
	log.Println("Stopping Canary")
	_ = syscall.Kill(-c.canaryCmd.Process.Pid, syscall.SIGTERM)
	done := make(chan struct{})
	go func() {
		_, _ = c.canaryCmd.Process.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		_ = syscall.Kill(-c.canaryCmd.Process.Pid, syscall.SIGKILL)
		_, _ = c.canaryCmd.Process.Wait()
	}
}

// per se it means waiting for container's broker to communicate correctly
func (c *ServiceManager) waitForBroker() {
	log.Println("start waiting for broker")
	timeout := time.After(120 * time.Second)
	brokerIsReadyChannel := make(chan bool)

	go func() {
		configuration := sarama.NewConfig()
		brokers := []string{c.KafkaBrokerAddress}

		for {
			// if we can create Cluster Admin, broker can communicate
			if _, err := sarama.NewClusterAdmin(brokers, configuration); err != nil {
				log.Println("waiting for broker's start")
				time.Sleep(time.Millisecond * 500)
				continue
			}
			break
		}

		brokerIsReadyChannel <- true
	}()

	select {
	case <-timeout:
		log.Println("Broker isn't ready within expected timeout")
		errObtainingLogs := c.dockerCompose("obtain logs from Kafka container", "logs")
		if errObtainingLogs != nil {
			log.Println("Problem obtaining logs from Kafka container")
			log.Fatal(errObtainingLogs.Error())
		}
		log.Fatal("containers are not in suitable state")
	case <-brokerIsReadyChannel:
		log.Println("Container (Broker) is ready")
	}
}

func (c *ServiceManager) dockerCompose(commandDescription string, composeArgs ...string) error {
	args := append([]string{"compose", "-f", c.pathDockerComposeKafka}, composeArgs...)
	return c.executeCmdWithLogging(commandDescription, "docker", args...)
}

func (c *ServiceManager) executeCmdWithLogging(commandDescription, commandName string, commandArgs ...string) error {

	cmd := exec.Command(commandName, commandArgs...)
	var stdout, stderr bytes.Buffer
	// redirect Stdout and Stderr of command to buffers
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	// execute command
	err := cmd.Run()
	// log Stdout and Stderr from command (stored within buffer)
	outStr, errStr := string(stdout.Bytes()), string(stderr.Bytes())
	log.Printf("cmd description: %s\n", commandDescription)
	log.Printf("execute cmd: %s %s\n", commandName, strings.Join(commandArgs, " "))
	log.Printf("cmd stdout:\n%s", outStr)
	log.Printf("cmd stderr:\n%s", errStr)
	return err
}

func (c *ServiceManager) setUpCanaryParamsViaEnv() {
	log.Println("Setting up environment variables")
	os.Setenv(config.ReconcileIntervalEnvVar, c.ReconcileIntervalTime)
	os.Setenv(config.TopicEnvVar, c.TopicTestName)
	os.Setenv(config.BootstrapServersEnvVar, c.KafkaBrokerAddress)
}

func waitTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case <-c:
		return false
	case <-time.After(timeout):
		return true
	}
}
