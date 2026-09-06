//
// Copyright Strimzi authors.
// License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
//

//go:build e2e

package test

import (
	"log"
	"os"
	"testing"

	"github.com/taekjaskyli/kafka-canary/internal/config"
	"github.com/taekjaskyli/kafka-canary/internal/otlptest"
	"github.com/taekjaskyli/kafka-canary/test/service_manager"
)

var (
	serviceManager *service_manager.ServiceManager
	otlpCollector  *otlptest.Collector
)

func TestMain(m *testing.M) {
	var err error
	otlpCollector, err = otlptest.Start()
	if err != nil {
		log.Fatal(err)
	}
	os.Setenv(config.TracingEnabledEnvVar, "true")
	os.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", otlpCollector.HTTPEndpoint())
	os.Setenv("OTEL_EXPORTER_OTLP_INSECURE", "true")
	os.Setenv("OTEL_BSP_SCHEDULE_DELAY", "200")
	os.Setenv("OTEL_SERVICE_NAME", "kafka-canary-e2e")

	serviceManager = service_manager.CreateManager()
	serviceManager.StartKafkaBroker()
	serviceManager.StartCanary()

	log.Println("Starting tests")
	code := m.Run()

	serviceManager.StopCanary()
	serviceManager.StopKafkaBroker()
	otlpCollector.Stop()
	os.Exit(code)
}
