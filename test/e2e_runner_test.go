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

	"github.com/taekjaskyli/kafka-canary/test/service_manager"
)

var (
	serviceManager *service_manager.ServiceManager
)

func TestMain(m *testing.M) {
	serviceManager = service_manager.CreateManager()
	serviceManager.StartKafkaBroker()
	serviceManager.StartCanary()

	log.Println("Starting tests")
	code := m.Run()

	serviceManager.StopCanary()
	serviceManager.StopKafkaBroker()
	// returning exit code of testing
	os.Exit(code)
}
