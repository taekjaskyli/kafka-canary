//
// Copyright Strimzi authors.
// License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
//

//go:build unit_test

package config

import (
	"testing"
	"time"

	"github.com/IBM/sarama"
)

func TestApplySaramaOverridesDefault(t *testing.T) {
	cfg := sarama.NewConfig()
	libraryDial := cfg.Net.DialTimeout
	librarySession := cfg.Consumer.Group.Session.Timeout
	c := &CanaryConfig{
		SaramaProducerRetryMax:            SaramaProducerRetryMaxDefault,
		SaramaProducerRetryBackoffMs:      SaramaUnsetMsDefault,
		SaramaNetDialTimeoutMs:            SaramaUnsetMsDefault,
		SaramaNetReadTimeoutMs:            SaramaUnsetMsDefault,
		SaramaNetWriteTimeoutMs:           SaramaUnsetMsDefault,
		SaramaNetKeepAliveMs:              SaramaUnsetMsDefault,
		SaramaConsumerSessionTimeoutMs:    SaramaUnsetMsDefault,
		SaramaConsumerHeartbeatIntervalMs: SaramaUnsetMsDefault,
		SaramaMetadataRefreshFrequencyMs:  SaramaUnsetMsDefault,
		SaramaAdminTimeoutMs:              SaramaUnsetMsDefault,
	}
	ApplySaramaOverrides(cfg, c)
	if cfg.Producer.Retry.Max != 0 {
		t.Fatalf("retry max = %d", cfg.Producer.Retry.Max)
	}
	if cfg.Net.DialTimeout != libraryDial {
		t.Fatalf("dial timeout changed: %s", cfg.Net.DialTimeout)
	}
	if cfg.Consumer.Group.Session.Timeout != librarySession {
		t.Fatalf("session timeout changed: %s", cfg.Consumer.Group.Session.Timeout)
	}
}

func TestApplySaramaOverridesFromEnv(t *testing.T) {
	t.Setenv(TopicConfigEnvVar, "")
	t.Setenv(PrometheusConsantLabelsEnvVar, "")
	t.Setenv(SaramaProducerRetryMaxEnvVar, "3")
	t.Setenv(SaramaNetDialTimeoutMsEnvVar, "15000")
	t.Setenv(SaramaConsumerSessionTimeoutMsEnvVar, "45000")
	t.Setenv(SaramaConsumerHeartbeatIntervalMsEnvVar, "15000")
	t.Setenv(SaramaAdminTimeoutMsEnvVar, "10000")
	canary := NewCanaryConfig()
	if canary.SaramaProducerRetryMax != 3 {
		t.Fatalf("retry max config = %d", canary.SaramaProducerRetryMax)
	}
	cfg := sarama.NewConfig()
	ApplySaramaOverrides(cfg, canary)
	if cfg.Producer.Retry.Max != 3 {
		t.Fatalf("retry max = %d", cfg.Producer.Retry.Max)
	}
	if cfg.Net.DialTimeout != 15*time.Second {
		t.Fatalf("dial = %s", cfg.Net.DialTimeout)
	}
	if cfg.Consumer.Group.Session.Timeout != 45*time.Second {
		t.Fatalf("session = %s", cfg.Consumer.Group.Session.Timeout)
	}
	if cfg.Consumer.Group.Heartbeat.Interval != 15*time.Second {
		t.Fatalf("heartbeat = %s", cfg.Consumer.Group.Heartbeat.Interval)
	}
	if cfg.Admin.Timeout != 10*time.Second {
		t.Fatalf("admin = %s", cfg.Admin.Timeout)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

func TestLookupOptionalIntEnvInvalid(t *testing.T) {
	t.Setenv(SaramaNetDialTimeoutMsEnvVar, "nope")
	if got := lookupOptionalIntEnv(SaramaNetDialTimeoutMsEnvVar); got != SaramaUnsetMsDefault {
		t.Fatalf("got %d", got)
	}
}
