//
// Copyright Strimzi authors.
// License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
//

package config

import (
	"time"

	"github.com/IBM/sarama"
)

// ApplySaramaOverrides copies CanaryConfig SARAMA_* fields onto cfg.
// Values of SaramaUnsetMsDefault (-1) leave the library default in place.
// Producer retry max is always applied (canary default 0).
func ApplySaramaOverrides(cfg *sarama.Config, c *CanaryConfig) {
	cfg.Producer.Retry.Max = c.SaramaProducerRetryMax
	applyMs(c.SaramaProducerRetryBackoffMs, func(d time.Duration) { cfg.Producer.Retry.Backoff = d })
	applyMs(c.SaramaNetDialTimeoutMs, func(d time.Duration) { cfg.Net.DialTimeout = d })
	applyMs(c.SaramaNetReadTimeoutMs, func(d time.Duration) { cfg.Net.ReadTimeout = d })
	applyMs(c.SaramaNetWriteTimeoutMs, func(d time.Duration) { cfg.Net.WriteTimeout = d })
	applyMs(c.SaramaNetKeepAliveMs, func(d time.Duration) { cfg.Net.KeepAlive = d })
	applyMs(c.SaramaConsumerSessionTimeoutMs, func(d time.Duration) { cfg.Consumer.Group.Session.Timeout = d })
	applyMs(c.SaramaConsumerHeartbeatIntervalMs, func(d time.Duration) { cfg.Consumer.Group.Heartbeat.Interval = d })
	applyMs(c.SaramaMetadataRefreshFrequencyMs, func(d time.Duration) { cfg.Metadata.RefreshFrequency = d })
	applyMs(c.SaramaAdminTimeoutMs, func(d time.Duration) { cfg.Admin.Timeout = d })
}

func applyMs(ms int, set func(time.Duration)) {
	if ms < 0 {
		return
	}
	set(time.Duration(ms) * time.Millisecond)
}
