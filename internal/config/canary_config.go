//
// Copyright Strimzi authors.
// License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
//

// Package config defining the canary configuration parameters
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/golang/glog"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	// environment variables declaration
	BootstrapServersEnvVar                  = "KAFKA_BOOTSTRAP_SERVERS"
	BootstrapBackoffMaxAttemptsEnvVar       = "KAFKA_BOOTSTRAP_BACKOFF_MAX_ATTEMPTS"
	BootstrapBackoffScaleEnvVar             = "KAFKA_BOOTSTRAP_BACKOFF_SCALE"
	TopicEnvVar                             = "TOPIC"
	TopicConfigEnvVar                       = "TOPIC_CONFIG"
	ManageTopicEnvVar                       = "MANAGE_TOPIC"
	ReconcileIntervalEnvVar                 = "RECONCILE_INTERVAL_MS"
	ClientIDEnvVar                          = "CLIENT_ID"
	ConsumerGroupIDEnvVar                   = "CONSUMER_GROUP_ID"
	ProducerLatencyBucketsEnvVar            = "PRODUCER_LATENCY_BUCKETS"
	EndToEndLatencyBucketsEnvVar            = "ENDTOEND_LATENCY_BUCKETS"
	ExpectedClusterSizeEnvVar               = "EXPECTED_CLUSTER_SIZE"
	KafkaVersionEnvVar                      = "KAFKA_VERSION"
	SaramaLogEnabledEnvVar                  = "SARAMA_LOG_ENABLED"
	SaramaProducerRetryMaxEnvVar            = "SARAMA_PRODUCER_RETRY_MAX"
	SaramaProducerRetryBackoffMsEnvVar      = "SARAMA_PRODUCER_RETRY_BACKOFF_MS"
	SaramaNetDialTimeoutMsEnvVar            = "SARAMA_NET_DIAL_TIMEOUT_MS"
	SaramaNetReadTimeoutMsEnvVar            = "SARAMA_NET_READ_TIMEOUT_MS"
	SaramaNetWriteTimeoutMsEnvVar           = "SARAMA_NET_WRITE_TIMEOUT_MS"
	SaramaNetKeepAliveMsEnvVar              = "SARAMA_NET_KEEP_ALIVE_MS"
	SaramaConsumerSessionTimeoutMsEnvVar    = "SARAMA_CONSUMER_SESSION_TIMEOUT_MS"
	SaramaConsumerHeartbeatIntervalMsEnvVar = "SARAMA_CONSUMER_HEARTBEAT_INTERVAL_MS"
	SaramaMetadataRefreshFrequencyMsEnvVar  = "SARAMA_METADATA_REFRESH_FREQUENCY_MS"
	SaramaAdminTimeoutMsEnvVar              = "SARAMA_ADMIN_TIMEOUT_MS"
	VerbosityLogLevelEnvVar                 = "VERBOSITY_LOG_LEVEL"
	TLSEnabledEnvVar                        = "TLS_ENABLED"
	TLSCACertEnvVar                         = "TLS_CA_CERT"
	TLSClientCertEnvVar                     = "TLS_CLIENT_CERT"
	TLSClientKeyEnvVar                      = "TLS_CLIENT_KEY"
	TLSInsecureSkipVerifyEnvVar             = "TLS_INSECURE_SKIP_VERIFY"
	SASLMechanismEnvVar                     = "SASL_MECHANISM"
	SASLUserEnvVar                          = "SASL_USER"
	SASLPasswordEnvVar                      = "SASL_PASSWORD"
	SASLOAuthTokenURLEnvVar                 = "SASL_OAUTH_TOKEN_URL"
	SASLOAuthClientIDEnvVar                 = "SASL_OAUTH_CLIENT_ID"
	SASLOAuthClientSecretEnvVar             = "SASL_OAUTH_CLIENT_SECRET"
	SASLOAuthScopeEnvVar                    = "SASL_OAUTH_SCOPE"
	ConnectionCheckIntervalEnvVar           = "CONNECTION_CHECK_INTERVAL_MS"
	ConnectionCheckLatencyBucketsEnvVar     = "CONNECTION_CHECK_LATENCY_BUCKETS"
	StatusCheckIntervalEnvVar               = "STATUS_CHECK_INTERVAL_MS"
	StatusTimeWindowEnvVar                  = "STATUS_TIME_WINDOW_MS"
	DynamicConfigFileEnvVar                 = "DYNAMIC_CONFIG_FILE"
	DynamicConfigWatcherIntervalEnvVar      = "DYNAMIC_CONFIG_WATCHER_INTERVAL"
	TracingEnabledEnvVar                    = "TRACING_ENABLED"
	MessageSizeEnvVar                       = "MESSAGE_SIZE"
	PrometheusConsantLabelsEnvVar           = "PROMETHEUS_CONSTANT_LABELS"
	// default values for environment variables
	BootstrapServersDefault              = "localhost:9092"
	BootstrapBackoffMaxAttemptsDefault   = 10
	BootstrapBackoffScaleDefault         = 5000
	TopicDefault                         = "kafka-canary"
	TopicConfigDefault                   = ""
	ManageTopicDefault                   = true
	ReconcileIntervalDefault             = 30000
	ClientIDDefault                      = "kafka-canary-client"
	ConsumerGroupIDDefault               = "kafka-canary-group"
	ProducerLatencyBucketsDefault        = "2,5,10,20,50,100,200,400"
	EndToEndLatencyBucketsDefault        = "5,10,20,50,100,200,400,800"
	ExpectedClusterSizeDefault           = -1 // "dynamic" reassignment is enabled
	KafkaVersionDefault                  = "3.2.0"
	SaramaLogEnabledDefault              = false
	SaramaProducerRetryMaxDefault        = 0
	SaramaUnsetMsDefault                 = -1 // env absent: keep Sarama library default
	VerbosityLogLevelDefault             = 0  // default 0 = INFO, 1 = DEBUG, 2 = TRACE
	TLSEnabledDefault                    = false
	TLSCACertDefault                     = ""
	TLSClientCertDefault                 = ""
	TLSClientKeyDefault                  = ""
	TLSInsecureSkipVerifyDefault         = false
	SASLMechanismDefault                 = ""
	SASLUserDefault                      = ""
	SASLPasswordDefault                  = ""
	SASLOAuthTokenURLDefault             = ""
	SASLOAuthClientIDDefault             = ""
	SASLOAuthClientSecretDefault         = ""
	SASLOAuthScopeDefault                = ""
	ConnectionCheckIntervalDefault       = 120000
	ConnectionCheckLatencyBucketsDefault = "100,200,400,800,1600"
	StatusCheckIntervalDefault           = 30000
	StatusTimeWindowDefault              = 300000
	DynamicConfigFileDefault             = ""
	DynamicConfigWatcherIntervalDefault  = 30000
	TracingEnabledDefault                = false
	MessageSizeDefault                   = 0
	PrometheusConsantLabelsDefault       = ""
)

type DynamicCanaryConfig struct {
	SaramaLogEnabled  *bool `json:"saramaLogEnabled"`
	VerbosityLogLevel *int  `json:"verbosityLogLevel"`
}

// CanaryConfig defines the canary tool configuration
type CanaryConfig struct {
	DynamicCanaryConfig
	BootstrapServers                  []string
	BootstrapBackoffMaxAttempts       int
	BootstrapBackoffScale             time.Duration
	Topic                             string
	TopicConfig                       map[string]string
	ManageTopic                       bool
	ReconcileInterval                 time.Duration
	ClientID                          string
	ConsumerGroupID                   string
	ProducerLatencyBuckets            []float64
	EndToEndLatencyBuckets            []float64
	ExpectedClusterSize               int
	KafkaVersion                      string
	DynamicConfigFile                 string
	TLSEnabled                        bool
	TLSCACert                         string
	TLSClientCert                     string
	TLSClientKey                      string
	TLSInsecureSkipVerify             bool
	SASLMechanism                     string
	SASLUser                          string
	SASLPassword                      string
	SASLOAuthTokenURL                 string
	SASLOAuthClientID                 string
	SASLOAuthClientSecret             string
	SASLOAuthScope                    string
	ConnectionCheckInterval           time.Duration
	ConnectionCheckLatencyBuckets     []float64
	StatusCheckInterval               time.Duration
	StatusTimeWindow                  time.Duration
	DynamicConfigWatcherInterval      time.Duration
	SaramaProducerRetryMax            int
	SaramaProducerRetryBackoffMs      int
	SaramaNetDialTimeoutMs            int
	SaramaNetReadTimeoutMs            int
	SaramaNetWriteTimeoutMs           int
	SaramaNetKeepAliveMs              int
	SaramaConsumerSessionTimeoutMs    int
	SaramaConsumerHeartbeatIntervalMs int
	SaramaMetadataRefreshFrequencyMs  int
	SaramaAdminTimeoutMs              int
	TracingEnabled                    bool
	MessageSize                       int
	PrometheusConstantLabels          prometheus.Labels
}

func NewDynamicCanaryConfig() *DynamicCanaryConfig {
	saramaLogEnabled := lookupBoolEnv(SaramaLogEnabledEnvVar, SaramaLogEnabledDefault)
	verbosityLogLevel := lookupIntEnv(VerbosityLogLevelEnvVar, VerbosityLogLevelDefault)

	dynamicCanaryConfig := DynamicCanaryConfig{
		SaramaLogEnabled:  &saramaLogEnabled,
		VerbosityLogLevel: &verbosityLogLevel,
	}
	return &dynamicCanaryConfig
}

func (c DynamicCanaryConfig) String() string {
	commaPad := func(str string) string {
		if len(str) > 0 {
			str = str + ", "
		}
		return str
	}

	str := "{"
	if c.SaramaLogEnabled != nil {
		str = str + fmt.Sprintf("SaramaLogEnabled:%t", *c.SaramaLogEnabled)
	}
	if c.VerbosityLogLevel != nil {
		str = commaPad(str) + fmt.Sprintf("VerbosityLogLevel:%d", *c.VerbosityLogLevel)
	}
	str = str + "}"
	return str
}

// NewCanaryConfig returns an configuration instance from environment variables
func NewCanaryConfig() *CanaryConfig {
	dynamicCanaryConfig := NewDynamicCanaryConfig()

	config := CanaryConfig{
		DynamicCanaryConfig:               *dynamicCanaryConfig,
		BootstrapServers:                  strings.Split(lookupStringEnv(BootstrapServersEnvVar, BootstrapServersDefault), ","),
		BootstrapBackoffMaxAttempts:       lookupIntEnv(BootstrapBackoffMaxAttemptsEnvVar, BootstrapBackoffMaxAttemptsDefault),
		BootstrapBackoffScale:             time.Duration(lookupIntEnv(BootstrapBackoffScaleEnvVar, BootstrapBackoffScaleDefault)),
		Topic:                             lookupStringEnv(TopicEnvVar, TopicDefault),
		TopicConfig:                       convertKVPairsToMap(lookupStringEnv(TopicConfigEnvVar, TopicConfigDefault)),
		ManageTopic:                       lookupBoolEnv(ManageTopicEnvVar, ManageTopicDefault),
		ReconcileInterval:                 time.Duration(lookupIntEnv(ReconcileIntervalEnvVar, ReconcileIntervalDefault)),
		ClientID:                          lookupStringEnv(ClientIDEnvVar, ClientIDDefault),
		ConsumerGroupID:                   lookupStringEnv(ConsumerGroupIDEnvVar, ConsumerGroupIDDefault),
		ProducerLatencyBuckets:            latencyBuckets(lookupStringEnv(ProducerLatencyBucketsEnvVar, ProducerLatencyBucketsDefault)),
		EndToEndLatencyBuckets:            latencyBuckets(lookupStringEnv(EndToEndLatencyBucketsEnvVar, EndToEndLatencyBucketsDefault)),
		ExpectedClusterSize:               lookupIntEnv(ExpectedClusterSizeEnvVar, ExpectedClusterSizeDefault),
		KafkaVersion:                      lookupStringEnv(KafkaVersionEnvVar, KafkaVersionDefault),
		TLSEnabled:                        lookupBoolEnv(TLSEnabledEnvVar, TLSEnabledDefault),
		TLSCACert:                         lookupStringEnv(TLSCACertEnvVar, TLSCACertDefault),
		TLSClientCert:                     lookupStringEnv(TLSClientCertEnvVar, TLSClientCertDefault),
		TLSClientKey:                      lookupStringEnv(TLSClientKeyEnvVar, TLSClientKeyDefault),
		TLSInsecureSkipVerify:             lookupBoolEnv(TLSInsecureSkipVerifyEnvVar, TLSInsecureSkipVerifyDefault),
		SASLMechanism:                     lookupStringEnv(SASLMechanismEnvVar, SASLMechanismDefault),
		SASLUser:                          lookupStringEnv(SASLUserEnvVar, SASLUserDefault),
		SASLPassword:                      lookupStringEnv(SASLPasswordEnvVar, SASLPasswordDefault),
		SASLOAuthTokenURL:                 lookupStringEnv(SASLOAuthTokenURLEnvVar, SASLOAuthTokenURLDefault),
		SASLOAuthClientID:                 lookupStringEnv(SASLOAuthClientIDEnvVar, SASLOAuthClientIDDefault),
		SASLOAuthClientSecret:             lookupStringEnv(SASLOAuthClientSecretEnvVar, SASLOAuthClientSecretDefault),
		SASLOAuthScope:                    lookupStringEnv(SASLOAuthScopeEnvVar, SASLOAuthScopeDefault),
		ConnectionCheckInterval:           time.Duration(lookupIntEnv(ConnectionCheckIntervalEnvVar, ConnectionCheckIntervalDefault)),
		ConnectionCheckLatencyBuckets:     latencyBuckets(lookupStringEnv(ConnectionCheckLatencyBucketsEnvVar, ConnectionCheckLatencyBucketsDefault)),
		StatusCheckInterval:               time.Duration(lookupIntEnv(StatusCheckIntervalEnvVar, StatusCheckIntervalDefault)),
		StatusTimeWindow:                  time.Duration(lookupIntEnv(StatusTimeWindowEnvVar, StatusTimeWindowDefault)),
		DynamicConfigFile:                 lookupStringEnv(DynamicConfigFileEnvVar, DynamicConfigFileDefault),
		DynamicConfigWatcherInterval:      time.Duration(lookupIntEnv(DynamicConfigWatcherIntervalEnvVar, DynamicConfigWatcherIntervalDefault)),
		SaramaProducerRetryMax:            lookupIntEnv(SaramaProducerRetryMaxEnvVar, SaramaProducerRetryMaxDefault),
		SaramaProducerRetryBackoffMs:      lookupOptionalIntEnv(SaramaProducerRetryBackoffMsEnvVar),
		SaramaNetDialTimeoutMs:            lookupOptionalIntEnv(SaramaNetDialTimeoutMsEnvVar),
		SaramaNetReadTimeoutMs:            lookupOptionalIntEnv(SaramaNetReadTimeoutMsEnvVar),
		SaramaNetWriteTimeoutMs:           lookupOptionalIntEnv(SaramaNetWriteTimeoutMsEnvVar),
		SaramaNetKeepAliveMs:              lookupOptionalIntEnv(SaramaNetKeepAliveMsEnvVar),
		SaramaConsumerSessionTimeoutMs:    lookupOptionalIntEnv(SaramaConsumerSessionTimeoutMsEnvVar),
		SaramaConsumerHeartbeatIntervalMs: lookupOptionalIntEnv(SaramaConsumerHeartbeatIntervalMsEnvVar),
		SaramaMetadataRefreshFrequencyMs:  lookupOptionalIntEnv(SaramaMetadataRefreshFrequencyMsEnvVar),
		SaramaAdminTimeoutMs:              lookupOptionalIntEnv(SaramaAdminTimeoutMsEnvVar),
		TracingEnabled:                    lookupBoolEnv(TracingEnabledEnvVar, TracingEnabledDefault),
		MessageSize:                       messageSize(),
		PrometheusConstantLabels:          convertKVPairsToPrometheusLabels(lookupStringEnv(PrometheusConsantLabelsEnvVar, PrometheusConsantLabelsDefault)),
	}
	return &config
}

func lookupStringEnv(envVar string, defaultValue string) string {
	envVarValue, ok := os.LookupEnv(envVar)
	if !ok {
		return defaultValue
	}
	return envVarValue
}

func lookupIntEnv(envVar string, defaultValue int) int {
	envVarValue, ok := os.LookupEnv(envVar)
	if !ok || envVarValue == "" {
		return defaultValue
	}
	intVal, err := strconv.Atoi(envVarValue)
	if err != nil {
		glog.Warningf("%s=%q is not an integer; using %d", envVar, envVarValue, defaultValue)
		return defaultValue
	}
	return intVal
}

func lookupOptionalIntEnv(envVar string) int {
	raw, ok := os.LookupEnv(envVar)
	if !ok || raw == "" {
		return SaramaUnsetMsDefault
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		glog.Warningf("%s=%q is not an integer; ignoring", envVar, raw)
		return SaramaUnsetMsDefault
	}
	return n
}

func lookupBoolEnv(envVar string, defaultValue bool) bool {
	envVarValue, ok := os.LookupEnv(envVar)
	if !ok || envVarValue == "" {
		return defaultValue
	}
	boolVal, err := strconv.ParseBool(envVarValue)
	if err != nil {
		glog.Warningf("%s=%q is not a boolean; using %t", envVar, envVarValue, defaultValue)
		return defaultValue
	}
	return boolVal
}

func latencyBuckets(bucketsConfig string) []float64 {
	sBuckets := strings.Split(bucketsConfig, ",")
	fBuckets := make([]float64, len(sBuckets))
	for i := 0; i < len(sBuckets); i++ {
		f, err := strconv.ParseFloat(sBuckets[i], 64)
		if err != nil {
			glog.Fatalf("Error parsing buckets configuration for %s: %v", bucketsConfig, err)
		}
		fBuckets[i] = f
	}
	return fBuckets
}

func convertKVPairsToMap(kvPairsStr string) map[string]string {
	if len(kvPairsStr) == 0 {
		return nil
	}

	kvPairMap := make(map[string]string)
	kvPairs := strings.Split(kvPairsStr, ";")
	for i, kvPair := range kvPairs {
		// just exits if latest kvPair is empty (allows trailing ";")
		if i == len(kvPairs)-1 && len(kvPair) == 0 {
			break
		}
		kv := strings.Split(kvPair, "=")
		// key-value pair split has to have two fields (key has to be not empty)
		if len(kv) != 2 || len(kv[0]) == 0 {
			panic(fmt.Errorf("error parsing pair [%s]: not a valid key-value pair", kvPair))
		}
		kvPairMap[kv[0]] = kv[1]
	}
	return kvPairMap
}

func convertKVPairsToPrometheusLabels(kvPairsStr string) prometheus.Labels {

	kvPairsMap := convertKVPairsToMap(kvPairsStr)
	if kvPairsMap == nil {
		return nil
	}

	var constantLabels = make(prometheus.Labels)
	for key, value := range kvPairsMap {
		constantLabels[key] = value
	}

	return constantLabels
}

func (c CanaryConfig) String() string {

	// just using placeholders for certs/keys (content or paths)
	TLSCACert := ""
	if c.TLSCACert != "" {
		TLSCACert = "[CA cert]"
	}
	TLSClientCert := ""
	if c.TLSClientCert != "" {
		TLSClientCert = "[Client cert]"
	}
	TLSClientKey := ""
	if c.TLSClientKey != "" {
		TLSClientKey = "[Client key]"
	}

	SASLUser := ""
	SASLPassword := ""
	if c.SASLUser != "" {
		SASLUser = "[SASL user]"
	}
	if c.SASLPassword != "" {
		SASLPassword = "[SASL password]"
	}
	SASLOAuthClientSecret := ""
	if c.SASLOAuthClientSecret != "" {
		SASLOAuthClientSecret = "[OAuth client secret]"
	}

	return fmt.Sprintf("{BootstrapServers:%s, BootstrapBackoffMaxAttempts:%d, BootstrapBackoffScale:%d, Topic:%s, TopicConfig:%v, ManageTopic:%t, ReconcileInterval:%d ms, "+
		"ClientID:%s, ConsumerGroupID:%s, ProducerLatencyBuckets:%v, EndToEndLatencyBuckets:%v, ExpectedClusterSize:%d, KafkaVersion:%s,"+
		"TLSEnabled:%t, TLSCACert:%s, TLSClientCert:%s, TLSClientKey:%s, TLSInsecureSkipVerify:%t,"+
		"SASLMechanism:%s, SASLUser:%s, SASLPassword:%s, SASLOAuthTokenURL:%s, SASLOAuthClientID:%s, SASLOAuthClientSecret:%s, SASLOAuthScope:%s, "+
		"ConnectionCheckInterval:%d ms, ConnectionCheckLatencyBuckets:%v, StatusCheckInterval:%d ms, StatusTimeWindow:%d ms,"+
		"DynamicConfigFile: %s, DynamicCanaryConfig: %s, DynamicConfigWatcherInterval: %d ms, "+
		"SaramaProducerRetryMax:%d, SaramaProducerRetryBackoffMs:%d, SaramaNetDialTimeoutMs:%d, SaramaNetReadTimeoutMs:%d, SaramaNetWriteTimeoutMs:%d, SaramaNetKeepAliveMs:%d, "+
		"SaramaConsumerSessionTimeoutMs:%d, SaramaConsumerHeartbeatIntervalMs:%d, SaramaMetadataRefreshFrequencyMs:%d, SaramaAdminTimeoutMs:%d, "+
		"TracingEnabled:%t, MessageSize:%d KB}",
		c.BootstrapServers, c.BootstrapBackoffMaxAttempts, c.BootstrapBackoffScale, c.Topic, c.TopicConfig, c.ManageTopic, c.ReconcileInterval, c.ClientID, c.ConsumerGroupID,
		c.ProducerLatencyBuckets, c.EndToEndLatencyBuckets, c.ExpectedClusterSize, c.KafkaVersion,
		c.TLSEnabled, TLSCACert, TLSClientCert, TLSClientKey, c.TLSInsecureSkipVerify, c.SASLMechanism, SASLUser, SASLPassword,
		c.SASLOAuthTokenURL, c.SASLOAuthClientID, SASLOAuthClientSecret, c.SASLOAuthScope,
		c.ConnectionCheckInterval, c.ConnectionCheckLatencyBuckets, c.StatusCheckInterval, c.StatusTimeWindow,
		c.DynamicConfigFile, c.DynamicCanaryConfig, c.DynamicConfigWatcherInterval,
		c.SaramaProducerRetryMax, c.SaramaProducerRetryBackoffMs, c.SaramaNetDialTimeoutMs, c.SaramaNetReadTimeoutMs, c.SaramaNetWriteTimeoutMs, c.SaramaNetKeepAliveMs,
		c.SaramaConsumerSessionTimeoutMs, c.SaramaConsumerHeartbeatIntervalMs, c.SaramaMetadataRefreshFrequencyMs, c.SaramaAdminTimeoutMs,
		c.TracingEnabled, c.MessageSize)
}

func messageSize() int {
	raw, ok := os.LookupEnv(MessageSizeEnvVar)
	if !ok || raw == "" {
		return MessageSizeDefault
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		glog.Warningf("%s=%q is not an integer KB; using 0 (short JSON)", MessageSizeEnvVar, raw)
		return MessageSizeDefault
	}
	if n < 0 {
		glog.Warningf("%s=%d is negative; using 0 (short JSON). Minimum positive value is 1 KB", MessageSizeEnvVar, n)
		return MessageSizeDefault
	}
	return n
}

// MessageSizeBytes is the message value length. 0 leaves the short JSON.
func (c CanaryConfig) MessageSizeBytes() int {
	if c.MessageSize <= 0 {
		return 0
	}
	return c.MessageSize * 1024
}
