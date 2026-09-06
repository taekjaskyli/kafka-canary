//
// Copyright Strimzi authors.
// License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
//

//go:build unit_test

// Package security defining some security related tools
package security

import (
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/IBM/sarama"
	"github.com/taekjaskyli/kafka-canary/internal/config"
)

func TestNoAuth(t *testing.T) {
	canaryConfig := config.NewCanaryConfig()
	saramaConfig := sarama.NewConfig()
	e := SetAuthConfig(canaryConfig, saramaConfig)
	if e == nil {
		t.Fail()
	}
}

func TestSASLPlainAuthNoUserPassword(t *testing.T) {
	os.Setenv(config.SASLMechanismEnvVar, "PLAIN")
	canaryConfig := config.NewCanaryConfig()
	saramaConfig := sarama.NewConfig()
	e := SetAuthConfig(canaryConfig, saramaConfig)
	if e == nil {
		t.Fail()
	}
}

func TestSASLPlainAuth(t *testing.T) {
	os.Setenv(config.SASLMechanismEnvVar, "PLAIN")
	os.Setenv(config.SASLUserEnvVar, "user")
	os.Setenv(config.SASLPasswordEnvVar, "password")
	canaryConfig := config.NewCanaryConfig()
	saramaConfig := sarama.NewConfig()
	e := SetAuthConfig(canaryConfig, saramaConfig)
	if e != nil ||
		!saramaConfig.Net.SASL.Enable || saramaConfig.Net.SASL.Version != sarama.SASLHandshakeV1 ||
		saramaConfig.Net.SASL.Mechanism != sarama.SASLMechanism(canaryConfig.SASLMechanism) ||
		saramaConfig.Net.SASL.User != canaryConfig.SASLUser || saramaConfig.Net.SASL.Password != canaryConfig.SASLPassword {
		t.Fail()
	}
}

func TestSASLOAuthAuth(t *testing.T) {
	canaryConfig := &config.CanaryConfig{
		SASLMechanism:         sarama.SASLTypeOAuth,
		SASLOAuthTokenURL:     "https://idp.example.com/token",
		SASLOAuthClientID:     "canary",
		SASLOAuthClientSecret: "s3cret",
		SASLOAuthScope:        "openid",
	}
	saramaConfig := sarama.NewConfig()
	if err := SetAuthConfig(canaryConfig, saramaConfig); err != nil {
		t.Fatalf("SetAuthConfig: %v", err)
	}
	if !saramaConfig.Net.SASL.Enable ||
		saramaConfig.Net.SASL.Version != sarama.SASLHandshakeV1 ||
		saramaConfig.Net.SASL.Mechanism != sarama.SASLTypeOAuth ||
		saramaConfig.Net.SASL.TokenProvider == nil ||
		saramaConfig.Net.SASL.User != "" ||
		saramaConfig.Net.SASL.Password != "" {
		t.Fail()
	}
	p, ok := saramaConfig.Net.SASL.TokenProvider.(*oauthTokenProvider)
	if !ok {
		t.Fatal("TokenProvider is not oauthTokenProvider")
	}
	if p.tokenURL != canaryConfig.SASLOAuthTokenURL ||
		p.clientID != canaryConfig.SASLOAuthClientID ||
		p.clientSecret != canaryConfig.SASLOAuthClientSecret ||
		p.scope != canaryConfig.SASLOAuthScope {
		t.Fail()
	}
}

func TestSASLOAuthRoundTripMockIdP(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		if !ok || user != "kafka-canary" || pass != "from-secret" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if err := r.ParseForm(); err != nil {
			t.Errorf("ParseForm: %v", err)
			return
		}
		if r.Form.Get("grant_type") != "client_credentials" {
			t.Errorf("grant_type = %s", r.Form.Get("grant_type"))
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"access_token": "keycloak-shaped-token",
			"expires_in": 300,
			"refresh_expires_in": 1800,
			"token_type": "Bearer",
			"not-before-policy": 0,
			"scope": "profile email"
		}`))
	}))
	defer srv.Close()

	t.Setenv(config.SASLMechanismEnvVar, "OAUTHBEARER")
	t.Setenv(config.SASLOAuthTokenURLEnvVar, srv.URL)
	t.Setenv(config.SASLOAuthClientIDEnvVar, "kafka-canary")
	t.Setenv(config.SASLOAuthClientSecretEnvVar, "from-secret")
	t.Setenv(config.SASLOAuthScopeEnvVar, "")

	canaryConfig := config.NewCanaryConfig()
	saramaConfig := sarama.NewConfig()
	saramaConfig.Producer.Return.Successes = true
	saramaConfig.Producer.RequiredAcks = sarama.WaitForAll
	saramaConfig.Producer.Retry.Max = 0
	if err := SetAuthConfig(canaryConfig, saramaConfig); err != nil {
		t.Fatalf("SetAuthConfig: %v", err)
	}
	if err := saramaConfig.Validate(); err != nil {
		t.Fatalf("sarama Validate: %v", err)
	}
	tok, err := saramaConfig.Net.SASL.TokenProvider.Token()
	if err != nil {
		t.Fatalf("Token: %v", err)
	}
	if tok.Token != "keycloak-shaped-token" {
		t.Fatalf("token = %q", tok.Token)
	}
}

func TestSASLOAuthAuthMissingFields(t *testing.T) {
	cases := []config.CanaryConfig{
		{SASLMechanism: sarama.SASLTypeOAuth, SASLOAuthClientID: "id", SASLOAuthClientSecret: "s"},
		{SASLMechanism: sarama.SASLTypeOAuth, SASLOAuthTokenURL: "https://idp.example.com/token", SASLOAuthClientSecret: "s"},
		{SASLMechanism: sarama.SASLTypeOAuth, SASLOAuthTokenURL: "https://idp.example.com/token", SASLOAuthClientID: "id"},
		{SASLMechanism: sarama.SASLTypeOAuth, SASLOAuthTokenURL: "not-a-url", SASLOAuthClientID: "id", SASLOAuthClientSecret: "s"},
	}
	for i := range cases {
		c := cases[i]
		if err := SetAuthConfig(&c, sarama.NewConfig()); err == nil {
			t.Errorf("case %d: expected error", i)
		}
	}
}
