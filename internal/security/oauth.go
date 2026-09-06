//
// Copyright Strimzi authors.
// License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
//

package security

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/taekjaskyli/kafka-canary/internal/config"
)

const (
	oauthHTTPTimeout      = 10 * time.Second
	oauthMaxResponseBytes = 1 << 20
	oauthDefaultTTL       = 60 * time.Second
	oauthExpirySkew       = 30 * time.Second
)

func setOAuthConfig(canaryConfig *config.CanaryConfig, saramaConfig *sarama.Config) error {
	if canaryConfig.SASLOAuthTokenURL == "" {
		return errors.New("SASL OAuth token URL must be specified")
	}
	if canaryConfig.SASLOAuthClientID == "" {
		return errors.New("SASL OAuth client ID must be specified")
	}
	if canaryConfig.SASLOAuthClientSecret == "" {
		return errors.New("SASL OAuth client secret must be specified")
	}
	if err := validateOAuthTokenURL(canaryConfig.SASLOAuthTokenURL); err != nil {
		return err
	}

	saramaConfig.Net.SASL.Enable = true
	saramaConfig.Net.SASL.Version = sarama.SASLHandshakeV1
	saramaConfig.Net.SASL.Mechanism = sarama.SASLTypeOAuth
	saramaConfig.Net.SASL.TokenProvider = newOAuthTokenProvider(
		canaryConfig.SASLOAuthTokenURL,
		canaryConfig.SASLOAuthClientID,
		canaryConfig.SASLOAuthClientSecret,
		canaryConfig.SASLOAuthScope,
	)
	return nil
}

func validateOAuthTokenURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return errors.New("SASL OAuth token URL must be an http(s) URL")
	}
	return nil
}

type oauthTokenProvider struct {
	tokenURL     string
	clientID     string
	clientSecret string
	scope        string
	httpClient   *http.Client

	mu     sync.Mutex
	cached string
	expiry time.Time
}

func newOAuthTokenProvider(tokenURL, clientID, clientSecret, scope string) *oauthTokenProvider {
	return &oauthTokenProvider{
		tokenURL:     tokenURL,
		clientID:     clientID,
		clientSecret: clientSecret,
		scope:        scope,
		httpClient:   &http.Client{Timeout: oauthHTTPTimeout},
	}
}

func (p *oauthTokenProvider) Token() (*sarama.AccessToken, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cached != "" && time.Now().Before(p.expiry) {
		return &sarama.AccessToken{Token: p.cached}, nil
	}
	if err := p.refreshLocked(); err != nil {
		return nil, err
	}
	return &sarama.AccessToken{Token: p.cached}, nil
}

func (p *oauthTokenProvider) refreshLocked() error {
	form := url.Values{}
	form.Set("grant_type", "client_credentials")
	if p.scope != "" {
		form.Set("scope", p.scope)
	}

	req, err := http.NewRequest(http.MethodPost, p.tokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.SetBasicAuth(p.clientID, p.clientSecret)

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, oauthMaxResponseBytes))
		return fmt.Errorf("oauth token endpoint returned HTTP %d", resp.StatusCode)
	}

	var body struct {
		AccessToken string      `json:"access_token"`
		ExpiresIn   json.Number `json:"expires_in"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, oauthMaxResponseBytes)).Decode(&body); err != nil {
		return fmt.Errorf("oauth token endpoint returned invalid JSON: %w", err)
	}
	if body.AccessToken == "" {
		return errors.New("oauth token endpoint returned an empty access_token")
	}

	ttl := oauthDefaultTTL
	if secs, err := body.ExpiresIn.Int64(); err == nil && secs > 0 {
		ttl = time.Duration(secs) * time.Second
	}
	skew := oauthExpirySkew
	if ttl <= skew {
		skew = ttl / 2
	}
	p.cached = body.AccessToken
	p.expiry = time.Now().Add(ttl - skew)
	return nil
}
