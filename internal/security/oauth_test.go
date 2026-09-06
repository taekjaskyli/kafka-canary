//
// Copyright Strimzi authors.
// License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
//

//go:build unit_test

package security

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

func TestOAuthTokenProviderClientCredentials(t *testing.T) {
	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.Method != http.MethodPost {
			t.Errorf("method = %s", r.Method)
		}
		if r.Header.Get("Content-Type") != "application/x-www-form-urlencoded" {
			t.Errorf("content-type = %s", r.Header.Get("Content-Type"))
		}
		user, pass, ok := r.BasicAuth()
		if !ok || user != "canary" || pass != "s3cret" {
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
		if r.Form.Get("scope") != "openid" {
			t.Errorf("scope = %s", r.Form.Get("scope"))
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token": "tok-1",
			"expires_in":   3600,
			"token_type":   "Bearer",
		})
	}))
	defer srv.Close()

	p := newOAuthTokenProvider(srv.URL, "canary", "s3cret", "openid")
	tok, err := p.Token()
	if err != nil {
		t.Fatalf("Token: %v", err)
	}
	if tok.Token != "tok-1" {
		t.Fatalf("token = %q", tok.Token)
	}

	tok2, err := p.Token()
	if err != nil {
		t.Fatalf("cached Token: %v", err)
	}
	if tok2.Token != "tok-1" {
		t.Fatalf("cached token = %q", tok2.Token)
	}
	if requests.Load() != 1 {
		t.Fatalf("token endpoint calls = %d, want 1", requests.Load())
	}
}

func TestOAuthTokenProviderRefreshesAfterExpiry(t *testing.T) {
	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := requests.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token": "tok-" + strconv.Itoa(int(n)),
			"expires_in":   3600,
		})
	}))
	defer srv.Close()

	p := newOAuthTokenProvider(srv.URL, "canary", "s3cret", "")
	if _, err := p.Token(); err != nil {
		t.Fatalf("Token: %v", err)
	}
	p.expiry = time.Now().Add(-time.Second)
	tok, err := p.Token()
	if err != nil {
		t.Fatalf("refreshed Token: %v", err)
	}
	if tok.Token != "tok-2" {
		t.Fatalf("token = %q, want tok-2", tok.Token)
	}
	if requests.Load() != 2 {
		t.Fatalf("token endpoint calls = %d, want 2", requests.Load())
	}
}

func TestOAuthTokenProviderHTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":"invalid_client"}`))
	}))
	defer srv.Close()

	p := newOAuthTokenProvider(srv.URL, "canary", "wrong", "")
	tok, err := p.Token()
	if err == nil {
		t.Fatal("expected error")
	}
	if tok != nil {
		t.Fatalf("token = %#v", tok)
	}
}

func TestOAuthTokenProviderEmptyAccessToken(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"expires_in":3600}`))
	}))
	defer srv.Close()

	p := newOAuthTokenProvider(srv.URL, "canary", "s3cret", "")
	if _, err := p.Token(); err == nil {
		t.Fatal("expected error")
	}
}
