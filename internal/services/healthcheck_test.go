//go:build unit_test

package services

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func resetReadiness() {
	producedOK.Store(false)
	consumedOK.Store(false)
}

func TestLivenessAlwaysOK(t *testing.T) {
	resetReadiness()
	rr := httptest.NewRecorder()
	LivenessHandler().ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/liveness", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("liveness status %d", rr.Code)
	}
}

func TestReadinessWaitsForProduceAndConsume(t *testing.T) {
	resetReadiness()
	h := ReadinessHandler()

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/readiness", nil))
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("want 503 before produce/consume, got %d", rr.Code)
	}

	MarkProducedOK()
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/readiness", nil))
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("want 503 after produce only, got %d", rr.Code)
	}

	MarkConsumedOK()
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/readiness", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("want 200 after produce and consume, got %d", rr.Code)
	}
	body, _ := io.ReadAll(rr.Body)
	if string(body) != "OK" {
		t.Fatalf("body %q", body)
	}
}
