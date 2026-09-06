//
// Copyright Strimzi authors.
// License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
//

package services

import (
	"net/http"
	"sync/atomic"
)

var (
	producedOK atomic.Bool
	consumedOK atomic.Bool
)

// MarkProducedOK records that at least one canary record was produced.
func MarkProducedOK() {
	producedOK.Store(true)
}

// MarkConsumedOK records that at least one canary record was consumed.
func MarkConsumedOK() {
	consumedOK.Store(true)
}

// Ready is true after the first successful produce and consume.
// It stays true afterwards so a later Kafka outage does not drop the pod from Service endpoints.
func Ready() bool {
	return producedOK.Load() && consumedOK.Load()
}

func LivenessHandler() http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		rw.WriteHeader(http.StatusOK)
		rw.Write([]byte("OK"))
	})
}

func ReadinessHandler() http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		if !Ready() {
			rw.WriteHeader(http.StatusServiceUnavailable)
			rw.Write([]byte("not ready"))
			return
		}
		rw.WriteHeader(http.StatusOK)
		rw.Write([]byte("OK"))
	})
}
