//
// Copyright Strimzi authors.
// License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
//

// Package services defines an interface for canary services and related implementations
package services

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
)

const (
	payloadJSONOverhead = len(`,"payload":""`)
	alphanum            = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
)

// CanaryMessage defines the payload of a canary message
type CanaryMessage struct {
	ProducerID string `json:"producerId"`
	MessageID  int    `json:"messageId"`
	Timestamp  int64  `json:"timestamp"`
}

func NewCanaryMessage(bytes []byte) CanaryMessage {
	var cm CanaryMessage
	json.Unmarshal(bytes, &cm)
	return cm
}

func (cm CanaryMessage) Json() string {
	return cm.JsonSize(0)
}

// JsonSize is the message value. size <= 0 leaves the short JSON
// (`producerId`, `messageId`, `timestamp`). size > 0 is the exact value
// length: a `payload` field of random alphanum fills the rest.
func (cm CanaryMessage) JsonSize(size int) string {
	var filler []byte
	if size > 0 {
		filler = randomAlphanum(size)
	}
	return cm.jsonSizeWith(size, filler)
}

// jsonSizeWith stitches a pre-generated filler so the caller can RNG before
// setting Timestamp (produce/e2e latency must not include generation).
func (cm CanaryMessage) jsonSizeWith(size int, filler []byte) string {
	core, _ := json.Marshal(cm)
	if size <= 0 {
		return string(core)
	}
	padLen := size - len(core) - payloadJSONOverhead
	if len(filler) < padLen {
		n := randomAlphanum(padLen - len(filler))
		filler = append(append([]byte(nil), filler...), n...)
	}
	out := make([]byte, 0, size)
	out = append(out, core[:len(core)-1]...)
	out = append(out, `,"payload":"`...)
	out = append(out, filler[:padLen]...)
	out = append(out, '"', '}')
	return string(out)
}

func randomAlphanum(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = alphanum[rand.IntN(len(alphanum))]
	}
	return b
}

func (cm CanaryMessage) String() string {
	return fmt.Sprintf("{ProducerID:%s, MessageID:%d, Timestamp:%d}",
		cm.ProducerID, cm.MessageID, cm.Timestamp)
}
