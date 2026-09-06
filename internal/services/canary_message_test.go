//
// Copyright Strimzi authors.
// License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
//

//go:build unit_test

// Package services defines an interface for canary services and related implementations
package services

import (
	"encoding/json"
	"strings"
	"testing"
	"unicode"

	"github.com/IBM/sarama"
)

func TestCanaryMessage(t *testing.T) {
	cm := CanaryMessage{
		ProducerID: "producer-id",
		MessageID:  0,
		Timestamp:  12345,
	}
	want := "{\"producerId\":\"producer-id\",\"messageId\":0,\"timestamp\":12345}"
	if cm.Json() != want {
		t.Errorf("JSON got = %s, want = %s", cm.Json(), want)
	}
}

func TestCanaryMessageEncode(t *testing.T) {
	cm := CanaryMessage{
		ProducerID: "producer-id",
		MessageID:  0,
		Timestamp:  12345,
	}
	encoder := sarama.StringEncoder(cm.Json())
	bytes, _ := encoder.Encode()

	decodedCm := NewCanaryMessage(bytes)

	if cm != decodedCm {
		t.Errorf("got = %v, want = %v", decodedCm, cm)
	}

	wrong := "{\"producerId\":\"producer-id\",\"messageId\":1,\"timestamp\":67890}"

	encoder = sarama.StringEncoder(wrong)
	bytes, _ = encoder.Encode()

	decodedCm = NewCanaryMessage(bytes)

	if cm == decodedCm {
		t.Errorf("got %v should be different from %v", decodedCm, cm)
	}
}

type sizedPayload struct {
	ProducerID string `json:"producerId"`
	MessageID  int    `json:"messageId"`
	Timestamp  int64  `json:"timestamp"`
	Payload    string `json:"payload"`
}

func TestCanaryMessageJsonSize(t *testing.T) {
	cm := CanaryMessage{
		ProducerID: "producer-id",
		MessageID:  0,
		Timestamp:  12345,
	}
	core := cm.Json()
	if len(core) != len("{\"producerId\":\"producer-id\",\"messageId\":0,\"timestamp\":12345}") {
		t.Fatalf("unpadded JSON changed: %s", core)
	}
	if got := cm.JsonSize(0); got != core {
		t.Errorf("size 0 should match Json(), got %s", got)
	}
	const wantLen = 1024
	got := cm.JsonSize(wantLen)
	if len(got) != wantLen {
		t.Fatalf("len=%d, want %d", len(got), wantLen)
	}
	var decoded sizedPayload
	if err := json.Unmarshal([]byte(got), &decoded); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if decoded.ProducerID != cm.ProducerID || decoded.MessageID != cm.MessageID || decoded.Timestamp != cm.Timestamp {
		t.Fatalf("core fields changed after sizing: %+v", decoded)
	}
	if decoded.Payload == "" || strings.Trim(decoded.Payload, alphanum) != "" {
		t.Fatalf("payload should be alphanum, got %q", decoded.Payload)
	}
	cm.MessageID = 99
	got2 := cm.JsonSize(wantLen)
	if len(got2) != wantLen {
		t.Fatalf("len=%d after messageId grew, want %d", len(got2), wantLen)
	}
}

func TestCanaryMessageJsonSizeSameKeys(t *testing.T) {
	cm := CanaryMessage{ProducerID: "producer-id", MessageID: 1, Timestamp: 12345}
	a := cm.JsonSize(1024)
	b := cm.JsonSize(1024)
	var da, db sizedPayload
	if err := json.Unmarshal([]byte(a), &da); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(b), &db); err != nil {
		t.Fatal(err)
	}
	if da.ProducerID != db.ProducerID || da.MessageID != db.MessageID || da.Timestamp != db.Timestamp {
		t.Fatalf("core keys/values should be identical: %+v vs %+v", da, db)
	}
	if da.Payload == db.Payload {
		t.Fatal("payload values should differ between messages")
	}
}

func TestCanaryMessageJsonSizeWithFiller(t *testing.T) {
	cm := CanaryMessage{ProducerID: "producer-id", MessageID: 0, Timestamp: 12345}
	const wantLen = 1024
	filler := make([]byte, wantLen)
	for i := range filler {
		filler[i] = 'A'
	}
	got := cm.jsonSizeWith(wantLen, filler)
	if len(got) != wantLen {
		t.Fatalf("len=%d, want %d", len(got), wantLen)
	}
	var decoded sizedPayload
	if err := json.Unmarshal([]byte(got), &decoded); err != nil {
		t.Fatal(err)
	}
	if strings.Trim(decoded.Payload, "A") != "" {
		t.Fatalf("expected filler prefix, got %q", decoded.Payload)
	}
}

func TestRandomAlphanumCharset(t *testing.T) {
	s := randomAlphanum(64)
	if len(s) != 64 {
		t.Fatalf("len=%d", len(s))
	}
	for _, c := range s {
		if c > unicode.MaxASCII || !(unicode.IsLetter(rune(c)) || unicode.IsDigit(rune(c))) {
			t.Fatalf("non-alphanum %q", c)
		}
	}
}
