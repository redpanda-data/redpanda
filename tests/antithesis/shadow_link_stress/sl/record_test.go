// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package main

import (
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestRecordRoundTrip(t *testing.T) {
	r := makeRecord(0xdeadbeef, 7, "tsv2", 2)
	if r.Topic != "tsv2" || r.Partition != 2 {
		t.Fatalf("record targets %s/%d, want tsv2/2", r.Topic, r.Partition)
	}
	p, reason := parseRecord(r, "tsv2", 2)
	if reason != "" {
		t.Fatalf("valid record rejected: %s", reason)
	}
	if p.nonce != 0xdeadbeef || p.seq != 7 {
		t.Fatalf("parsed identity = %x/%d, want deadbeef/7", p.nonce, p.seq)
	}
}

func TestRecordRejectsTampering(t *testing.T) {
	cases := map[string]func(*kgo.Record){
		"flipped value byte": func(r *kgo.Record) { r.Value[len(r.Value)/2] ^= 1 },
		"truncated value":    func(r *kgo.Record) { r.Value = r.Value[:len(r.Value)-3] },
		"key/value mismatch": func(r *kgo.Record) { r.Key = []byte(slMagic + ":0000000000000001:9") },
	}
	for name, tamper := range cases {
		r := makeRecord(1, 1, "cloud", 0)
		tamper(r)
		if _, reason := parseRecord(r, "cloud", 0); reason == "" {
			t.Errorf("%s: tampered record accepted", name)
		}
	}
}

func TestRecordRejectsWrongProvenance(t *testing.T) {
	r := makeRecord(1, 1, "cloud", 0)
	if _, reason := parseRecord(r, "cloud", 1); reason == "" || !strings.Contains(reason, "partition") {
		t.Errorf("wrong partition not caught (reason=%q)", reason)
	}
	if _, reason := parseRecord(r, "local", 0); reason == "" || !strings.Contains(reason, "topic") {
		t.Errorf("wrong topic not caught (reason=%q)", reason)
	}
}

func TestVerifyRecordsOrdering(t *testing.T) {
	a1 := makeRecord(0xa, 1, "local", 0)
	a2 := makeRecord(0xa, 2, "local", 0)
	b1 := makeRecord(0xb, 1, "local", 0)
	for i, r := range []*kgo.Record{a1, b1, a2} {
		r.Offset = int64(10 + i)
	}
	// Interleaved nonces in seq order: fine.
	if res := verifyRecords("local", 0, []*kgo.Record{a1, b1, a2}); res.bad != 0 || res.reordered != 0 {
		t.Fatalf("clean interleaving flagged: %+v", res)
	}
	// a2 before a1: one reorder.
	a1.Offset, a2.Offset = 12, 10
	if res := verifyRecords("local", 0, []*kgo.Record{a2, b1, a1}); res.reordered != 1 {
		t.Fatalf("reorder not flagged: %+v", res)
	}
}
