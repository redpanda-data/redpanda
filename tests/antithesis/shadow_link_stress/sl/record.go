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
	"fmt"
	"hash/crc32"
	"strconv"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
)

// slMagic tags every value this workload produces. A reader recovers the
// guarantee that a record's contents are ours from the record itself: the
// magic proves provenance, the trailing CRC proves the bytes survived the
// round trip (source log -> shadow fetch -> target log -> consumer) intact,
// the key echoes the value's identity so a torn-apart key/value pair is
// caught, and the embedded topic and partition prove the record was served
// from where it was written — on either cluster.
const slMagic = "SLS1"

// makeRecord builds a self-describing record. The layout is:
//
//	key   = MAGIC:<nonce-hex>:<seq>
//	value = MAGIC:<nonce-hex>:<seq>:<topic>:<partition>:<payload-hex>:<crc-hex>
//
// where the CRC covers everything before it. Topic names must not contain
// ':' (ours are tsv2/cloud/local). Topic and Partition are set so the manual
// partitioner honours them and the value's claim matches where it lands.
func makeRecord(nonce uint64, seq int, topic string, part int32) *kgo.Record {
	key := fmt.Sprintf("%s:%016x:%d", slMagic, nonce, seq)
	body := fmt.Sprintf("%s:%s:%d:%x", key, topic, part, randN(1<<30))
	crc := crc32.ChecksumIEEE([]byte(body))
	return &kgo.Record{
		Key:       []byte(key),
		Value:     fmt.Appendf(nil, "%s:%08x", body, crc),
		Topic:     topic,
		Partition: part,
	}
}

// parsedRecord is the decoded identity of a workload record.
type parsedRecord struct {
	nonce uint64
	seq   int64
}

// parseRecord checks that r, read from readTopic/readPart, is a well-formed,
// intact record this workload produced there, and returns its decoded
// identity. The reason is empty when the record checks out, otherwise it
// describes the first problem found.
func parseRecord(r *kgo.Record, readTopic string, readPart int32) (parsedRecord, string) {
	s := string(r.Value)
	// The CRC is the final colon-separated field; it is hex, so the last
	// colon always separates body from CRC even though the payload is free-form.
	i := strings.LastIndexByte(s, ':')
	if i < 0 {
		return parsedRecord{}, "value has no crc field"
	}
	body, crcStr := s[:i], s[i+1:]
	crc, err := strconv.ParseUint(crcStr, 16, 32)
	if err != nil {
		return parsedRecord{}, "crc is not hex"
	}
	if uint32(crc) != crc32.ChecksumIEEE([]byte(body)) {
		return parsedRecord{}, "crc mismatch (corrupt bytes)"
	}

	// body = MAGIC:<nonce>:<seq>:<topic>:<partition>:<payload>
	f := strings.SplitN(body, ":", 6)
	if len(f) != 6 || f[0] != slMagic {
		return parsedRecord{}, "bad magic or layout"
	}
	nonce, err := strconv.ParseUint(f[1], 16, 64)
	if err != nil {
		return parsedRecord{}, "nonce is not hex"
	}
	seq, err := strconv.ParseInt(f[2], 10, 64)
	if err != nil {
		return parsedRecord{}, "seq is not an int"
	}
	if f[3] != readTopic {
		return parsedRecord{}, fmt.Sprintf("topic mismatch: value claims %q, read from %q", f[3], readTopic)
	}
	part, err := strconv.ParseInt(f[4], 10, 32)
	if err != nil {
		return parsedRecord{}, "partition is not an int"
	}
	if int32(part) != readPart {
		return parsedRecord{}, fmt.Sprintf("partition mismatch: value claims %d, read from %d", part, readPart)
	}
	wantKey := f[0] + ":" + f[1] + ":" + f[2]
	if string(r.Key) != wantKey {
		return parsedRecord{}, fmt.Sprintf("key/value mismatch: key=%q, value embeds %q", r.Key, wantKey)
	}
	return parsedRecord{nonce: nonce, seq: seq}, ""
}

// verifyResult summarises a scan of a partition's records.
type verifyResult struct {
	bad         int   // records that failed an integrity/provenance check
	reordered   int   // records out of their producer's sequence order
	firstOff    int64 // offset of the first bad/reordered record, -1 if none
	firstReason string
}

// verifyRecords scans recs (in the offset order the broker served them) and
// checks that every record is intact and belongs to this workload
// (parseRecord), and that each producer's records appear in strictly
// increasing sequence order. A producer writes its batch in seq order and
// the idempotent producer preserves per-partition order; shadow replication
// copies offsets verbatim, so the property must hold on both clusters.
func verifyRecords(topic string, part int32, recs []*kgo.Record) verifyResult {
	res := verifyResult{firstOff: -1}
	lastSeq := map[uint64]int64{}
	for _, r := range recs {
		p, reason := parseRecord(r, topic, part)
		reordered := false
		if reason == "" {
			if prev, ok := lastSeq[p.nonce]; ok && p.seq <= prev {
				reordered = true
				reason = fmt.Sprintf("producer %016x out of order: seq %d at or after %d", p.nonce, p.seq, prev)
			} else {
				lastSeq[p.nonce] = p.seq
			}
		}
		if reason != "" {
			if reordered {
				res.reordered++
			} else {
				res.bad++
			}
			if res.firstReason == "" {
				res.firstOff, res.firstReason = r.Offset, reason
			}
		}
	}
	return res
}

// offsets extracts the offsets of recs in order.
func offsets(recs []*kgo.Record) []int64 {
	offs := make([]int64, len(recs))
	for i, r := range recs {
		offs[i] = r.Offset
	}
	return offs
}
