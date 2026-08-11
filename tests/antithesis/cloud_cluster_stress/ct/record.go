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

// fooMagic tags every value produced to foo. Concurrent producers
// land records at offsets none of them can predict, so the offset checks alone
// cannot tell whether a record's contents are the ones we wrote. A reader
// recovers that guarantee from the record itself: the magic proves the record
// is one of ours (not garbage, a foreign topic, or a torn read), the trailing
// CRC proves the bytes survived the round trip through cloud storage intact,
// the key echoes the value's identity so a torn-apart key/value pair is caught,
// and the embedded partition proves the record was served from the partition it
// was written to.
const fooMagic = "CTS1"

// makeFooRecord builds a self-describing record for the given producer nonce,
// per-producer sequence, and target partition. The nonce keeps keys from
// concurrent producers distinct. The layout is:
//
//	key   = MAGIC:<nonce-hex>:<seq>
//	value = MAGIC:<nonce-hex>:<seq>:<partition>:<payload-hex>:<crc-hex>
//
// where the CRC covers everything before it. Partition is set so a manual
// partitioner honours it and the value's claim matches where it lands.
func makeFooRecord(nonce uint64, seq int, part int32) *kgo.Record {
	key := fmt.Sprintf("%s:%016x:%d", fooMagic, nonce, seq)
	body := fmt.Sprintf("%s:%d:%x", key, part, randN(1<<30))
	crc := crc32.ChecksumIEEE([]byte(body))
	return &kgo.Record{
		Key:       []byte(key),
		Value:     fmt.Appendf(nil, "%s:%08x", body, crc),
		Partition: part,
	}
}

// parsedFooRecord is the decoded identity of a foo record.
type parsedFooRecord struct {
	nonce uint64
	seq   int64
	part  int32
}

// parseFooRecord checks that r, read from partition readPart, is a well-formed,
// intact record this workload produced, and returns its decoded identity. The
// reason is empty when the record checks out, otherwise it describes the first
// problem found and the parsedFooRecord is zero.
func parseFooRecord(r *kgo.Record, readPart int32) (parsedFooRecord, string) {
	s := string(r.Value)
	// The CRC is the final colon-separated field; it is hex, so the last
	// colon in the value always separates body from CRC even though the
	// payload may itself contain colons.
	i := strings.LastIndexByte(s, ':')
	if i < 0 {
		return parsedFooRecord{}, "value has no crc field"
	}
	body, crcStr := s[:i], s[i+1:]
	crc, err := strconv.ParseUint(crcStr, 16, 32)
	if err != nil {
		return parsedFooRecord{}, "crc is not hex"
	}
	if uint32(crc) != crc32.ChecksumIEEE([]byte(body)) {
		return parsedFooRecord{}, "crc mismatch (corrupt bytes)"
	}

	// body = MAGIC:<nonce>:<seq>:<partition>:<payload>
	f := strings.SplitN(body, ":", 5)
	if len(f) != 5 || f[0] != fooMagic {
		return parsedFooRecord{}, "bad magic or layout"
	}
	nonce, err := strconv.ParseUint(f[1], 16, 64)
	if err != nil {
		return parsedFooRecord{}, "nonce is not hex"
	}
	seq, err := strconv.ParseInt(f[2], 10, 64)
	if err != nil {
		return parsedFooRecord{}, "seq is not an int"
	}
	part, err := strconv.ParseInt(f[3], 10, 32)
	if err != nil {
		return parsedFooRecord{}, "partition is not an int"
	}
	if int32(part) != readPart {
		return parsedFooRecord{}, fmt.Sprintf("partition mismatch: value claims %d, read from %d", part, readPart)
	}
	wantKey := f[0] + ":" + f[1] + ":" + f[2]
	if string(r.Key) != wantKey {
		return parsedFooRecord{}, fmt.Sprintf("key/value mismatch: key=%q, value embeds %q", r.Key, wantKey)
	}
	return parsedFooRecord{nonce: nonce, seq: seq, part: int32(part)}, ""
}

// verifyResult summarises a scan of a partition's records.
type verifyResult struct {
	bad         int   // records that failed an integrity/provenance check
	reordered   int   // records out of their producer's sequence order
	firstOff    int64 // offset of the first bad/reordered record, -1 if none
	firstReason string
}

// verifyFooRecords scans recs (in the offset order the broker served them) and
// checks two things: every record is intact and belongs to us (parseFooRecord),
// and each producer's records appear in strictly increasing sequence order.
// The order check holds because a producer writes its batch in seq order and
// the idempotent producer preserves per-partition order, so the seqs of any one
// nonce form an increasing subsequence within a partition; a non-increasing
// step means reordering or a duplicate. Records this reader cannot parse are
// left out of the order check.
func verifyFooRecords(part int32, recs []*kgo.Record) verifyResult {
	res := verifyResult{firstOff: -1}
	lastSeq := map[uint64]int64{}
	for _, r := range recs {
		p, reason := parseFooRecord(r, part)
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
