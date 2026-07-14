// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// The ctc workload fuzzes compaction on a compacted cloud topic with a
// convergent key/value model. A fixed key space is split into buckets, and
// a tracker file per bucket records the last committed counter value per
// key. A producer round exclusively locks one bucket, produces the next run
// of counter values for a subset of its keys, and commits the new counters
// back to the tracker only once every record is acked. Per key, values in
// the log can regress (an uncommitted run is re-produced by a later round),
// but the latest record per key converges upward: it must carry a value at
// or above the tracker's committed one, and compaction must preserve it.
// The sweeper asserts the log-shape invariants that survive compaction
// (in-order offsets, record integrity, per-producer produce order) and that
// compaction visibly runs; holding surviving values against the tracker's
// committed counters is what the tracker exists for, and comes later.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	ctcTopic         = "ctc"
	ctcPartitions    = 3
	ctcReplicas      = 3
	ctcBuckets       = 32
	ctcKeysPerBucket = 32
	// ctcMaxStep caps how many new values one round appends per key.
	ctcMaxStep = 16
	// ctcMaxPadChunks caps the record padding (16 hex chars per chunk) that
	// varies record sizes.
	ctcMaxPadChunks = 32
	// ctcProduceBudget bounds how long one round keeps retrying before it
	// exits uncommitted; the next round for the bucket re-produces the range.
	ctcProduceBudget = 3 * time.Minute

	ctcMagic = "CTC1"
)

func ctcTrackerDir() string {
	if d := os.Getenv("CTC_TRACKER_DIR"); d != "" {
		return d
	}
	return "/var/lib/ct_stress"
}

// ctcKey is the compaction identity of logical key id. Ids map to partitions
// statically so a key's whole history lives in one partition and "latest
// record" is well defined.
func ctcKey(id int) string {
	return fmt.Sprintf("%s:%04d", ctcMagic, id)
}

func ctcPartition(id int) int32 {
	return int32(id % ctcPartitions)
}

// makeCtcRecord builds the record carrying counter value val of key id:
//
//	key   = MAGIC:<id>
//	value = MAGIC:<id>:<val>:<nonce-hex>:<partition>:<pad>:<crc-hex>
//
// following the foo record layout (see makeFooRecord): the CRC covers everything
// before it, the value echoes the key, and the embedded partition lets a
// reader verify the record was served from the partition it was written to.
// The pad varies record sizes but is derived from (nonce, id, val) rather
// than drawn from randomness, so a record is a pure function of its identity.
func makeCtcRecord(nonce uint64, id int, val int64) *kgo.Record {
	key := ctcKey(id)
	part := ctcPartition(id)
	h := nonce ^ uint64(id)*2654435761 ^ uint64(val)*0x9e3779b97f4a7c15
	pad := strings.Repeat(fmt.Sprintf("%016x", h), 1+int(h%ctcMaxPadChunks))
	body := fmt.Sprintf("%s:%d:%016x:%d:%s", key, val, nonce, part, pad)
	crc := crc32.ChecksumIEEE([]byte(body))
	return &kgo.Record{
		Key:       []byte(key),
		Value:     fmt.Appendf(nil, "%s:%08x", body, crc),
		Partition: part,
	}
}

// parsedCtcRecord is the decoded identity of a ctc record.
type parsedCtcRecord struct {
	id    int
	val   int64
	nonce uint64
}

// parseCtcRecord checks that r, read from partition readPart, is a
// well-formed, intact ctc record and returns its decoded identity. The
// reason is empty when the record checks out, otherwise it describes the
// first problem found and the parsedCtcRecord is zero.
func parseCtcRecord(r *kgo.Record, readPart int32) (parsedCtcRecord, string) {
	s := string(r.Value)
	i := strings.LastIndexByte(s, ':')
	if i < 0 {
		return parsedCtcRecord{}, "value has no crc field"
	}
	body, crcStr := s[:i], s[i+1:]
	crc, err := strconv.ParseUint(crcStr, 16, 32)
	if err != nil {
		return parsedCtcRecord{}, "crc is not hex"
	}
	if uint32(crc) != crc32.ChecksumIEEE([]byte(body)) {
		return parsedCtcRecord{}, "crc mismatch (corrupt bytes)"
	}

	// body = MAGIC:<id>:<val>:<nonce>:<partition>:<pad>
	f := strings.SplitN(body, ":", 6)
	if len(f) != 6 || f[0] != ctcMagic {
		return parsedCtcRecord{}, "bad magic or layout"
	}
	id, err := strconv.Atoi(f[1])
	if err != nil {
		return parsedCtcRecord{}, "key id is not an int"
	}
	val, err := strconv.ParseInt(f[2], 10, 64)
	if err != nil {
		return parsedCtcRecord{}, "value counter is not an int"
	}
	nonce, err := strconv.ParseUint(f[3], 16, 64)
	if err != nil {
		return parsedCtcRecord{}, "nonce is not hex"
	}
	part, err := strconv.ParseInt(f[4], 10, 32)
	if err != nil {
		return parsedCtcRecord{}, "partition is not an int"
	}
	if int32(part) != readPart {
		return parsedCtcRecord{}, fmt.Sprintf("partition mismatch: value claims %d, read from %d", part, readPart)
	}
	wantKey := f[0] + ":" + f[1]
	if string(r.Key) != wantKey {
		return parsedCtcRecord{}, fmt.Sprintf("key/value mismatch: key=%q, value embeds %q", r.Key, wantKey)
	}
	return parsedCtcRecord{id: id, val: val, nonce: nonce}, ""
}

// ctcTracker is a bucket's committed counter per key, indexed by the key's
// position within the bucket. Zero (the seed state) means nothing committed;
// produced values start at 1.
type ctcTracker struct {
	Committed []int64 `json:"committed"`
}

func ctcBucketPaths(b int) (lock, data string) {
	dir := ctcTrackerDir()
	return filepath.Join(dir, fmt.Sprintf("bucket-%02d.lock", b)),
		filepath.Join(dir, fmt.Sprintf("bucket-%02d.json", b))
}

// lockCtcBucket takes an exclusive flock on a random free bucket, probing
// each bucket at most once, and returns a nil file when all are busy. The
// lock lives on a file separate from the tracker data because the data file
// is replaced by rename on commit, which would silently detach a lock held
// on it. Closing the returned file (or the process dying) releases the lock.
func lockCtcBucket() (int, *os.File, error) {
	if err := os.MkdirAll(ctcTrackerDir(), 0o755); err != nil {
		return 0, nil, err
	}
	start := randN(ctcBuckets)
	for i := range ctcBuckets {
		b := (start + i) % ctcBuckets
		lockPath, _ := ctcBucketPaths(b)
		f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o644)
		if err != nil {
			return 0, nil, err
		}
		err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			return b, f, nil
		}
		f.Close()
		if !errors.Is(err, syscall.EWOULDBLOCK) {
			return 0, nil, err
		}
	}
	return 0, nil, nil
}

// loadCtcTracker reads the bucket's committed counters; a missing file is
// the seed state. A tracker that exists but does not decode is an error, not
// a reset: silently reseeding would lower committed values and invalidate
// everything produced so far.
func loadCtcTracker(b int) (*ctcTracker, error) {
	_, dataPath := ctcBucketPaths(b)
	raw, err := os.ReadFile(dataPath)
	if errors.Is(err, os.ErrNotExist) {
		return &ctcTracker{Committed: make([]int64, ctcKeysPerBucket)}, nil
	}
	if err != nil {
		return nil, err
	}
	var t ctcTracker
	if err := json.Unmarshal(raw, &t); err != nil {
		return nil, fmt.Errorf("tracker %s corrupt: %w", dataPath, err)
	}
	if len(t.Committed) != ctcKeysPerBucket {
		return nil, fmt.Errorf("tracker %s has %d keys, want %d", dataPath, len(t.Committed), ctcKeysPerBucket)
	}
	return &t, nil
}

// saveCtcTracker atomically replaces the bucket's tracker file. Must be
// called with the bucket lock held.
func saveCtcTracker(b int, t *ctcTracker) error {
	_, dataPath := ctcBucketPaths(b)
	raw, err := json.Marshal(t)
	if err != nil {
		return err
	}
	tmp := dataPath + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(raw); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, dataPath)
}

// parallel_driver_produce_ctc: advance one tracker bucket. Locks a random
// free bucket, produces the next run of counter values for a random subset
// of its keys, and commits the new counters only after every record is
// acked. The round keeps one client — one idempotent producer session, so
// per-partition order holds across its retries — and retries to ack within
// a budget: it either commits having observed every ack, or exits with the
// tracker untouched and the next round for the bucket re-produces the same
// values.
func produceCtc() error {
	b, lock, err := lockCtcBucket()
	if err != nil {
		return err
	}
	if lock == nil {
		fmt.Println("ctc: all buckets locked by other producers; skipping")
		return nil
	}
	defer lock.Close()

	tracker, err := loadCtcTracker(b)
	if err != nil {
		return err
	}

	// Advance a random subset of the bucket's keys (at least one) by a
	// random step each, so keys age unevenly and compaction sees a mix of
	// hot and cold keys.
	steps := make([]int64, ctcKeysPerBucket)
	picked := 0
	for i := range steps {
		if randN(2) == 1 {
			steps[i] = 1 + int64(randN(ctcMaxStep))
			picked++
		}
	}
	if picked == 0 {
		steps[randN(ctcKeysPerBucket)] = 1 + int64(randN(ctcMaxStep))
		picked = 1
	}

	// build emits the round's records in ascending (key id, value) order,
	// which per partition is also the produce order. Within one nonce that
	// order is preserved end to end: the idempotent session keeps it on the
	// wire, and compaction only removes records, never reorders them — the
	// invariant the sweeper asserts per nonce.
	build := func(nonce uint64) []*kgo.Record {
		var recs []*kgo.Record
		for i, step := range steps {
			id := b*ctcKeysPerBucket + i
			for v := tracker.Committed[i] + 1; v <= tracker.Committed[i]+step; v++ {
				recs = append(recs, makeCtcRecord(nonce, id, v))
			}
		}
		return recs
	}
	nonce := rng.Uint64()
	recs := build(nonce)

	cl, err := newClient(
		kgo.ClientID(fmt.Sprintf("ct_stress/produce_ctc/%016x", nonce)),
		kgo.DefaultProduceTopic(ctcTopic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(5*time.Millisecond),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.ProducerOnDataLossDetected(reportDataLoss),
	)
	if err != nil {
		return err
	}
	defer cl.Close()

	fmt.Printf("ctc bucket %d: producing %d records for %d keys (nonce=%016x)\n",
		b, len(recs), picked, nonce)
	deadline := time.Now().Add(ctcProduceBudget)
	for attempt := 1; ; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		err := cl.ProduceSync(ctx, recs...).FirstErr()
		cancel()
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			fmt.Printf("ctc bucket %d: round uncommitted after %d attempts (expected under faults) (nonce=%016x): %v\n",
				b, attempt, nonce, err)
			return nil
		}
		fmt.Printf("ctc bucket %d: produce attempt %d failed, retrying (nonce=%016x): %v\n",
			b, attempt, nonce, err)
		// A timed-out attempt may have acked a subset; the retry re-produces
		// the whole round under a fresh nonce, so the acked leftovers of the
		// old attempt and the new attempt never interleave within one nonce.
		// The duplicate (key, value) pairs this lands are part of the model.
		nonce = rng.Uint64()
		recs = build(nonce)
		time.Sleep(2 * time.Second)
	}

	for i, step := range steps {
		tracker.Committed[i] += step
	}
	if err := saveCtcTracker(b, tracker); err != nil {
		return err
	}
	assert.Reachable("workload committed a compacted cloud topic round",
		map[string]any{"bucket": b, "records": len(recs), "nonce": fmt.Sprintf("%016x", nonce)})
	fmt.Printf("ctc bucket %d: committed %d records (nonce=%016x)\n", b, len(recs), nonce)
	return nil
}

// parallel_driver_sweep_ctc: read one random ctc partition end to end and
// assert the shape of a compacted log (see validateCtcRange). The surviving
// value per key is not yet held against the tracker's committed counters;
// the tracker exists so a later checker can.
func sweepCtc() error {
	part := int32(randN(ctcPartitions))
	lo, hi, err := partitionBounds(ctcTopic, part)
	if err != nil || hi <= lo {
		return nil // empty or unreadable under faults; nothing to sweep
	}
	recs, err := readRange(ctcTopic, part, lo, hi)
	if err != nil {
		return nil
	}
	assert.Sometimes(len(recs) > 0, "compacted cloud topic sweep consumes a non-empty range",
		map[string]any{"partition": part, "lo": lo, "hi": hi})
	validateCtcRange(part, lo, hi, recs)
	return nil
}

// validateCtcRange asserts that recs — the result of reading [lo, hi) from a
// ctc partition — have the shape of a compacted log; it backs both the
// anytime sweep and finally_check_complete. Compaction removes records but
// never reorders survivors, so offsets must still be strictly increasing
// (though gapped, unlike foo, so no contiguity here) and each producer
// nonce's records must still appear in its produce order — ascending (key
// id, value), the order build() emits and the idempotent session preserves.
// Records must be intact and ours. Offset gaps are compaction's fingerprint
// in an otherwise gapless non-transactional log, so seeing one sometimes
// proves compaction actually runs and the workload has not degraded to
// plain produce/consume. An empty read is a no-op: the invariants hold on
// whatever a fault leaves readable, so they never false-positive on
// truncation.
func validateCtcRange(part int32, lo, hi int64, recs []*kgo.Record) {
	if len(recs) == 0 {
		return
	}

	type lastPos struct {
		id  int
		val int64
	}
	lastByNonce := map[uint64]lastPos{}
	latest := make(map[int]int64)
	var bad, reordered, gaps int
	var missing int64
	firstOff, firstReason := int64(-1), ""
	inOrder := true
	prev := int64(-1)
	for _, r := range recs {
		if prev >= 0 {
			if r.Offset <= prev {
				inOrder = false
			} else if r.Offset > prev+1 {
				gaps++
				missing += r.Offset - prev - 1
			}
		}
		prev = r.Offset
		p, reason := parseCtcRecord(r, part)
		isReorder := false
		if reason == "" {
			lp, seen := lastByNonce[p.nonce]
			if seen && (p.id < lp.id || (p.id == lp.id && p.val <= lp.val)) {
				isReorder = true
				reason = fmt.Sprintf("producer %016x out of order: key %d val %d at or after key %d val %d",
					p.nonce, p.id, p.val, lp.id, lp.val)
			} else {
				lastByNonce[p.nonce] = lastPos{id: p.id, val: p.val}
				latest[p.id] = p.val
			}
		}
		if reason != "" {
			if isReorder {
				reordered++
			} else {
				bad++
			}
			if firstReason == "" {
				firstOff, firstReason = r.Offset, reason
			}
		}
	}
	superseded := len(recs) - bad - reordered - len(latest)
	partial := prev != hi-1

	details := map[string]any{
		"command": cmdName, "partition": part, "lo": lo, "hi": hi,
		"count": len(recs), "keys": len(latest), "superseded": superseded,
		"gaps": gaps, "missing": missing,
		"bad_data": bad, "reordered": reordered,
		"bad_offset": firstOff, "bad_reason": firstReason,
	}
	assert.Always(inOrder, "compacted cloud topic read returns in-order offsets", details)
	assert.Always(bad == 0, "compacted cloud topic records are intact and self-consistent", details)
	assert.Always(reordered == 0, "compacted cloud topic per-producer produce order is preserved", details)
	if !finallyPhase() {
		assert.Sometimes(gaps > 0, "compacted cloud topic sweep observes compaction gaps", details)
	}

	fmt.Printf("ctc sweep %d [%d,%d) -> %d records (keys=%d superseded=%d gaps=%d missing=%d bad=%d reordered=%d partial=%v)\n",
		part, lo, hi, len(recs), len(latest), superseded, gaps, missing, bad, reordered, partial)
	if firstReason != "" {
		fmt.Printf("ctc sweep %d first bad record at offset %d: %s\n", part, firstOff, firstReason)
	}
}
