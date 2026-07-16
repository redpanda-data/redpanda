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
// a tracker file per bucket records, per key, the last committed counter
// value and the highest broker-acked write (offset plus record identity).
// A producer round exclusively locks one bucket, produces the next run of
// counter values for a subset of its keys, folds every ack it observes into
// the acked summary (on failed rounds too), and commits the new counters
// only once every record is acked.
//
// Committed counters drive value generation; they are not a safety bound on
// surviving values. Idempotent sessions do not fence each other, so a
// produce request from an abandoned producer session can sit in a paused
// broker and apply after a later round commits higher counters, legally
// leaving a lower value as a key's latest record. What can never legally happen is a
// key's newest surviving offset regressing below an acked offset:
// compaction drops a record only when the key has a newer one, and such
// stragglers only push the newest offset up. The checkers hold reads
// against the acked summary (validateCtcAcked) on top of the log-shape
// invariants that survive compaction (in-order offsets, record integrity,
// per-producer produce order) and the liveness property that compaction
// visibly runs.
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
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sys/unix"
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
	// ctcProduceBudget bounds one round's produce, applied as the produce
	// context timeout; franz-go retries transient failures within it. A
	// round that cannot deliver within the budget exits uncommitted and a
	// later round for the bucket re-produces the range.
	ctcProduceBudget = 3 * time.Minute

	ctcMagic = "CTC1"
)

func ctcTrackerDir() string {
	if d := os.Getenv("CTC_TRACKER_DIR"); d != "" {
		return d
	}
	return "/var/lib/ct"
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

// ctcTracker is a bucket's per-key progress, indexed by the key's position
// within the bucket: the committed counter (zero, the seed state, means
// nothing committed; produced values start at 1) and the highest acked
// write. Committed only moves once a whole round is acked; Acked advances
// on every observed ack, including in rounds that end uncommitted.
type ctcTracker struct {
	Committed []int64    `json:"committed"`
	Acked     []ctcAcked `json:"acked"`
}

// ctcAcked is a key's highest broker-acked write: the offset the ack
// reported and the identity of the record at it, for content comparison
// when that exact offset is the key's surviving latest. Offset -1 (the seed
// state) means no ack observed yet. Only observed acks are recorded, never
// intent, so the summary can lag reality but never overstate it.
type ctcAcked struct {
	Offset int64  `json:"offset"`
	Val    int64  `json:"val"`
	Nonce  uint64 `json:"nonce"`
}

func newCtcTracker() *ctcTracker {
	t := &ctcTracker{
		Committed: make([]int64, ctcKeysPerBucket),
		Acked:     make([]ctcAcked, ctcKeysPerBucket),
	}
	for i := range t.Acked {
		t.Acked[i].Offset = -1
	}
	return t
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
		err = unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			return b, f, nil
		}
		f.Close()
		if !errors.Is(err, unix.EWOULDBLOCK) {
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
		return newCtcTracker(), nil
	}
	if err != nil {
		return nil, err
	}
	var t ctcTracker
	if err := json.Unmarshal(raw, &t); err != nil {
		return nil, fmt.Errorf("tracker %s corrupt: %w", dataPath, err)
	}
	if len(t.Committed) != ctcKeysPerBucket || len(t.Acked) != ctcKeysPerBucket {
		return nil, fmt.Errorf("tracker %s has %d committed and %d acked keys, want %d",
			dataPath, len(t.Committed), len(t.Acked), ctcKeysPerBucket)
	}
	return &t, nil
}

// loadCtcAckedSnapshot reads every bucket's acked summary into one slice
// indexed by key id. No locks: saveCtcTracker replaces the data file by
// rename, so each read sees a complete tracker. The summary is monotone
// (offsets only advance), which is what makes checking a log read against
// it race-free: take the snapshot before reading the log, and whatever the
// log shows is at least as new as the snapshot.
func loadCtcAckedSnapshot() ([]ctcAcked, error) {
	snap := make([]ctcAcked, ctcBuckets*ctcKeysPerBucket)
	for b := range ctcBuckets {
		t, err := loadCtcTracker(b)
		if err != nil {
			return nil, err
		}
		copy(snap[b*ctcKeysPerBucket:], t.Acked)
	}
	return snap, nil
}

// recordCtcAcks folds one produce attempt's per-record results into the
// bucket's acked summary, keeping the highest acked offset per key. It runs
// on failed attempts too: a timed-out round may have acked a subset, and
// those acks are facts the checkers hold Redpanda to.
func recordCtcAcks(t *ctcTracker, b int, res kgo.ProduceResults) {
	for _, pr := range res {
		if pr.Err != nil {
			continue
		}
		p, reason := parseCtcRecord(pr.Record, pr.Record.Partition)
		if reason != "" {
			continue
		}
		i := p.id - b*ctcKeysPerBucket
		if i < 0 || i >= ctcKeysPerBucket {
			continue
		}
		if a := &t.Acked[i]; pr.Record.Offset > a.Offset {
			*a = ctcAcked{Offset: pr.Record.Offset, Val: p.val, Nonce: p.nonce}
		}
	}
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
// acked. The produce is a single attempt on one idempotent session, bounded
// by the round context, within which franz-go retries transient failures
// itself by resending the same batches under the same sequence numbers. If
// the budget expires or franz-go gives up on any record, the round exits
// with the counters untouched (see the comment on the error path) and a
// later round for the bucket re-produces the range. Either way, every ack
// observed is folded into the tracker's acked summary (recordCtcAcks) and
// persisted before exit.
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

	// The round's records are emitted in ascending (key id, value) order,
	// which per partition is also the produce order. Within one nonce that
	// order is preserved end to end: the idempotent session keeps it on the
	// wire, and compaction only removes records, never reorders them — the
	// invariant the sweeper asserts per nonce.
	nonce := rng.Uint64()
	var recs []*kgo.Record
	for i, step := range steps {
		id := b*ctcKeysPerBucket + i
		for v := tracker.Committed[i] + 1; v <= tracker.Committed[i]+step; v++ {
			recs = append(recs, makeCtcRecord(nonce, id, v))
		}
	}

	cl, err := newClient(
		kgo.ClientID(fmt.Sprintf("ct/produce_ctc/%016x", nonce)),
		kgo.DefaultProduceTopic(ctcTopic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(5*time.Millisecond),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.ProducerOnDataLossDetected(reportDataLoss),
		// Lets the round context below actually end the round. By default
		// franz-go refuses to fail an in-flight record — it cannot tell
		// "never received" from "written but the reply was lost" — and waits
		// for the outcome, unbounded under a long fault. Records that land
		// despite being failed to us become stragglers under this nonce,
		// which the acked summary (they are never recorded) and the
		// acked-offset checks tolerate by design. The session is never
		// produced on again after a failure, so its then-inconsistent
		// sequence window does not matter.
		kgo.AllowIdempotentProduceCancellation(),
	)
	if err != nil {
		return err
	}
	defer cl.Close()

	fmt.Printf("ctc bucket %d: producing %d records for %d keys (nonce=%016x)\n",
		b, len(recs), picked, nonce)
	// The round's single bound: when it fires, franz-go fails whatever is
	// still unresolved (in-flight included, see the client option above) and
	// the round exits uncommitted.
	ctx, cancel := context.WithTimeout(context.Background(), ctcProduceBudget)
	defer cancel()
	res := cl.ProduceSync(ctx, recs...)
	recordCtcAcks(tracker, b, res)
	if err := res.FirstErr(); err != nil {
		// No in-process retry: how franz-go leaves the session after giving
		// up is failure-specific — some paths keep the producer id and rewind
		// the sequence numbers, others reload the id — so a resend on it
		// risks mis-attributed acks (rewound sequences dedup whatever bytes
		// come next, acking them at offsets holding the old records), and a
		// correct retry needs a fresh session producing the range under a
		// fresh nonce. That is exactly what the next round for this bucket
		// is. Exit uncommitted, keeping the acks that were observed; this
		// session's applied-but-unacked batches may surface later as
		// stragglers, which the acked-offset checks tolerate by design.
		fmt.Printf("ctc bucket %d: round uncommitted (expected under faults) (nonce=%016x): %v\n",
			b, nonce, err)
		return saveCtcTracker(b, tracker)
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

// parallel_driver_sweep_ctc: read one random ctc partition end to end,
// assert the shape of a compacted log (validateCtcRange), and hold the read
// against the trackers' acked summaries (validateCtcAcked). The summary is
// snapshotted before the read so the comparison is race-free. The loss half
// of the acked check is only sound when the read reached the high watermark
// and the high watermark held still across the read — a record appended
// mid-read beyond hi legally lets compaction drop everything the [lo, hi)
// window held for that key — so the bounds are re-fetched afterwards and
// loss is only asserted when hi is unchanged.
func sweepCtc() error {
	acked, err := loadCtcAckedSnapshot()
	if err != nil {
		return err
	}
	pol := anytimeRead()
	part := int32(randN(ctcPartitions))
	lo, hi, err := partitionBounds(ctcTopic, part, pol)
	if err != nil || hi <= lo {
		return nil // empty or unreadable under faults; nothing to sweep
	}
	recs := readRange(ctcTopic, part, lo, hi, pol)
	assert.Sometimes(len(recs) > 0, "compacted cloud topic sweep consumes a non-empty range",
		map[string]any{"partition": part, "lo": lo, "hi": hi})
	latest, complete := validateCtcRange(part, lo, hi, recs)
	stable := false
	if complete {
		_, hi2, err := partitionBounds(ctcTopic, part, pol)
		stable = err == nil && hi2 == hi
	}
	validateCtcAcked(part, hi, latest, acked, complete && stable)
	return nil
}

// ctcLatest is a key's newest record within one read: the record at the
// highest offset, with its decoded identity.
type ctcLatest struct {
	off   int64
	val   int64
	nonce uint64
}

// validateCtcRange asserts that recs — the result of reading [lo, hi) from a
// ctc partition — have the shape of a compacted log; it backs both the
// anytime sweep and eventually_check_complete. Compaction removes records but
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
//
// Returns each key's newest record in the read and whether the read reached
// the high watermark — the inputs validateCtcAcked needs.
func validateCtcRange(part int32, lo, hi int64, recs []*kgo.Record) (map[int]ctcLatest, bool) {
	if len(recs) == 0 {
		return nil, false
	}

	type lastPos struct {
		id  int
		val int64
	}
	lastByNonce := map[uint64]lastPos{}
	latest := make(map[int]ctcLatest)
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
			// latest tracks each key's newest record by offset regardless of
			// the per-nonce order check below: a mis-ordered record still
			// survives in the log, and the acked-write check compares against
			// what survives.
			if l, ok := latest[p.id]; !ok || r.Offset > l.off {
				latest[p.id] = ctcLatest{off: r.Offset, val: p.val, nonce: p.nonce}
			}
			lp, seen := lastByNonce[p.nonce]
			if seen && (p.id < lp.id || (p.id == lp.id && p.val <= lp.val)) {
				isReorder = true
				reason = fmt.Sprintf("producer %016x out of order: key %d val %d at or after key %d val %d",
					p.nonce, p.id, p.val, lp.id, lp.val)
			} else {
				lastByNonce[p.nonce] = lastPos{id: p.id, val: p.val}
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
	if !quiescedPhase() {
		assert.Sometimes(gaps > 0, "compacted cloud topic sweep observes compaction gaps", details)
	}

	fmt.Printf("ctc sweep %d [%d,%d) -> %d records (keys=%d superseded=%d gaps=%d missing=%d bad=%d reordered=%d partial=%v)\n",
		part, lo, hi, len(recs), len(latest), superseded, gaps, missing, bad, reordered, partial)
	if firstReason != "" {
		fmt.Printf("ctc sweep %d first bad record at offset %d: %s\n", part, firstOff, firstReason)
	}
	return latest, !partial
}

// validateCtcAcked holds one partition read against the trackers' acked
// summaries — latest is the read's newest record per key, acked a snapshot
// taken before the read (both sides are monotone, so the comparison is
// race-free). Two properties:
//
//   - immutability: when a key's newest surviving record sits exactly at
//     its acked offset, it must be the acked record. A log never rewrites
//     an offset, so any read may assert this, partial or not.
//   - no acked write lost: every key acked on this partition must have a
//     surviving record at or beyond its acked offset. Compaction drops a
//     record only when the key has a newer one, and stragglers from
//     abandoned rounds only push the newest offset up, so the newest
//     surviving offset never legally regresses below an acked one. This is
//     only sound when the read reached the high watermark and hi held still
//     across it; assertLoss says the caller vouches for that.
//
// Keys whose acked offset is at or beyond hi prove nothing in the anytime
// phase (a stale leader can serve an old hi) and are skipped. In the
// eventually phase the cluster is quiesced, hi is the true end of the log,
// and an acked offset beyond it means the high watermark regressed, so there
// they are a hard failure.
func validateCtcAcked(part int32, hi int64, latest map[int]ctcLatest, acked []ctcAcked, assertLoss bool) {
	checked, lost, mismatched, beyondHi := 0, 0, 0, 0
	firstKey, firstReason := -1, ""
	for id, a := range acked {
		if a.Offset < 0 || ctcPartition(id) != part {
			continue
		}
		if a.Offset >= hi {
			beyondHi++
			if quiescedPhase() && firstReason == "" {
				firstKey, firstReason = id, fmt.Sprintf("acked offset %d at or beyond quiesced hi %d", a.Offset, hi)
			}
			continue
		}
		checked++
		reason := ""
		l, ok := latest[id]
		switch {
		case !ok:
			if assertLoss {
				lost++
				reason = fmt.Sprintf("no surviving record; acked offset %d val %d", a.Offset, a.Val)
			}
		case l.off < a.Offset:
			if assertLoss {
				lost++
				reason = fmt.Sprintf("newest surviving offset %d below acked offset %d (val %d)", l.off, a.Offset, a.Val)
			}
		case l.off == a.Offset && (l.val != a.Val || l.nonce != a.Nonce):
			mismatched++
			reason = fmt.Sprintf("record at acked offset %d is not the acked one: got val %d nonce %016x, acked val %d nonce %016x",
				a.Offset, l.val, l.nonce, a.Val, a.Nonce)
		}
		if reason != "" && firstReason == "" {
			firstKey, firstReason = id, reason
		}
	}

	details := map[string]any{
		"command": cmdName, "partition": part, "hi": hi,
		"acked_keys": checked, "beyond_hi": beyondHi,
		"lost": lost, "mismatched": mismatched, "loss_checked": assertLoss,
		"bad_key": firstKey, "bad_reason": firstReason,
	}
	assert.Always(mismatched == 0, "compacted cloud topic record at an acked offset is the acked record", details)
	if assertLoss {
		assert.Always(lost == 0, "compacted cloud topic keeps a record at or beyond every acked offset", details)
	}
	if quiescedPhase() {
		assert.Always(beyondHi == 0, "eventually: compacted cloud topic high watermark covers every acked offset", details)
	} else if assertLoss {
		assert.Sometimes(checked > 0, "compacted cloud topic sweep checks acked writes against a complete read", details)
	}

	fmt.Printf("ctc acked check %d: %d keys checked (loss_checked=%v lost=%d mismatched=%d beyond_hi=%d)\n",
		part, checked, assertLoss, lost, mismatched, beyondHi)
	if firstReason != "" {
		fmt.Printf("ctc acked check %d first bad key %d: %s\n", part, firstKey, firstReason)
	}
}
