package verifier

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/redpanda-data/redpanda/tests/go/kgo-verifier/pkg/util"
	log "github.com/sirupsen/logrus"
	"github.com/twmb/franz-go/pkg/kgo"
)

func NewValidatorStatus(compacted bool, expectFullyCompacted bool, topic string, nPartitions int32) ValidatorStatus {
	return ValidatorStatus{
		MaxOffsetsConsumed:   make(map[int32]int64),
		lastCheckpoint:       time.Now(),
		compacted:            compacted,
		expectFullyCompacted: expectFullyCompacted,
	}
}

type ValidatorStatus struct {
	// To help human beings reading logs, not functionally necessary.
	Name string `json:"name"`

	// How many messages did we try to transmit?
	ValidReads int64 `json:"valid_reads"`

	// How many validation errors (indicating bugs!)
	InvalidReads int64 `json:"invalid_reads"`

	// Number of times the record offset jumped forward (indicating gaps in consumption)
	// Only tracked in no compaction case
	OffsetGaps int64 `json:"offset_gaps"`

	// How many validation errors on extents that are not
	// designated as valid by the producer (indicates
	// offsets where retries happened or where unrelated
	// data was written to the topic)
	OutOfScopeInvalidReads int64 `json:"out_of_scope_invalid_reads"`

	// The highest valid offset consumed throughout the consumer's lifetime
	MaxOffsetsConsumed map[int32]int64 `json:"max_offsets_consumed"`

	LostOffsets map[int32]int64 `json:"lost_offsets"`

	// The number of tombstones consumed
	TombstonesConsumed int64 `json:"tombstones_consumed"`

	// Concurrent access happens when doing random reads
	// with multiple reader fibers
	lock sync.Mutex

	// For emitting checkpoints on time intervals
	lastCheckpoint time.Time

	// User bytes read since last checkpoint (payload without protocol overhead)
	userBytesSinceLastCheckpoint int64

	// Last consumed offset per partition. Used to assert monotonicity and check for gaps.
	lastOffsetConsumed map[int32]int64

	// Last leader epoch per partition. Used to assert monotonicity.
	lastLeaderEpoch map[int32]int32

	// Whether the topic to be consumed is compacted. Gaps in offsets will be ignored if true.
	compacted bool

	// Whether the values consumed should be verified against the last produced value for a given key in the log.
	expectFullyCompacted bool
}

func (cs *ValidatorStatus) ValidateRecord(r *kgo.Record, validRanges *TopicOffsetRanges, latestValuesProduced *LatestValueMap) {
	expect_header_value := fmt.Sprintf("%06d.%018d", 0, r.Offset)
	log.Debugf("Consumed %s on p=%d at o=%d leaderEpoch=%d", r.Key, r.Partition, r.Offset, r.LeaderEpoch)
	cs.lock.Lock()
	defer cs.lock.Unlock()

	if r.Value == nil {
		cs.TombstonesConsumed += 1
	}

	// Rough estimate of bytes read
	cs.userBytesSinceLastCheckpoint += int64(recordUserSize(r))

	if r.LeaderEpoch < cs.lastLeaderEpoch[r.Partition] {
		log.Panicf("Out of order leader epoch on p=%d at o=%d leaderEpoch=%d. Previous leaderEpoch=%d",
			r.Partition, r.Offset, r.LeaderEpoch, cs.lastLeaderEpoch[r.Partition])
	}

	currentMax, present := cs.lastOffsetConsumed[r.Partition]
	if present {
		if currentMax < r.Offset {
			expected := currentMax + 1
			if r.Offset != expected && !cs.compacted {
				cs.OffsetGaps += 1
				log.Warnf("Gap detected in consumed offsets. Expected %d, but got %d", expected, r.Offset)
			}
		} else {
			log.Panicf("Out of order read. Max consumed offset(partition=%d)=%d; Current record offset=%d", r.Partition, currentMax, r.Offset)
		}
	}

	var got_header_value string
	if len(r.Headers) > 0 {
		got_header_value = string(r.Headers[0].Value)
	}

	recordExpected := expect_header_value == got_header_value
	if !recordExpected {
		shouldBeValid := validRanges.Contains(r.Partition, r.Offset)

		if shouldBeValid {
			cs.InvalidReads += 1
			log.Panicf("Bad read at offset %d on partition %s/%d.  Expect '%s', found '%s'", r.Offset, r.Topic, r.Partition, expect_header_value, got_header_value)
		} else {
			cs.OutOfScopeInvalidReads += 1
			log.Infof("Ignoring read validation at offset outside valid range %s/%d %d", r.Topic, r.Partition, r.Offset)
		}
	} else {
		cs.ValidReads += 1
		log.Debugf("Read OK (%s) on p=%d at o=%d", r.Headers[0].Value, r.Partition, r.Offset)
	}

	if cs.expectFullyCompacted {
		latestValue, exists := latestValuesProduced.GetValue(r.Partition, string(r.Key))
		if !exists || string(latestValue) != string(r.Value) {
			log.Panicf("Consumed value for key %s does not match the latest produced value in a compacted topic- did compaction for partition %s/%d occur betwen producing and consuming?", r.Key, r.Topic, r.Partition)
		}
	}

	cs.recordOffset(r, recordExpected)

	if time.Since(cs.lastCheckpoint) > time.Second*5 {
		cs.Checkpoint()
		cs.lastCheckpoint = time.Now()
	}
}

func (cs *ValidatorStatus) recordOffset(r *kgo.Record, recordExpected bool) {
	if cs.MaxOffsetsConsumed == nil {
		cs.MaxOffsetsConsumed = make(map[int32]int64)
	}
	if cs.lastOffsetConsumed == nil {
		cs.lastOffsetConsumed = make(map[int32]int64)
	}
	if cs.lastLeaderEpoch == nil {
		cs.lastLeaderEpoch = make(map[int32]int32)
	}
	// We bump highest offset only for valid records.
	if r.Offset > cs.MaxOffsetsConsumed[r.Partition] && recordExpected {
		cs.MaxOffsetsConsumed[r.Partition] = r.Offset
	}

	cs.lastOffsetConsumed[r.Partition] = r.Offset
	cs.lastLeaderEpoch[r.Partition] = r.LeaderEpoch
}

func (cs *ValidatorStatus) RecordLostOffsets(p int32, count int64) {
	cs.lock.Lock()
	defer cs.lock.Unlock()

	if cs.LostOffsets == nil {
		cs.LostOffsets = make(map[int32]int64)
	}

	cs.LostOffsets[p] += count
}

func (cs *ValidatorStatus) ResetMonotonicityTestState() {
	cs.lock.Lock()
	defer cs.lock.Unlock()

	cs.lastOffsetConsumed = make(map[int32]int64)
	cs.lastLeaderEpoch = make(map[int32]int32)
}

func (cs *ValidatorStatus) SetMonotonicityTestStateForPartition(partition int32, offset int64) {
	cs.lock.Lock()
	defer cs.lock.Unlock()

	if cs.lastOffsetConsumed == nil {
		cs.lastOffsetConsumed = make(map[int32]int64)
	}

	cs.lastOffsetConsumed[partition] = offset
}

func (cs *ValidatorStatus) Checkpoint() {
	log.Infof("Validator status: %s", cs.String())

	elapsed := time.Since(cs.lastCheckpoint)
	if elapsed <= 0 {
		return
	}

	// Calculate throughput in MB/s
	mbRead := float64(cs.userBytesSinceLastCheckpoint) / (1024.0 * 1024.0)
	throughput := mbRead / elapsed.Seconds()
	log.Infof("Validator throughput (user payload): %.2f MB/s", throughput)

	// Reset byte counter
	cs.userBytesSinceLastCheckpoint = 0
}

func (cs *ValidatorStatus) String() string {
	data, err := json.Marshal(cs)
	util.Chk(err, "Status serialization error")
	return string(data)
}

func recordUserSize(r *kgo.Record) int {
	sz := len(r.Key) + len(r.Value)
	for _, h := range r.Headers {
		sz += len(h.Key) + len(h.Value)
	}
	return sz
}
