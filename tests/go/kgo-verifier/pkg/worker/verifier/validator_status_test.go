package verifier_test

import (
	"testing"

	"github.com/redpanda-data/redpanda/tests/go/kgo-verifier/pkg/worker/verifier"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestValidatorStatus_ValidateRecordHappyPath(t *testing.T) {
	validator := verifier.NewValidatorStatus(false, false, "topic", 1)
	validRanges := verifier.NewTopicOffsetRanges("topic", 1)
	validRanges.Insert(0, 41)
	validRanges.Insert(0, 42)

	validator.ValidateRecord(&kgo.Record{
		Offset:      41,
		LeaderEpoch: 0,
		Headers:     []kgo.RecordHeader{{Key: "key", Value: []byte("000000.000000000000000041")}},
	}, &validRanges, nil)

	validator.ValidateRecord(&kgo.Record{
		Offset:      42,
		LeaderEpoch: 0,
		Headers:     []kgo.RecordHeader{{Key: "key", Value: []byte("000000.000000000000000042")}},
	}, &validRanges, nil)

	validator.ValidateRecord(&kgo.Record{
		Offset:      43,
		LeaderEpoch: 1,
	}, &validRanges, nil)

	assert.Equal(t, int64(2), validator.ValidReads)
	assert.Equal(t, int64(0), validator.InvalidReads)
	assert.Equal(t, int64(1), validator.OutOfScopeInvalidReads)

	// Only valid reads increment the max offset consumed.
	assert.Equal(t, int64(42), validator.MaxOffsetsConsumed[0])

}

func TestValidatorStatus_ValidateRecordInvalidRead(t *testing.T) {
	validator := verifier.NewValidatorStatus(false, false, "topic", 1)
	validRanges := verifier.NewTopicOffsetRanges("topic", 1)
	validRanges.Insert(0, 41)

	// Miss-match between expected offset as recorded in the header and the actual offset.
	func() {
		defer func() {
			if r := recover(); r != nil {
				assert.Equal(t, "Bad read at offset 41 on partition /0.  Expect '000000.000000000000000041', found '000000.000000000000000040'", r.(*logrus.Entry).Message)
			}
		}()

		validator.ValidateRecord(&kgo.Record{
			Offset:      41,
			LeaderEpoch: 0,
			Headers:     []kgo.RecordHeader{{Key: "key", Value: []byte("000000.000000000000000040")}},
		}, &validRanges, nil)
	}()
}

func TestValidatorStatus_ValidateRecordNonMonotonicOffset(t *testing.T) {
	validator := verifier.NewValidatorStatus(false, false, "topic", 1)
	validRanges := verifier.NewTopicOffsetRanges("topic", 1)

	validator.ValidateRecord(&kgo.Record{
		Offset:      41,
		LeaderEpoch: 0,
	}, &validRanges, nil)

	// Same offset read again.
	func() {
		defer func() {
			if r := recover(); r != nil {
				assert.Equal(t, "Out of order read. Max consumed offset(partition=0)=41; Current record offset=41", r.(*logrus.Entry).Message)
			}
		}()

		validator.ValidateRecord(&kgo.Record{
			Offset:      41,
			LeaderEpoch: 0,
		}, &validRanges, nil)
	}()

	// Lower offset read after a higher offset.
	func() {
		defer func() {
			if r := recover(); r != nil {
				assert.Equal(t, "Out of order read. Max consumed offset(partition=0)=41; Current record offset=40", r.(*logrus.Entry).Message)
			}
		}()

		validator.ValidateRecord(&kgo.Record{
			Offset:      40,
			LeaderEpoch: 0,
		}, &validRanges, nil)
	}()
}

func TestValidatorStatus_ValidateRecordNonMonotonicLeaderEpoch(t *testing.T) {
	validator := verifier.NewValidatorStatus(false, false, "topic", 1)
	validRanges := verifier.NewTopicOffsetRanges("topic", 1)

	validator.ValidateRecord(&kgo.Record{
		Offset:      41,
		LeaderEpoch: 1,
	}, &validRanges, nil)

	func() {
		defer func() {
			if r := recover(); r != nil {
				assert.Equal(t, "Out of order leader epoch on p=0 at o=42 leaderEpoch=0. Previous leaderEpoch=1", r.(*logrus.Entry).Message)
			}
		}()

		validator.ValidateRecord(&kgo.Record{
			Offset:      42,
			LeaderEpoch: 0,
		}, &validRanges, nil)
	}()

}
