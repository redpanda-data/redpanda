package verifier_test

import (
	"reflect"
	"testing"

	"github.com/redpanda-data/redpanda/tests/go/kgo-verifier/pkg/worker/verifier"
)

func TestOffsetRanges(t *testing.T) {
	r := verifier.OffsetRanges{}
	r.Insert(0)
	r.Insert(1)
	r.Insert(2)
	r.Insert(10)
}

func TestOffsetRangesOutOfOrder(t *testing.T) {
	r := verifier.OffsetRanges{}
	r.Insert(0)
	r.Insert(4)
	r.Insert(5)
	r.Insert(6)
	r.Insert(10)

	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("Expected to panic on out of order insert")
			}
		}()
		r.Insert(3)
	}()

}

func TestOffsetRangesTolerateOutOfOrderContinuous(t *testing.T) {
	r := verifier.OffsetRanges{
		TolerateDataLoss: true,
	}
	r.Insert(0)
	r.Insert(4)
	r.Insert(5)
	r.Insert(6)
	r.Insert(10)

	// This will truncate the ranges.
	r.Insert(5)

	expected := verifier.OffsetRanges{
		TolerateDataLoss: true,
	}
	expected.Insert(0)
	expected.Insert(4)
	expected.Insert(5)

	if !reflect.DeepEqual(expected, r) {
		t.Errorf("Expected %v, got %v", expected, r.Ranges)
	}
}

func TestOffsetRangesTolerateOutOfOrderGaps(t *testing.T) {
	r := verifier.OffsetRanges{
		TolerateDataLoss: true,
	}
	r.Insert(0)
	r.Insert(3)
	r.Insert(5)
	r.Insert(7)
	r.Insert(10)

	// This will truncate the ranges.
	r.Insert(5)

	expected := verifier.OffsetRanges{
		TolerateDataLoss: true,
	}
	expected.Insert(0)
	expected.Insert(3)
	expected.Insert(5)

	if !reflect.DeepEqual(expected, r) {
		t.Errorf("Expected %v, got %v", expected, r.Ranges)
	}
}

func TestOffsetRangesTolerateOutOfOrderInsideGap(t *testing.T) {
	r := verifier.OffsetRanges{
		TolerateDataLoss: true,
	}
	r.Insert(0)
	r.Insert(3)
	r.Insert(7)
	r.Insert(10)

	// This will truncate the ranges.
	r.Insert(5)

	expected := verifier.OffsetRanges{
		TolerateDataLoss: true,
	}
	expected.Insert(0)
	expected.Insert(3)
	expected.Insert(5)

	if !reflect.DeepEqual(expected, r) {
		t.Errorf("Expected %v, got %v", expected, r)
	}
}
