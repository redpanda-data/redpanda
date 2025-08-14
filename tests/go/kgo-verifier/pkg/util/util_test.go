package util_test

import (
	"testing"

	"github.com/redpanda-data/redpanda/tests/go/kgo-verifier/pkg/util"
)

func TestLoopStateDefault(t *testing.T) {
	s := util.NewLoopState(false)
	if !s.Next() {
		t.Error("Next must returns true on first pass")
	}
	if s.Next() {
		t.Error("Next must return false after first pass")
	}
}

func TestLoopStateDoLoop(t *testing.T) {
	s := util.NewLoopState(true)
	for i := 0; i < 3; i++ {
		if !s.Next() {
			t.Error("Next must return true as long as Loop is set to true")
		}
	}
	s.RequestLastPass()
	if !s.Next() {
		t.Error("Next must return true after RequestLastPass")
	}
	if s.Next() {
		t.Error("Next must return false after RequestLastPass")
	}

	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Error("Next must panic after RequestLastPass and third Next call")
			}
		}()
		s.Next()
	}()
}

func TestLoopStateDoLoopStopImmediately(t *testing.T) {
	s := util.NewLoopState(true)
	s.RequestLastPass()
	if !s.Next() {
		t.Error("Next must return true after RequestLastPass")
	}
	if s.Next() {
		t.Error("Next must return false after RequestLastPass")
	}

	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Error("Next must panic after RequestLastPass and third Next call")
			}
		}()
		s.Next()
	}()
}
