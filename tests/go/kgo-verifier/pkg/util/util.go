package util

import (
	"fmt"
	"os"
	"sync"

	log "github.com/sirupsen/logrus"
)

func Die(msg string, args ...interface{}) {
	formatted := fmt.Sprintf(msg, args...)
	log.Error(formatted)
	os.Exit(1)
}

func Chk(err error, msg string, args ...interface{}) {
	if err != nil {
		Die(msg, args...)
	}
}

// loopState is a helper struct holding common state for managing consumer loops.
type loopState struct {
	mu sync.RWMutex

	// `loop` is set to false when looping should stop. If it is false initially
	// then `Next()` must return true at least once.
	// To achieve that, we set lastPass to true to indicate that Next() returned
	// true at least once when `loop` was false and on the next run we know that
	// we are done and need to fuse the state.
	loop     bool
	lastPass bool

	// fused is set to true after Next returns false. It is used to enforce
	// the invariant that Next must not be called after it returned false
	// previously.
	fused bool
}

// NewLoopState creates a state object for managing a consumer loops.
func NewLoopState(loop bool) *loopState {
	return &loopState{loop: loop}
}

// RequestLastPass requests for the loop to run one more time before exiting.
func (ls *loopState) RequestLastPass() {
	ls.mu.Lock()
	defer ls.mu.Unlock()

	ls.loop = false
}

// Next returns true if current loop iteration should run.
func (ls *loopState) Next() bool {
	ls.mu.Lock()
	defer ls.mu.Unlock()

	if ls.fused {
		panic("invariant: Next must not be called after it returned false previously")
	}

	if ls.lastPass {
		ls.fused = true
		return false
	} else if !ls.loop {
		log.Info("This is the last pass.")
		ls.lastPass = true
	}

	return true
}
