// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package out

import (
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/briandowns/spinner"
	"github.com/mattn/go-isatty"
)

// Spinner provides animated progress indication for long-running operations.
// In TTY environments, it displays an animated spinner.
// In non-TTY environments, it gracefully degrades to simple text output.
type Spinner struct {
	spinner     *spinner.Spinner
	mu          sync.Mutex
	isTTY       bool
	output      io.Writer
	stopped     bool
	message     string
	startTime   time.Time
	stopCh      chan struct{}
	showElapsed bool
}

// spinnerConfig holds configuration options for the spinner.
type spinnerConfig struct {
	output      io.Writer
	showElapsed bool
}

// SpinnerOption configures a Spinner.
type SpinnerOption func(*spinnerConfig)

// WithOutput sets the output writer for the spinner.
// Defaults to os.Stdout.
func WithOutput(w io.Writer) SpinnerOption {
	return func(c *spinnerConfig) {
		c.output = w
	}
}

// WithElapsedTime enables showing elapsed time in the spinner message.
// When enabled, the spinner displays "(Xs elapsed)" after the message.
func WithElapsedTime() SpinnerOption {
	return func(c *spinnerConfig) {
		c.showElapsed = true
	}
}

// isTerminal checks if the given writer is a terminal.
func isTerminal(w io.Writer) bool {
	if f, ok := w.(*os.File); ok {
		return isatty.IsTerminal(f.Fd()) || isatty.IsCygwinTerminal(f.Fd())
	}
	return false
}

// NewSpinner creates and starts a new spinner with the given message.
// The spinner runs in a background goroutine until Stop(), Success(),
// or Fail() is called.
//
// In non-TTY environments, the spinner degrades gracefully - no animation
// is shown, only the final message when stopped.
//
// Example:
//
//	spinner := out.NewSpinner("Creating shadow link...")
//	defer spinner.Stop()
//	// ... do work ...
//	spinner.Success("Shadow link created")
func NewSpinner(message string, opts ...SpinnerOption) *Spinner {
	cfg := &spinnerConfig{
		output: os.Stdout,
	}
	for _, opt := range opts {
		opt(cfg)
	}

	s := &Spinner{
		output:      cfg.output,
		isTTY:       isTerminal(cfg.output),
		message:     message,
		showElapsed: cfg.showElapsed,
	}

	if !s.isTTY {
		// Non-TTY: print message once, no animation
		fmt.Fprintln(s.output, message)
		return s
	}

	// TTY: start animated spinner
	s.spinner = spinner.New(spinner.CharSets[14], 100*time.Millisecond,
		spinner.WithWriter(s.output))
	s.updateSuffix()
	s.spinner.Start()

	if s.showElapsed {
		s.startTime = time.Now()
		s.stopCh = make(chan struct{})
		// Start goroutine to update elapsed time every second
		go s.runElapsedTimeUpdater()
	}

	return s
}

// updateSuffix updates the spinner's suffix with the current message.
// Must be called with s.mu held or before the spinner is started.
func (s *Spinner) updateSuffix() {
	if s.showElapsed {
		elapsed := time.Since(s.startTime).Truncate(time.Second)
		s.spinner.Suffix = fmt.Sprintf(" %s (%v elapsed)", s.message, elapsed)
	} else {
		s.spinner.Suffix = " " + s.message
	}
}

// runElapsedTimeUpdater updates the spinner suffix every second to show elapsed time.
func (s *Spinner) runElapsedTimeUpdater() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.mu.Lock()
			if !s.stopped && s.spinner != nil {
				s.updateSuffix()
			}
			s.mu.Unlock()
		}
	}
}

// Stop stops the spinner without displaying a final message.
// Safe to call multiple times.
func (s *Spinner) Stop() {
	s.stopWith("")
}

// Success stops the spinner and displays a success message with a ✓ checkmark.
func (s *Spinner) Success(message string) {
	s.stopWith(fmt.Sprintf("\u2713 %s", message))
}

// Fail stops the spinner and displays an error message with an ✗.
func (s *Spinner) Fail(message string) {
	s.stopWith(fmt.Sprintf("\u2717 %s", message))
}

// UpdateMessage updates the spinner's message while it's running.
func (s *Spinner) UpdateMessage(message string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.stopped || s.spinner == nil {
		return
	}

	s.message = message
	s.updateSuffix()
}

// stopWith stops the spinner and displays the given final message.
func (s *Spinner) stopWith(finalMsg string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.stopped {
		return
	}
	s.stopped = true

	if s.spinner != nil {
		// Stop the elapsed time updater goroutine if running
		if s.showElapsed {
			close(s.stopCh)
		}

		if finalMsg != "" {
			s.spinner.FinalMSG = finalMsg + "\n"
		}
		s.spinner.Stop()
	} else if finalMsg != "" {
		// Non-TTY mode: just print the final message
		fmt.Fprintln(s.output, finalMsg)
	}
}
