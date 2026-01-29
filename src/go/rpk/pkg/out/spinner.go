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
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/briandowns/spinner"
	"github.com/fatih/color"
	"github.com/mattn/go-isatty"
)

// Spinner provides animated progress indication for long-running operations.
// In TTY environments, it displays an animated spinner with elapsed time.
// In non-TTY environments, it gracefully degrades to simple text output.
type Spinner struct {
	spinner     *spinner.Spinner
	mu          sync.Mutex
	isTTY       bool
	output      io.Writer
	stopped     bool
	startTime   time.Time
	showElapsed bool
	message     string
	cancel      context.CancelFunc
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
// or Fail() is called. The context is used for cancellation, if the
// context is cancelled, the spinner will stop automatically.
//
// In non-TTY environments, the spinner degrades gracefully - no animation
// is shown, only the final message when stopped.
//
// Example:
//
//	spinner := out.NewSpinner(ctx, "Creating shadow link...")
//	defer spinner.Stop()
//	// ... do work ...
//	spinner.Success("Shadow link created")
func NewSpinner(ctx context.Context, message string, opts ...SpinnerOption) *Spinner {
	cfg := &spinnerConfig{
		output: os.Stdout,
	}
	for _, opt := range opts {
		opt(cfg)
	}

	s := &Spinner{
		output:      cfg.output,
		isTTY:       isTerminal(cfg.output),
		startTime:   time.Now(),
		showElapsed: cfg.showElapsed,
		message:     message,
	}

	if !s.isTTY {
		// Non-TTY: print message once, no animation
		fmt.Fprintln(s.output, message)
		return s
	}

	// TTY: start animated spinner
	// CharSets[11] is the dot spinner (⣾)
	sp := spinner.New(spinner.CharSets[11], 100*time.Millisecond, spinner.WithWriter(cfg.output))
	sp.Suffix = " " + message
	s.spinner = sp
	sp.Start()

	// If elapsed time is enabled, start a goroutine to update the suffix.
	// We create a child context so we can cancel the goroutine when the
	// spinner stops, without affecting the parent context.
	if s.showElapsed {
		ctx, cancel := context.WithCancel(ctx)
		s.cancel = cancel
		go s.updateElapsedTime(ctx)
	}

	return s
}

// updateElapsedTime periodically updates the spinner suffix with elapsed time.
func (s *Spinner) updateElapsedTime(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.mu.Lock()
			if s.stopped || s.spinner == nil {
				s.mu.Unlock()
				return
			}
			elapsed := time.Since(s.startTime).Truncate(time.Second)
			s.spinner.Suffix = fmt.Sprintf(" %s (%v elapsed)", s.message, elapsed)
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
	green := color.New(color.FgGreen).SprintFunc()
	s.stopWith(fmt.Sprintf("%s %s\n", green("\u2713"), message))
}

// Fail stops the spinner and displays an error message with an ✗.
func (s *Spinner) Fail(message string) {
	red := color.New(color.FgRed).SprintFunc()
	s.stopWith(fmt.Sprintf("%s %s\n", red("\u2717"), message))
}

// UpdateMessage updates the spinner's message while it's running.
func (s *Spinner) UpdateMessage(message string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.stopped || s.spinner == nil {
		return
	}

	s.message = message
	if s.showElapsed {
		elapsed := time.Since(s.startTime).Truncate(time.Second)
		s.spinner.Suffix = fmt.Sprintf(" %s (%v elapsed)", message, elapsed)
	} else {
		s.spinner.Suffix = " " + message
	}
}

// stopWith stops the spinner and displays the given final message.
func (s *Spinner) stopWith(finalMsg string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.stopped {
		return
	}
	s.stopped = true

	// Cancel context to stop elapsed time updater
	if s.cancel != nil {
		s.cancel()
	}

	if s.spinner != nil {
		s.spinner.FinalMSG = finalMsg
		s.spinner.Stop()
	} else if finalMsg != "" {
		// Non-TTY mode: just print the final message
		fmt.Fprint(s.output, finalMsg)
	}
}
