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

	"github.com/charmbracelet/bubbles/spinner"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/mattn/go-isatty"
)

// Spinner provides animated progress indication for long-running operations.
// In TTY environments, it displays an animated spinner with elapsed time.
// In non-TTY environments, it gracefully degrades to simple text output.
type Spinner struct {
	program *tea.Program
	done    chan struct{}
	mu      sync.Mutex
	isTTY   bool
	output  io.Writer
	stopped bool
}

// spinnerConfig holds configuration options for the spinner.
type spinnerConfig struct {
	output io.Writer
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

// spinnerModel is the bubbletea model for the spinner.
type spinnerModel struct {
	spinner   spinner.Model
	message   string
	startTime time.Time
	quitting  bool
	finalMsg  string
}

// updateMessageMsg is sent to update the spinner's message.
type updateMessageMsg string

// quitMsg is sent to stop the spinner.
type quitMsg struct {
	finalMsg string
}

func (m spinnerModel) Init() tea.Cmd {
	return m.spinner.Tick
}

func (m spinnerModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case spinner.TickMsg:
		var cmd tea.Cmd
		m.spinner, cmd = m.spinner.Update(msg)
		return m, cmd
	case updateMessageMsg:
		m.message = string(msg)
		return m, nil
	case quitMsg:
		m.quitting = true
		m.finalMsg = msg.finalMsg
		return m, tea.Quit
	}
	return m, nil
}

func (m spinnerModel) View() string {
	if m.quitting {
		if m.finalMsg != "" {
			return m.finalMsg + "\n"
		}
		return ""
	}
	elapsed := time.Since(m.startTime).Truncate(time.Second)
	return fmt.Sprintf("%s %s (%v elapsed)", m.spinner.View(), m.message, elapsed)
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
		done:   make(chan struct{}),
		output: cfg.output,
		isTTY:  isTerminal(cfg.output),
	}

	if !s.isTTY {
		// Non-TTY: print message once, no animation
		fmt.Fprintln(s.output, message)
		close(s.done)
		return s
	}

	// TTY: start animated spinner
	sp := spinner.New()
	sp.Spinner = spinner.Dot

	model := spinnerModel{
		spinner:   sp,
		message:   message,
		startTime: time.Now(),
	}

	s.program = tea.NewProgram(model,
		tea.WithOutput(s.output),
		tea.WithoutSignalHandler(),
	)

	go func() {
		_, _ = s.program.Run()
		close(s.done)
	}()

	return s
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

	if s.stopped || s.program == nil {
		return
	}

	s.program.Send(updateMessageMsg(message))
}

// stopWith stops the spinner and displays the given final message.
func (s *Spinner) stopWith(finalMsg string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.stopped {
		return
	}
	s.stopped = true

	if s.program != nil {
		s.program.Send(quitMsg{finalMsg: finalMsg})
		<-s.done
	} else if finalMsg != "" {
		// Non-TTY mode: just print the final message
		fmt.Fprintln(s.output, finalMsg)
	}
}
