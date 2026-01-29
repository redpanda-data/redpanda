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
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// These tests use bytes.Buffer which is detected as non-TTY,
// so they test the graceful degradation path.

func TestSpinner_NonTTY_Success(t *testing.T) {
	buf := new(bytes.Buffer)
	spinner := NewSpinner(context.Background(), "Working...", WithOutput(buf))
	spinner.Success("Done successfully")

	output := buf.String()
	require.Contains(t, output, "Working...")
	require.Contains(t, output, "\u2713 Done successfully")
}

func TestSpinner_NonTTY_Fail(t *testing.T) {
	buf := new(bytes.Buffer)
	spinner := NewSpinner(context.Background(), "Working...", WithOutput(buf))
	spinner.Fail("Something went wrong")

	output := buf.String()
	require.Contains(t, output, "Working...")
	require.Contains(t, output, "\u2717 Something went wrong")
}

func TestSpinner_NonTTY_Stop(t *testing.T) {
	buf := new(bytes.Buffer)
	spinner := NewSpinner(context.Background(), "Working...", WithOutput(buf))
	spinner.Stop()

	output := buf.String()
	require.Contains(t, output, "Working...")
	// Stop without message should not add extra output
	lines := strings.Split(strings.TrimSpace(output), "\n")
	require.Len(t, lines, 1)
}

func TestSpinner_MultipleStops(t *testing.T) {
	buf := new(bytes.Buffer)
	spinner := NewSpinner(context.Background(), "Working...", WithOutput(buf))

	// Multiple stops should be safe
	spinner.Stop()
	spinner.Stop()
	spinner.Success("This should not print")

	output := buf.String()
	lines := strings.Split(strings.TrimSpace(output), "\n")
	require.Len(t, lines, 1) // Only the initial message
}

func TestSpinner_SuccessThenFail(t *testing.T) {
	buf := new(bytes.Buffer)
	spinner := NewSpinner(context.Background(), "Working...", WithOutput(buf))

	spinner.Success("Done")
	spinner.Fail("This should not print") // Already stopped

	output := buf.String()
	require.Contains(t, output, "\u2713 Done")
	require.NotContains(t, output, "\u2717")
}

func TestSpinner_UpdateMessage_NonTTY(t *testing.T) {
	buf := new(bytes.Buffer)
	spinner := NewSpinner(context.Background(), "Initial message...", WithOutput(buf))

	// UpdateMessage in non-TTY mode is a no-op (no program running)
	spinner.UpdateMessage("Updated message")
	spinner.Success("Done")

	output := buf.String()
	require.Contains(t, output, "Initial message...")
	require.Contains(t, output, "\u2713 Done")
}
