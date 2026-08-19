// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package cmdstatus

import (
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

func TestMarkAndGet(t *testing.T) {
	cmd := &cobra.Command{Use: "thing"}
	s, n := Get(cmd)
	require.Empty(t, s, "unmarked commands must have no status")
	require.Empty(t, n)

	MarkExperimental(cmd, "The output format is still in flux.")
	s, n = Get(cmd)
	require.Equal(t, StatusExperimental, s)
	require.Equal(t, "The output format is still in flux.", n)
}

func TestSubtreeInheritance(t *testing.T) {
	parent := &cobra.Command{Use: "parent"}
	child := &cobra.Command{Use: "child"}
	grandchild := &cobra.Command{Use: "grandchild"}
	child.AddCommand(grandchild)
	parent.AddCommand(child)

	MarkBeta(parent, "")
	s, _ := Get(grandchild)
	require.Equal(t, StatusBeta, s, "status must inherit through the subtree")

	// A more specific mark on a descendant wins.
	MarkExperimental(grandchild, "")
	s, _ = Get(grandchild)
	require.Equal(t, StatusExperimental, s)
	s, _ = Get(child)
	require.Equal(t, StatusBeta, s)
}

func TestBanner(t *testing.T) {
	require.Empty(t, Banner("", ""), "GA renders no banner")
	require.Contains(t, Banner(StatusExperimental, ""), "EXPERIMENTAL")
	require.Contains(t, Banner(StatusTechPreview, ""), "TECHNICAL PREVIEW")
	require.Contains(t, Banner(StatusBeta, ""), "BETA")
	require.Contains(t, Banner(StatusLimitedAvailability, ""), "LIMITED AVAILABILITY")
	require.Contains(t, Banner(StatusExperimental, "Extra detail."), "Extra detail.")
	require.Contains(t, Banner("previeww", ""), `"previeww"`, "unknown statuses must be visible, not dropped")
}

func TestEveryDefinedStatusHasANotice(t *testing.T) {
	for _, status := range []string{StatusExperimental, StatusTechPreview, StatusBeta, StatusLimitedAvailability} {
		require.Contains(t, notices, status)
		require.Contains(t, notices[status], "This command", "notices follow the standard shape (%s)", status)
	}
}

func TestMarkPanicsOnUndefinedStatus(t *testing.T) {
	cmd := &cobra.Command{Use: "thing"}
	require.Panics(t, func() { Mark(cmd, "previeww", "") },
		"a typo'd status must fail at registration, not render generically")
	MarkTechPreview(cmd, "")
	s, _ := Get(cmd)
	require.Equal(t, StatusTechPreview, s)
	MarkLimitedAvailability(cmd, "")
	s, _ = Get(cmd)
	require.Equal(t, StatusLimitedAvailability, s)
}

func TestHelpRendersBanner(t *testing.T) {
	// Mirrors the help template installed in root.go.
	const helpTemplate = `{{with statusBanner .}}{{.}}

{{end}}{{with (or .Long .Short)}}{{. | trimTrailingWhitespaces}}

{{end}}{{if or .Runnable .HasSubCommands}}{{.UsageString}}{{end}}`

	cobra.AddTemplateFunc("statusBanner", HelpBanner)

	root := &cobra.Command{Use: "rpk"}
	sub := &cobra.Command{
		Use:   "thing",
		Short: "Do a thing",
		Long:  "Do a thing with detail.",
		Run:   func(*cobra.Command, []string) {},
	}
	root.AddCommand(sub)
	root.SetHelpTemplate(helpTemplate)

	var out strings.Builder
	root.SetOut(&out)
	root.SetArgs([]string{"thing", "--help"})
	require.NoError(t, root.Execute())
	require.Contains(t, out.String(), "Do a thing with detail.")
	require.NotContains(t, out.String(), "EXPERIMENTAL", "unmarked commands must render no banner")

	MarkExperimental(sub, "")
	out.Reset()
	root.SetArgs([]string{"thing", "--help"})
	require.NoError(t, root.Execute())
	require.Contains(t, out.String(), "This command is EXPERIMENTAL.")
	require.Contains(t, out.String(), "Do a thing with detail.", "the banner must not replace the description")
	require.Less(t,
		strings.Index(out.String(), "EXPERIMENTAL"),
		strings.Index(out.String(), "Do a thing with detail."),
		"the banner renders above the description")
}
