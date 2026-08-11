// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// Package cmdstatus marks commands with a maturity status (experimental or
// beta) in a structured way, replacing ad-hoc help-text markers such as
// "!!EXPERIMENTAL!!".
//
// Marking a command does three things, all from one call:
//
//   - `--help` renders a consistent notice explaining what the status means,
//     above the command's own description (see the help template in root.go).
//   - `--print-tree` exposes status and status_note fields on the command,
//     so documentation and other tooling can surface the status (for
//     example, as a page badge) without parsing help text.
//   - Subcommands inherit the status of their nearest marked ancestor, so
//     marking a command marks its whole subtree.
//
// GA commands are simply unmarked. Deprecation is not a status here: use
// cobra's native Deprecated field, which already changes runtime behavior.
//
// The defined statuses:
//
//   - Experimental: the interface and behavior may change or be removed in
//     any release without notice, and the command is not covered by support
//     SLAs. Use for commands whose shape is still being discovered.
//   - Beta: the interface is stabilizing and incompatible changes are
//     unlikely but still possible; support is best-effort and feedback is
//     encouraged. Use for commands on a path to GA.
//
// Managed plugins (rpk connect, rpk ai, ...) build their own command trees
// in their own repositories; they can adopt the same annotation keys and
// their statuses flow through the merged tree unchanged.
package cmdstatus

import (
	"fmt"

	"github.com/spf13/cobra"
)

// Annotation keys, on cobra.Command.Annotations. The values are stable
// public API for tooling that reads --print-tree or the annotations
// directly; change them only with a migration plan.
const (
	AnnotationStatus     = "redpanda.com/status"
	AnnotationStatusNote = "redpanda.com/status-note"
)

// The defined statuses. See the package documentation for their semantics.
const (
	StatusExperimental = "experimental"
	StatusBeta         = "beta"
)

// MarkExperimental marks cmd and its subtree as experimental. The optional
// note adds command-specific detail to the standard notice (pass "" for
// none).
func MarkExperimental(cmd *cobra.Command, note string) {
	mark(cmd, StatusExperimental, note)
}

// MarkBeta marks cmd and its subtree as beta. The optional note adds
// command-specific detail to the standard notice (pass "" for none).
func MarkBeta(cmd *cobra.Command, note string) {
	mark(cmd, StatusBeta, note)
}

func mark(cmd *cobra.Command, status, note string) {
	if cmd.Annotations == nil {
		cmd.Annotations = map[string]string{}
	}
	cmd.Annotations[AnnotationStatus] = status
	if note != "" {
		cmd.Annotations[AnnotationStatusNote] = note
	}
}

// Get returns the status and note for cmd, inheriting from the nearest
// marked ancestor. Both are empty for GA (unmarked) commands.
func Get(cmd *cobra.Command) (status, note string) {
	for c := cmd; c != nil; c = c.Parent() {
		if s, ok := c.Annotations[AnnotationStatus]; ok {
			return s, c.Annotations[AnnotationStatusNote]
		}
	}
	return "", ""
}

// Banner returns the standard help notice for a status, with the optional
// note appended, or "" for an empty status. Unknown statuses render a
// generic notice rather than being dropped, so a typo'd status is visible
// instead of silently unmarked.
func Banner(status, note string) string {
	if status == "" {
		return ""
	}
	var b string
	switch status {
	case StatusExperimental:
		b = "This command is EXPERIMENTAL. Its interface and behavior may change or be removed in any release without notice, and it is not covered by support SLAs."
	case StatusBeta:
		b = "This command is in BETA. Its interface is stabilizing and incompatible changes are unlikely but still possible; support is best-effort and feedback is encouraged."
	default:
		b = fmt.Sprintf("This command has status %q.", status)
	}
	if note != "" {
		b += " " + note
	}
	return b
}

// HelpBanner is the cobra template function backing the help template: it
// returns the rendered banner for a command, or "" when unmarked.
func HelpBanner(cmd *cobra.Command) string {
	return Banner(Get(cmd))
}
