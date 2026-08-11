// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// Package cmdstatus marks commands with a maturity status (experimental,
// technical preview, beta, or limited availability) in a structured way,
// replacing ad-hoc help-text markers such as "!!EXPERIMENTAL!!".
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
// The defined statuses mirror Redpanda's documented feature maturity
// stages (see "Features in beta" and "Features in limited availability" in
// the Redpanda Cloud overview), plus the engineering-level experimental
// marker:
//
//   - Experimental: an engineering statement about stability. The interface
//     and behavior may change or be removed in any release without notice,
//     and the command is not covered by Redpanda Support. Use for commands
//     whose shape is still being discovered.
//   - Technical preview: the earliest product lifecycle stage. Early access
//     for evaluation and feedback; may change significantly, not for
//     production, not covered by Redpanda Support.
//   - Beta: available for testing and feedback. The interface is
//     stabilizing but may still change; not for production and not covered
//     by Redpanda Support.
//   - Limited availability: production-ready and covered by Redpanda
//     Support for early adopters; access may be restricted or require
//     enablement.
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
	StatusExperimental        = "experimental"
	StatusTechPreview         = "tech-preview"
	StatusBeta                = "beta"
	StatusLimitedAvailability = "limited-availability"
)

// notices holds the standard help notice for each defined status. Adding a
// status means adding a constant, a row here, and a Mark helper.
var notices = map[string]string{
	StatusExperimental:        "This command is EXPERIMENTAL. Its interface and behavior may change or be removed in any release without notice, and it is not covered by Redpanda Support.",
	StatusTechPreview:         "This command is in TECHNICAL PREVIEW. It is early access for evaluation and feedback: its interface and behavior may change significantly, it should not be used in production, and it is not covered by Redpanda Support.",
	StatusBeta:                "This command is in BETA. It is available for testing and feedback: its interface is stabilizing but may still change, it should not be used in production, and it is not covered by Redpanda Support.",
	StatusLimitedAvailability: "This command is in LIMITED AVAILABILITY. It is production-ready and covered by Redpanda Support for early adopters; access may be restricted or require enablement.",
}

// MarkExperimental marks cmd and its subtree as experimental. The optional
// note adds command-specific detail to the standard notice (pass "" for
// none).
func MarkExperimental(cmd *cobra.Command, note string) {
	Mark(cmd, StatusExperimental, note)
}

// MarkTechPreview marks cmd and its subtree as a technical preview. The
// optional note adds command-specific detail to the standard notice (pass
// "" for none).
func MarkTechPreview(cmd *cobra.Command, note string) {
	Mark(cmd, StatusTechPreview, note)
}

// MarkBeta marks cmd and its subtree as beta. The optional note adds
// command-specific detail to the standard notice (pass "" for none).
func MarkBeta(cmd *cobra.Command, note string) {
	Mark(cmd, StatusBeta, note)
}

// MarkLimitedAvailability marks cmd and its subtree as limited
// availability. The optional note adds command-specific detail to the
// standard notice (pass "" for none).
func MarkLimitedAvailability(cmd *cobra.Command, note string) {
	Mark(cmd, StatusLimitedAvailability, note)
}

// Mark marks cmd and its subtree with one of the defined statuses. It
// panics on an undefined status: marking happens at command registration,
// so a typo fails the first test or invocation rather than silently
// rendering a generic notice.
func Mark(cmd *cobra.Command, status, note string) {
	if _, ok := notices[status]; !ok {
		panic(fmt.Sprintf("cmdstatus.Mark: undefined status %q on %q", status, cmd.Name()))
	}
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
	b, ok := notices[status]
	if !ok {
		// Annotations can arrive from plugin command trees, so unknown
		// values render visibly instead of being dropped or panicking.
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
