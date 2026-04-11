// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package schemaregistry

import (
	"strings"
)

// QualifySubject returns the context-qualified form of a subject name.
// For example, QualifySubject(".myctx", "topic") returns ":.myctx:topic".
// When schemaCtx is empty the subject is returned unchanged.
func QualifySubject(schemaCtx, subject string) string {
	if schemaCtx == "" {
		return subject
	}
	return ":" + schemaCtx + ":" + subject
}

// ContextSubjectPrefix returns the prefix string used in the subjectPrefix
// query parameter to filter subjects belonging to a given context.
// For example, ContextSubjectPrefix(".myctx") returns ":.myctx:".
// Returns "" when schemaCtx is empty.
func ContextSubjectPrefix(schemaCtx string) string {
	if schemaCtx == "" {
		return ""
	}
	return ":" + schemaCtx + ":"
}

// StripContextQualifier removes the ":{ctx}:" prefix from a subject name.
// For example, StripContextQualifier(".myctx", ":.myctx:topic") returns "topic".
// If the subject does not start with the expected prefix, it is returned unchanged.
// When schemaCtx is empty the subject is returned unchanged.
func StripContextQualifier(schemaCtx, subject string) string {
	if schemaCtx == "" {
		return subject
	}
	pfx := ContextSubjectPrefix(schemaCtx)
	return strings.TrimPrefix(subject, pfx)
}

// ParseSubjectContext extracts the context and bare subject from a raw
// context-qualified subject name. For example:
//
//	":.test:my-topic" → (".test", "my-topic")
//	"plain-topic"     → ("", "plain-topic")
func ParseSubjectContext(subject string) (schemaCtx, bare string) {
	rest, ok := strings.CutPrefix(subject, ":")
	if !ok {
		return "", subject
	}
	// Format is ":{ctx}:{subject}".
	ctx, bare, ok := strings.Cut(rest, ":")
	if !ok {
		return "", subject
	}
	return ctx, bare
}

// DisplayContext returns a human-readable context name. Empty string and "."
// are shown as "default"; all other values are returned as-is.
func DisplayContext(schemaCtx string) string {
	if schemaCtx == "" || schemaCtx == "." {
		return "default"
	}
	return schemaCtx
}
