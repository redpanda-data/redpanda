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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestQualifySubject(t *testing.T) {
	tests := []struct {
		name      string
		schemaCtx string
		subject   string
		want      string
	}{
		{"empty context", "", "topic", "topic"},
		{"with context", ".myctx", "topic", ":.myctx:topic"},
		{"default context", ".", "topic", ":.:" + "topic"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, QualifySubject(tt.schemaCtx, tt.subject))
		})
	}
}

func TestContextSubjectPrefix(t *testing.T) {
	require.Equal(t, "", ContextSubjectPrefix(""))
	require.Equal(t, ":.myctx:", ContextSubjectPrefix(".myctx"))
	require.Equal(t, ":.:", ContextSubjectPrefix("."))
}

func TestStripContextQualifier(t *testing.T) {
	tests := []struct {
		name      string
		schemaCtx string
		subject   string
		want      string
	}{
		{"empty context", "", "topic", "topic"},
		{"with matching prefix", ".myctx", ":.myctx:topic", "topic"},
		{"no matching prefix", ".myctx", "topic", "topic"},
		{"different context prefix", ".myctx", ":.other:topic", ":.other:topic"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, StripContextQualifier(tt.schemaCtx, tt.subject))
		})
	}
}

func TestParseSubjectContext(t *testing.T) {
	tests := []struct {
		name     string
		subject  string
		wantCtx  string
		wantBare string
	}{
		{"plain subject", "my-topic", "", "my-topic"},
		{"qualified subject", ":.test:my-topic", ".test", "my-topic"},
		{"default context", ":.:my-topic", ".", "my-topic"},
		{"colon but no second colon", ":broken", "", ":broken"},
		{"empty subject", "", "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotCtx, gotBare := ParseSubjectContext(tt.subject)
			require.Equal(t, tt.wantCtx, gotCtx)
			require.Equal(t, tt.wantBare, gotBare)
		})
	}
}

func TestDisplayContext(t *testing.T) {
	tests := []struct {
		name string
		ctx  string
		want string
	}{
		{"empty", "", "default"},
		{"dot", ".", "default"},
		{"named", ".test", ".test"},
		{"other", ".myctx", ".myctx"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, DisplayContext(tt.ctx))
		})
	}
}
