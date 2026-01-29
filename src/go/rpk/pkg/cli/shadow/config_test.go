// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package shadow

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"
)

// findRedpandaRepoRoot searches upward for the *Redpanda* repository root by
// looking for the MODULE.bazel file.
func findRedpandaRepoRoot(t *testing.T) string {
	// Try from current working directory first. It has to be found from WD to
	// work with Bazel tests.
	dir, err := os.Getwd()
	require.NoError(t, err)
	for {
		if _, err := os.Stat(filepath.Join(dir, "MODULE.bazel")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("failed to find repo root (MODULE.bazel file)")
		}
		dir = parent
	}
}

func TestToScreamingSnakeCase(t *testing.T) {
	tests := []struct {
		name  string
		input string
		exp   string
	}{
		{"simple two words", "PatternType", "PATTERN_TYPE"},
		{"acronym followed by word", "ACLResource", "ACL_RESOURCE"},
		{"acronym followed by multiple words", "ACLPermissionType", "ACL_PERMISSION_TYPE"},
		{"three words", "ShadowLinkState", "SHADOW_LINK_STATE"},
		{"three words with acronym", "ShadowACLState", "SHADOW_ACL_STATE"},
		{"acronym only", "ACL", "ACL"},
		{"single word", "State", "STATE"},
		{"empty string", "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := toScreamingSnakeCase(tt.input)
			require.Equal(t, tt.exp, got)
		})
	}
}

// TestBufGenYamlCoreModuleVersion ensures the buf.gen.yaml module version
// matches the go.mod dependency version for buf.build/gen/go/redpandadata/core.
// When updating the core proto dependency in go.mod, developers must also run
// `buf generate` to regenerate the proto comments.
func TestBufGenYamlCoreModuleVersion(t *testing.T) {
	repoRoot := findRedpandaRepoRoot(t)

	// Read buf.gen.yaml and extract the commit from:
	// module: buf.build/redpandadata/core:<commit>
	bufGenPath := filepath.Join(repoRoot, "buf.gen.yaml")
	bufGenContent, err := os.ReadFile(bufGenPath)
	require.NoError(t, err, "failed to read buf.gen.yaml")

	bufGenRe := regexp.MustCompile(`module:\s*buf\.build/redpandadata/core:([a-f0-9]{12})`)
	bufGenMatch := bufGenRe.FindSubmatch(bufGenContent)
	require.NotNil(t, bufGenMatch, "failed to find module version in buf.gen.yaml")
	bufGenCommit := string(bufGenMatch[1])

	// Read go.mod and extract the commit from:
	// buf.build/gen/go/redpandadata/core/protocolbuffers/go v1.36.11-20251218215858-<commit>.1
	goModPath := filepath.Join(repoRoot, "src", "go", "rpk", "go.mod")
	goModContent, err := os.ReadFile(goModPath)
	require.NoError(t, err, "failed to read go.mod")

	// The version format is: v<major>.<minor>.<patch>-<timestamp>-<commit>.<build>
	// We need to extract the 12-char commit hash before the final .<build> suffix.
	goModRe := regexp.MustCompile(`buf\.build/gen/go/redpandadata/core/protocolbuffers/go\s+v[0-9]+\.[0-9]+\.[0-9]+-[0-9]{14}-([a-f0-9]{12})\.[0-9]+`)
	goModMatch := goModRe.FindSubmatch(goModContent)
	require.NotNil(t, goModMatch, "failed to find buf.build/gen/go/redpandadata/core/protocolbuffers/go version in go.mod")
	goModCommit := string(goModMatch[1])

	require.Equal(t, goModCommit, bufGenCommit,
		"buf.gen.yaml module version (%s) does not match go.mod dependency version (%s). "+
			"After updating the buf.build/gen/go/redpandadata/core/protocolbuffers/go dependency in go.mod, "+
			"update buf.gen.yaml module commit hash and run 'buf generate' from the repo root to regenerate proto comments.",
		bufGenCommit, goModCommit)
}
