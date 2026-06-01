// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package generate

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

// buildSkillsTarball builds an in-memory gzip+tar mimicking a GitHub repo
// tarball: every file is nested under "<repo>-<branch>/".
func buildSkillsTarball(t *testing.T, files map[string]string) []byte {
	t.Helper()
	var buf bytes.Buffer
	gzw := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gzw)
	for name, content := range files {
		hdr := &tar.Header{
			Name:     "skills-main/" + name,
			Typeflag: tar.TypeReg,
			Mode:     0o644,
			Size:     int64(len(content)),
		}
		require.NoError(t, tw.WriteHeader(hdr))
		_, err := tw.Write([]byte(content))
		require.NoError(t, err)
	}
	require.NoError(t, tw.Close())
	require.NoError(t, gzw.Close())
	return buf.Bytes()
}

func TestSkillExtract(t *testing.T) {
	files := map[string]string{
		"README.md":                        "top-level readme, ignored",
		"skills/rpk/SKILL.md":              "rpk skill",
		"skills/rpk/references/topics.md":  "rpk reference",
		"skills/rpk-topic/SKILL.md":        "rpk-topic skill",
		"skills/streaming/SKILL.md":        "streaming skill",
		"skills/sql/references/queries.md": "sql reference",
	}

	rpkOnly := func(n string) bool { return wantSkill(n, false) }
	allSkills := func(n string) bool { return wantSkill(n, true) }

	parse := func(t *testing.T) skillCatalog {
		t.Helper()
		catalog, err := parseSkills(bytes.NewReader(buildSkillsTarball(t, files)))
		require.NoError(t, err)
		return catalog
	}

	t.Run("default installs only rpk skills", func(t *testing.T) {
		fs := afero.NewMemMapFs()
		got, err := extractSkills(fs, parse(t), "/dest", rpkOnly)
		require.NoError(t, err)
		require.Equal(t, []string{"rpk", "rpk-topic"}, got)

		// rpk files land at the expected paths with their content.
		b, err := afero.ReadFile(fs, filepath.Join("/dest", "rpk", "SKILL.md"))
		require.NoError(t, err)
		require.Equal(t, "rpk skill", string(b))
		b, err = afero.ReadFile(fs, filepath.Join("/dest", "rpk", "references", "topics.md"))
		require.NoError(t, err)
		require.Equal(t, "rpk reference", string(b))

		// Non-rpk skills are not installed.
		exists, err := afero.DirExists(fs, filepath.Join("/dest", "streaming"))
		require.NoError(t, err)
		require.False(t, exists)
		exists, err = afero.DirExists(fs, filepath.Join("/dest", "sql"))
		require.NoError(t, err)
		require.False(t, exists)
	})

	t.Run("all installs every skill", func(t *testing.T) {
		fs := afero.NewMemMapFs()
		got, err := extractSkills(fs, parse(t), "/dest", allSkills)
		require.NoError(t, err)
		require.Equal(t, []string{"rpk", "rpk-topic", "sql", "streaming"}, got)
	})

	t.Run("membership predicate installs exactly that set", func(t *testing.T) {
		fs := afero.NewMemMapFs()
		want := map[string]bool{"rpk-topic": true, "sql": true}
		got, err := extractSkills(fs, parse(t), "/dest", func(n string) bool { return want[n] })
		require.NoError(t, err)
		require.Equal(t, []string{"rpk-topic", "sql"}, got)
	})

	t.Run("no match returns empty, no error", func(t *testing.T) {
		fs := afero.NewMemMapFs()
		got, err := extractSkills(fs, parse(t), "/dest", func(string) bool { return false })
		require.NoError(t, err)
		require.Empty(t, got)
	})

	t.Run("stale files are cleared", func(t *testing.T) {
		fs := afero.NewMemMapFs()
		stale := filepath.Join("/dest", "rpk", "old.md")
		require.NoError(t, afero.WriteFile(fs, stale, []byte("stale"), 0o644))

		_, err := extractSkills(fs, parse(t), "/dest", rpkOnly)
		require.NoError(t, err)

		exists, err := afero.Exists(fs, stale)
		require.NoError(t, err)
		require.False(t, exists)
	})
}

func TestSkillList(t *testing.T) {
	files := map[string]string{
		"README.md":                        "ignored",
		"skills/rpk/SKILL.md":              "rpk skill",
		"skills/rpk/references/topics.md":  "rpk reference",
		"skills/rpk-topic/SKILL.md":        "rpk-topic skill",
		"skills/streaming/SKILL.md":        "streaming skill",
		"skills/sql/references/queries.md": "sql reference",
	}
	catalog, err := parseSkills(bytes.NewReader(buildSkillsTarball(t, files)))
	require.NoError(t, err)
	require.Equal(t, []string{"rpk", "rpk-topic", "sql", "streaming"}, catalog.names())
}

func TestSkillMatch(t *testing.T) {
	available := []string{"rpk", "rpk-topic", "rpk-group", "sql", "streaming"}
	for _, tc := range []struct {
		name    string
		pattern string
		want    []string
	}{
		{"regex matches subset", `rpk-.*`, []string{"rpk-topic", "rpk-group"}},
		{"anchored alternation", `^rpk-(topic|group)$`, []string{"rpk-topic", "rpk-group"}},
		{"no match returns empty", `nomatch-xyz`, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := matchSkills(available, regexp.MustCompile(tc.pattern))
			require.Equal(t, tc.want, got)
		})
	}
}

func TestResolveSkillMatch(t *testing.T) {
	available := []string{"rpk", "rpk-topic", "sql", "streaming"}
	for _, tc := range []struct {
		name        string
		skills      string
		all         bool
		wantErr     bool
		wantInstall bool     // expected install return
		wantMatch   []string // names in available the predicate should select
	}{
		{
			name:        "default uses rpk filter",
			wantInstall: true,
			wantMatch:   []string{"rpk", "rpk-topic"},
		},
		{
			name:        "default with all installs everything",
			all:         true,
			wantInstall: true,
			wantMatch:   []string{"rpk", "rpk-topic", "sql", "streaming"},
		},
		{
			name:        "regex selects matching skills",
			skills:      `^rpk-`,
			wantInstall: true,
			wantMatch:   []string{"rpk-topic"},
		},
		{
			name:    "invalid regex errors",
			skills:  "(",
			wantErr: true,
		},
		{
			name:    "no regex match errors",
			skills:  "nomatch-xyz",
			wantErr: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			match, install, err := resolveSkillMatch(tc.skills, tc.all, available)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantInstall, install)

			want := make(map[string]bool, len(tc.wantMatch))
			for _, n := range tc.wantMatch {
				want[n] = true
			}
			for _, n := range available {
				require.Equal(t, want[n], match(n), "match(%q)", n)
			}
		})
	}
}

func TestSkillProvider(t *testing.T) {
	t.Run("default provider is claude", func(t *testing.T) {
		prov, ok := skillProviders[defaultProvider]
		require.True(t, ok)
		require.Equal(t, "claude", prov.name)

		home, err := os.UserHomeDir()
		require.NoError(t, err)
		dir, err := prov.defaultDir()
		require.NoError(t, err)
		require.Equal(t, filepath.Join(home, ".claude", "skills"), dir)
	})

	t.Run("unknown provider is not registered", func(t *testing.T) {
		_, ok := skillProviders["bogus"]
		require.False(t, ok)
	})

	t.Run("sorted provider names", func(t *testing.T) {
		require.Equal(t, []string{"claude"}, sortedProviderNames())
	})
}
