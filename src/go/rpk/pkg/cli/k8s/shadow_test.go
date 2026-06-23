// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package k8s

import (
	"path/filepath"
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/plugin"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

func writeExec(t *testing.T, fs afero.Fs, path, contents string) {
	t.Helper()
	require.NoError(t, fs.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, afero.WriteFile(fs, path, []byte(contents), 0o755))
}

// TestResolution_SelfManagedShadowsManaged documents the shadowing the
// hardening guards against: with both a self-managed .rpk-k8s and the
// rpk-managed .rpk.managed-k8s in the same directory, the self-managed copy
// sorts first and wins resolution, demoting the managed binary to a shadow.
func TestResolution_SelfManagedShadowsManaged(t *testing.T) {
	t.Setenv("HOME", "/home/testuser")
	t.Setenv("PATH", "")
	binDir := "/home/testuser/.local/bin"

	fs := afero.NewMemMapFs()
	writeExec(t, fs, filepath.Join(binDir, ".rpk-k8s"), "self-managed")
	writeExec(t, fs, filepath.Join(binDir, ".rpk.managed-k8s"), "managed")

	k, ok := plugin.ListPlugins(fs, plugin.UserPaths()).Find(pluginSlug)
	require.True(t, ok)
	require.False(t, k.Managed, "self-managed copy should win resolution")
	require.Equal(t, filepath.Join(binDir, ".rpk-k8s"), k.Path)
	require.Contains(t, k.ShadowedPaths, filepath.Join(binDir, ".rpk.managed-k8s"))
}

// TestShadowingSelfManaged covers the install-time warning predicate: after
// installing the managed binary, a self-managed copy that shadows it must be
// reported so the user knows `rpk k8s` will still run the stale binary.
func TestShadowingSelfManaged(t *testing.T) {
	t.Setenv("HOME", "/home/testuser")
	t.Setenv("PATH", "")
	binDir := "/home/testuser/.local/bin"

	t.Run("self-managed shadows freshly installed managed", func(t *testing.T) {
		fs := afero.NewMemMapFs()
		writeExec(t, fs, filepath.Join(binDir, ".rpk-k8s"), "self-managed")
		writeExec(t, fs, filepath.Join(binDir, ".rpk.managed-k8s"), "managed")

		path, shadowed := shadowingSelfManaged(fs)
		require.True(t, shadowed)
		require.Equal(t, filepath.Join(binDir, ".rpk-k8s"), path)
	})

	t.Run("only managed present is not shadowed", func(t *testing.T) {
		fs := afero.NewMemMapFs()
		writeExec(t, fs, filepath.Join(binDir, ".rpk.managed-k8s"), "managed")

		_, shadowed := shadowingSelfManaged(fs)
		require.False(t, shadowed)
	})

	t.Run("no plugin present is not shadowed", func(t *testing.T) {
		fs := afero.NewMemMapFs()
		_, shadowed := shadowingSelfManaged(fs)
		require.False(t, shadowed)
	})
}

// TestRefuseSelfManaged covers the uninstall guard: rpk refuses to delete a
// plugin it did not install unless --force is given, but always proceeds for
// the rpk-managed binary.
func TestRefuseSelfManaged(t *testing.T) {
	managed := &plugin.Plugin{Path: "/b/.rpk.managed-k8s", Managed: true}
	self := &plugin.Plugin{Path: "/b/.rpk-k8s", Managed: false}

	require.Empty(t, refuseSelfManaged(managed, false), "managed plugin removed without --force")
	require.Empty(t, refuseSelfManaged(managed, true))
	require.NotEmpty(t, refuseSelfManaged(self, false), "self-managed refused without --force")
	require.Empty(t, refuseSelfManaged(self, true), "--force overrides the self-managed refusal")
}
