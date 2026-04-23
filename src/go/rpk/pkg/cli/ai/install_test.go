// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package ai

import (
	"archive/tar"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

// fakeRpaiTarGz returns a .tar.gz archive containing a single rpai-binary
// file, plus the sha256 (hex) of the inner binary.
func fakeRpaiTarGz(t *testing.T, inner []byte) ([]byte, string) {
	t.Helper()
	var buf bytesBuffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	require.NoError(t, tw.WriteHeader(&tar.Header{
		Name: "rpai",
		Mode: 0o755,
		Size: int64(len(inner)),
	}))
	_, err := tw.Write(inner)
	require.NoError(t, err)
	require.NoError(t, tw.Close())
	require.NoError(t, gz.Close())

	sum := sha256.Sum256(inner)
	return buf.Bytes(), hex.EncodeToString(sum[:])
}

// bytesBuffer is a minimal alias so the test file doesn't need to import
// bytes in a separate block while keeping the imports tidy.
type bytesBuffer struct{ b []byte }

func (b *bytesBuffer) Write(p []byte) (int, error) { b.b = append(b.b, p...); return len(p), nil }
func (b *bytesBuffer) Bytes() []byte               { return b.b }

// installServer serves /rpai/manifest.json and /rpai.tar.gz matching the
// given binary + sha.
func installServer(t *testing.T, tarGz []byte, sha string) *httptest.Server {
	t.Helper()
	osArch := runtime.GOOS + "-" + runtime.GOARCH
	mux := http.NewServeMux()
	mux.HandleFunc("/rpai.tar.gz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/gzip")
		w.Write(tarGz)
	})
	var manifestBody string
	mux.HandleFunc("/rpai/manifest.json", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, manifestBody)
	})
	ts := httptest.NewServer(mux)
	manifestBody = fmt.Sprintf(`{
  "archives": [{
    "version": "0.2.0",
    "is_latest": true,
    "artifacts": {"%s": {"path": "%s/rpai.tar.gz", "sha256": "%s"}}
  }]
}`, osArch, ts.URL, sha)
	return ts
}

func TestInstallRpai_Download(t *testing.T) {
	t.Setenv("HOME", "/home/testuser")

	innerBin := []byte("fake-rpai-binary")
	tarGz, sha := fakeRpaiTarGz(t, innerBin)
	ts := installServer(t, tarGz, sha)
	defer ts.Close()
	t.Setenv("RPK_PLUGIN_REPOSITORY", ts.URL)

	fs := afero.NewMemMapFs()
	require.NoError(t, fs.MkdirAll("/home/testuser/.local/bin", 0o755))

	path, version, err := installRpai(t.Context(), fs, "latest")
	require.NoError(t, err)
	require.Equal(t, "0.2.0", version)
	require.Equal(t, "/home/testuser/.local/bin/.rpk.managed-rpai", path)

	// Binary landed on disk with the correct contents + mode.
	got, err := afero.ReadFile(fs, path)
	require.NoError(t, err)
	require.Equal(t, innerBin, got)
	info, err := fs.Stat(path)
	require.NoError(t, err)
	require.True(t, info.Mode().Perm()&0o111 != 0, "installed binary should be executable")
}

func TestInstallRpai_VersionLookup(t *testing.T) {
	t.Setenv("HOME", "/home/testuser")

	innerBin := []byte("fake-rpai-binary-v0-1-0")
	tarGz, sha := fakeRpaiTarGz(t, innerBin)

	// Serve a manifest that lists 0.1.0 and 0.2.0, neither marked latest so
	// we must pin to 0.1.0 explicitly.
	osArch := runtime.GOOS + "-" + runtime.GOARCH
	mux := http.NewServeMux()
	mux.HandleFunc("/rpai.tar.gz", func(w http.ResponseWriter, _ *http.Request) {
		w.Write(tarGz)
	})
	var manifestBody string
	mux.HandleFunc("/rpai/manifest.json", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, manifestBody)
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()
	manifestBody = fmt.Sprintf(`{
  "archives": [
    {"version":"0.1.0","is_latest":false,"artifacts":{"%s":{"path":"%s/rpai.tar.gz","sha256":"%s"}}},
    {"version":"0.2.0","is_latest":false,"artifacts":{"%s":{"path":"%s/unused.tar.gz","sha256":"deadbeef"}}}
  ]
}`, osArch, ts.URL, sha, osArch, ts.URL)
	t.Setenv("RPK_PLUGIN_REPOSITORY", ts.URL)

	fs := afero.NewMemMapFs()
	require.NoError(t, fs.MkdirAll("/home/testuser/.local/bin", 0o755))

	_, version, err := installRpai(t.Context(), fs, "0.1.0")
	require.NoError(t, err)
	require.Equal(t, "0.1.0", version)
}

func TestInstallRpai_SHAMismatch(t *testing.T) {
	t.Setenv("HOME", "/home/testuser")

	innerBin := []byte("payload")
	tarGz, _ := fakeRpaiTarGz(t, innerBin)
	// Lie about the sha to force a mismatch.
	ts := installServer(t, tarGz, "0000000000000000")
	defer ts.Close()
	t.Setenv("RPK_PLUGIN_REPOSITORY", ts.URL)

	fs := afero.NewMemMapFs()
	require.NoError(t, fs.MkdirAll("/home/testuser/.local/bin", 0o755))

	_, _, err := installRpai(t.Context(), fs, "latest")
	require.Error(t, err)
	require.True(t,
		strings.Contains(err.Error(), "checksum") || strings.Contains(err.Error(), "does not contain expected"),
		"expected checksum-related error, got: %v", err)
}
