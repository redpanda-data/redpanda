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
	"archive/tar"
	"bytes"
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

func fakeK8sTarGz(t *testing.T, inner []byte) ([]byte, string) {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	require.NoError(t, tw.WriteHeader(&tar.Header{Name: "rpk-k8s", Mode: 0o755, Size: int64(len(inner))}))
	_, err := tw.Write(inner)
	require.NoError(t, err)
	require.NoError(t, tw.Close())
	require.NoError(t, gz.Close())
	sum := sha256.Sum256(inner)
	return buf.Bytes(), hex.EncodeToString(sum[:])
}

func installServer(t *testing.T, tarGz []byte, sha string) *httptest.Server {
	t.Helper()
	osArch := runtime.GOOS + "-" + runtime.GOARCH
	mux := http.NewServeMux()
	mux.HandleFunc("/k8s.tar.gz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/gzip")
		w.Write(tarGz)
	})
	var manifestBody string
	mux.HandleFunc("/k8s/manifest.json", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, manifestBody)
	})
	ts := httptest.NewServer(mux)
	manifestBody = fmt.Sprintf(`{
  "archives": [{
    "version": "25.3.5",
    "is_latest": true,
    "artifacts": {"%s": {"path": "%s/k8s.tar.gz", "sha256": "%s"}}
  }]
}`, osArch, ts.URL, sha)
	return ts
}

func TestInstallK8s_Download(t *testing.T) {
	t.Setenv("HOME", "/home/testuser")
	innerBin := []byte("fake-rpk-k8s-binary")
	tarGz, sha := fakeK8sTarGz(t, innerBin)
	ts := installServer(t, tarGz, sha)
	defer ts.Close()
	t.Setenv("RPK_PLUGIN_REPOSITORY", ts.URL)

	fs := afero.NewMemMapFs()
	require.NoError(t, fs.MkdirAll("/home/testuser/.local/bin", 0o755))

	path, version, err := installK8sPlugin(t.Context(), fs, "latest")
	require.NoError(t, err)
	require.Equal(t, "25.3.5", version)
	require.Equal(t, "/home/testuser/.local/bin/.rpk.managed-k8s", path)

	got, err := afero.ReadFile(fs, path)
	require.NoError(t, err)
	require.Equal(t, innerBin, got)
}

func TestInstallK8s_SHAMismatch(t *testing.T) {
	t.Setenv("HOME", "/home/testuser")
	innerBin := []byte("payload")
	tarGz, _ := fakeK8sTarGz(t, innerBin)
	ts := installServer(t, tarGz, "0000000000000000")
	defer ts.Close()
	t.Setenv("RPK_PLUGIN_REPOSITORY", ts.URL)

	fs := afero.NewMemMapFs()
	require.NoError(t, fs.MkdirAll("/home/testuser/.local/bin", 0o755))

	_, _, err := installK8sPlugin(t.Context(), fs, "latest")
	require.Error(t, err)
	require.True(t,
		strings.Contains(err.Error(), "checksum") || strings.Contains(err.Error(), "does not contain expected"),
		"expected checksum error, got: %v", err)
}

func TestValidateVersion(t *testing.T) {
	for _, ok := range []string{"latest", "25.3.5", "v25.3.5", "25.3.5-rc1"} {
		require.NoError(t, validateVersion(ok), ok)
	}
	for _, bad := range []string{"", "abc", "25", "garbage"} {
		require.Error(t, validateVersion(bad), bad)
	}
}
