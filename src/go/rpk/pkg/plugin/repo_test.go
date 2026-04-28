// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package plugin

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

const sampleManifest = `{
  "archives": [
    {
      "version": "0.1.0",
      "is_latest": false,
      "artifacts": {
        "%s": {"path": "https://dl.example.com/foo_0.1.0.tar.gz", "sha256": "aaaa"}
      }
    },
    {
      "version": "0.2.0",
      "is_latest": true,
      "artifacts": {
        "%s": {"path": "https://dl.example.com/foo_0.2.0.tar.gz", "sha256": "bbbb"}
      }
    }
  ]
}`

func serveManifest(t *testing.T, slug, body string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/"+slug+"/manifest.json", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, body)
	}))
}

func TestRepoManifest_LatestArtifact(t *testing.T) {
	osArch := runtime.GOOS + "-" + runtime.GOARCH
	ts := serveManifest(t, "foo", fmt.Sprintf(sampleManifest, osArch, osArch))
	defer ts.Close()

	t.Setenv("RPK_PLUGIN_REPOSITORY", ts.URL)

	cl, err := NewRepoClient("foo", "Foo CLI")
	require.NoError(t, err)
	manifest, err := cl.Manifest(t.Context())
	require.NoError(t, err)

	art, version, err := manifest.LatestArtifact("Foo CLI")
	require.NoError(t, err)
	require.Equal(t, "0.2.0", version)
	require.Equal(t, "https://dl.example.com/foo_0.2.0.tar.gz", art.Path)
	require.Equal(t, "bbbb", art.Sha256)
}

func TestRepoManifest_ArtifactVersion(t *testing.T) {
	osArch := runtime.GOOS + "-" + runtime.GOARCH
	ts := serveManifest(t, "foo", fmt.Sprintf(sampleManifest, osArch, osArch))
	defer ts.Close()

	t.Setenv("RPK_PLUGIN_REPOSITORY", ts.URL)

	cl, err := NewRepoClient("foo", "Foo CLI")
	require.NoError(t, err)
	manifest, err := cl.Manifest(t.Context())
	require.NoError(t, err)

	art, err := manifest.ArtifactVersion("Foo CLI", "0.1.0")
	require.NoError(t, err)
	require.Equal(t, "aaaa", art.Sha256)

	_, err = manifest.ArtifactVersion("Foo CLI", "9.9.9")
	require.Error(t, err)
	require.Contains(t, err.Error(), `unable to find version "9.9.9"`)
}

func TestRepoManifest_MissingOSArch(t *testing.T) {
	// Bogus os-arch so the current runtime key is never present.
	ts := serveManifest(t, "foo", fmt.Sprintf(sampleManifest, "plan9-mips", "plan9-mips"))
	defer ts.Close()

	t.Setenv("RPK_PLUGIN_REPOSITORY", ts.URL)

	cl, err := NewRepoClient("foo", "Foo CLI")
	require.NoError(t, err)
	manifest, err := cl.Manifest(t.Context())
	require.NoError(t, err)

	_, _, err = manifest.LatestArtifact("Foo CLI")
	require.Error(t, err)
	require.Contains(t, err.Error(), "no artifact found for os-arch")
	require.Contains(t, err.Error(), "Foo CLI")

	_, err = manifest.ArtifactVersion("Foo CLI", "0.1.0")
	require.Error(t, err)
	require.Contains(t, err.Error(), "no artifact found for os-arch")
}

func TestRepoManifest_NoLatest(t *testing.T) {
	body := `{"archives":[{"version":"0.1.0","is_latest":false,"artifacts":{}}]}`
	ts := serveManifest(t, "foo", body)
	defer ts.Close()

	t.Setenv("RPK_PLUGIN_REPOSITORY", ts.URL)

	cl, err := NewRepoClient("foo", "Foo CLI")
	require.NoError(t, err)
	manifest, err := cl.Manifest(t.Context())
	require.NoError(t, err)

	_, _, err = manifest.LatestArtifact("Foo CLI")
	require.Error(t, err)
	require.Contains(t, err.Error(), "no latest artifact found")
}

func TestRepoBaseURL_RespectsEnv(t *testing.T) {
	t.Setenv("RPK_PLUGIN_REPOSITORY", "https://staging.example.com")
	require.Equal(t, "https://staging.example.com", repoBaseURL())

	t.Setenv("RPK_PLUGIN_REPOSITORY", "")
	require.Equal(t, RepoBaseURL, repoBaseURL())
}
