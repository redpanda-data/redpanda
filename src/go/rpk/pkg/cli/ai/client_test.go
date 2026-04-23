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
        "%s": {"path": "https://dl.example.com/rpai_0.1.0.tar.gz", "sha256": "aaaa"}
      }
    },
    {
      "version": "0.2.0",
      "is_latest": true,
      "artifacts": {
        "%s": {"path": "https://dl.example.com/rpai_0.2.0.tar.gz", "sha256": "bbbb"}
      }
    }
  ]
}`

func serveManifest(t *testing.T, body string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/rpai/manifest.json", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, body)
	}))
}

func TestManifest_LatestArtifact(t *testing.T) {
	osArch := runtime.GOOS + "-" + runtime.GOARCH
	ts := serveManifest(t, fmt.Sprintf(sampleManifest, osArch, osArch))
	defer ts.Close()

	t.Setenv("RPK_PLUGIN_REPOSITORY", ts.URL)

	cl, err := newRepoClient()
	require.NoError(t, err)
	manifest, err := cl.Manifest(t.Context())
	require.NoError(t, err)

	art, version, err := manifest.LatestArtifact()
	require.NoError(t, err)
	require.Equal(t, "0.2.0", version)
	require.Equal(t, "https://dl.example.com/rpai_0.2.0.tar.gz", art.Path)
	require.Equal(t, "bbbb", art.Sha256)
}

func TestManifest_ArtifactVersion(t *testing.T) {
	osArch := runtime.GOOS + "-" + runtime.GOARCH
	ts := serveManifest(t, fmt.Sprintf(sampleManifest, osArch, osArch))
	defer ts.Close()

	t.Setenv("RPK_PLUGIN_REPOSITORY", ts.URL)

	cl, err := newRepoClient()
	require.NoError(t, err)
	manifest, err := cl.Manifest(t.Context())
	require.NoError(t, err)

	art, err := manifest.ArtifactVersion("0.1.0")
	require.NoError(t, err)
	require.Equal(t, "aaaa", art.Sha256)

	_, err = manifest.ArtifactVersion("9.9.9")
	require.Error(t, err)
	require.Contains(t, err.Error(), `unable to find version "9.9.9"`)
}

func TestManifest_MissingOSArch(t *testing.T) {
	// Use a bogus os-arch so the current runtime key is never present.
	ts := serveManifest(t, fmt.Sprintf(sampleManifest, "plan9-mips", "plan9-mips"))
	defer ts.Close()

	t.Setenv("RPK_PLUGIN_REPOSITORY", ts.URL)

	cl, err := newRepoClient()
	require.NoError(t, err)
	manifest, err := cl.Manifest(t.Context())
	require.NoError(t, err)

	_, _, err = manifest.LatestArtifact()
	require.Error(t, err)
	require.Contains(t, err.Error(), "no artifact found for os-arch")

	_, err = manifest.ArtifactVersion("0.1.0")
	require.Error(t, err)
	require.Contains(t, err.Error(), "no artifact found for os-arch")
}

func TestManifest_NoLatest(t *testing.T) {
	body := `{"archives":[{"version":"0.1.0","is_latest":false,"artifacts":{}}]}`
	ts := serveManifest(t, body)
	defer ts.Close()

	t.Setenv("RPK_PLUGIN_REPOSITORY", ts.URL)

	cl, err := newRepoClient()
	require.NoError(t, err)
	manifest, err := cl.Manifest(t.Context())
	require.NoError(t, err)

	_, _, err = manifest.LatestArtifact()
	require.Error(t, err)
	require.Contains(t, err.Error(), "no latest artifact found")
}

func TestGetPluginURL_RespectsEnv(t *testing.T) {
	t.Setenv("RPK_PLUGIN_REPOSITORY", "https://staging.example.com")
	require.Equal(t, "https://staging.example.com", getPluginURL())

	t.Setenv("RPK_PLUGIN_REPOSITORY", "")
	require.Equal(t, pluginBaseURL, getPluginURL())
}

func TestValidateVersion(t *testing.T) {
	cases := []struct {
		in      string
		wantErr bool
	}{
		{"latest", false},
		{"0.1.2", false},
		{"v0.1.2", false},
		{"1.2.3-rc1", false}, // suffix after patch is allowed by the regex
		{"0.1", true},
		{"foo", true},
		{"", true},
	}
	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			err := validateVersion(c.in)
			if c.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
