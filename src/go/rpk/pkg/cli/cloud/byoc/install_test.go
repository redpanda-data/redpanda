package byoc

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	byocpluginv1alpha1 "buf.build/gen/go/redpandadata/cloud/protocolbuffers/go/redpanda/api/byocplugin/v1alpha1"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestExtractShaFromLocation(t *testing.T) {
	tests := []struct {
		name     string
		location string
		expSha   string
		expErr   bool
	}{
		{
			name:     "valid location",
			location: "https://fake.com/rpk-plugins/byoc-linux-amd64/versions/24.1.19940510213245-sha256.45234430796c12612342546ef0f8243/byoc-linux-amd64-24.1.22220510213245-sha256.45234430796c12612342546ef0f8243.tar.gz",
			expSha:   "45234430796c12612342546ef0f8243",
		},
		{
			name:     "valid location with 16-char sha",
			location: "https://redpanda.com/plugin.byoc.deadbeef12345678.tar.gz",
			expSha:   "deadbeef12345678",
		},
		{
			name:     "no sha in filename",
			location: "https://redpanda.com/byoc.tar.gz",
			expErr:   true,
		},
		{
			name:     "no .tar.gz suffix",
			location: "https://fake.com/rpk-plugins/byoc-linux-amd64/versions/24.1.19940510213245-sha256.45234430796c12612342546ef0f8243/byoc-linux-amd64-24.1.22220510213245-sha256.45234430796c12612342546ef0f8243",
			expErr:   true,
		},
		{
			name:     "sha too short (15 chars)",
			location: "https://redpanda.com/plugin.byoc.abc123def456ghi.tar.gz",
			expErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sha, err := extractShaFromLocation(tt.location)
			if tt.expErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expSha, sha)
			}
		})
	}
}

func TestEnsurePluginVersion(t *testing.T) {
	t.Setenv("HOME", "/home")

	t.Run("plugin exists with matching sha", func(t *testing.T) {
		fs := afero.NewMemMapFs()
		tmpPluginPath := "/home/.local/bin/.rpk.managed-byoc"
		pluginBin := []byte("plugin-binary")
		expSHA := "7f512da80a46ab9883d159705be"
		_ = afero.WriteFile(fs, tmpPluginPath, pluginBin, 0o755)

		artifacts := []*byocpluginv1alpha1.Artifact{
			{Location: fmt.Sprintf("https://redpanda.dl.com/byoc.%v.tar.gz", expSHA)},
		}
		// stub publicAPI artifact
		ts := httptest.NewServer(artifactHandler(t, artifacts))
		defer ts.Close()
		testCfg, err := testConfig(fs, t, ts.URL)

		path, installed, err := ensurePluginVersion(t.Context(), fs, testCfg, "rp-d", true, "token")
		require.NoError(t, err)
		require.False(t, installed)
		require.Equal(t, tmpPluginPath, path)
	})

	t.Run("plugin exists without matching sha, update", func(t *testing.T) {
		fs := afero.NewMemMapFs()
		tmpPluginPath := "/home/.local/bin/.rpk.managed-byoc"
		pluginBin := []byte("plugin-binary")
		_ = afero.WriteFile(fs, tmpPluginPath, pluginBin, 0o755)

		// stub artifact file
		updatedPluginBin := []byte("plugin-binary-2")
		expSha := "13fc10b8f49a59034fff7f14951"
		ts := httptest.NewServer(fileHandler(updatedPluginBin))
		artifacts := []*byocpluginv1alpha1.Artifact{
			{Location: fmt.Sprintf("%v/byoc.%v.tar.gz", ts.URL, expSha)},
		}

		// stub publicAPI artifact
		ts2 := httptest.NewServer(artifactHandler(t, artifacts))
		defer ts2.Close()
		testCfg, err := testConfig(fs, t, ts2.URL)

		path, installed, err := ensurePluginVersion(t.Context(), fs, testCfg, "rp-d", true, "token")
		require.NoError(t, err)
		require.True(t, installed) // This checks if it was installed.
		require.Equal(t, tmpPluginPath, path)
	})

	t.Run("no plugin, install new", func(t *testing.T) {
		fs := afero.NewMemMapFs()
		fs.MkdirAll("/home/.local/bin", 0o755)

		// stub artifact file
		updatedPluginBin := []byte("plugin-binary-2")
		expSha := "13fc10b8f49a59034fff7f14951"
		ts := httptest.NewServer(fileHandler(updatedPluginBin))
		artifacts := []*byocpluginv1alpha1.Artifact{
			{Location: fmt.Sprintf("%v/byoc.%v.tar.gz", ts.URL, expSha)},
		}

		// stub publicAPI artifact
		ts2 := httptest.NewServer(artifactHandler(t, artifacts))
		defer ts2.Close()
		testCfg, err := testConfig(fs, t, ts2.URL)

		expPluginPath := "/home/.local/bin/.rpk.managed-byoc"
		exists, err := afero.Exists(fs, expPluginPath)
		require.NoError(t, err)
		require.False(t, exists)

		path, installed, err := ensurePluginVersion(t.Context(), fs, testCfg, "rp-d", true, "token")
		require.NoError(t, err)
		require.True(t, installed) // This checks if it was installed.

		require.Equal(t, expPluginPath, path)
		exists, err = afero.Exists(fs, expPluginPath)
		require.NoError(t, err)
		require.True(t, exists)
	})
}

func testConfig(fs afero.Fs, t *testing.T, publicAPIURL string) (*config.Config, error) {
	p := new(config.Params)
	t.Setenv("RPK_PUBLIC_API_URL", publicAPIURL)
	return p.Load(fs)
}

func artifactHandler(t *testing.T, artifacts []*byocpluginv1alpha1.Artifact) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "ListLatestArtifacts") {
			resp := &byocpluginv1alpha1.ListLatestArtifactsResponse{
				Artifacts: artifacts,
			}
			marshal, err := proto.Marshal(resp)
			require.NoError(t, err)
			w.Header().Set("Content-Type", "application/proto")
			w.Write(marshal)
		} else if strings.Contains(r.URL.Path, "ListArtifactsByRedpandaID") {
			resp := &byocpluginv1alpha1.ListArtifactsByRedpandaIDResponse{
				Artifacts: artifacts,
			}
			marshal, err := proto.Marshal(resp)
			require.NoError(t, err)
			w.Header().Set("Content-Type", "application/proto")
			w.Write(marshal)
		}
	}
}

func fileHandler(f []byte) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		gz := gzip.NewWriter(w)
		defer gz.Close()

		tw := tar.NewWriter(gz)
		defer tw.Close()

		hdr := &tar.Header{
			Name: "byoc-plugin",
			Mode: 0o644,
			Size: int64(len(f)),
		}
		tw.WriteHeader(hdr)
		tw.Write(f)
	}
}
