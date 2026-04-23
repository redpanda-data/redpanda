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
	"strings"
	"testing"

	controlplanev1 "buf.build/gen/go/redpandadata/cloud/protocolbuffers/go/redpanda/api/controlplane/v1"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestParseFlags_StripsRpkGlobals(t *testing.T) {
	// Build a minimal `rpk ai ...` command tree so parseFlags sees the
	// same rpk-global flags it would in production. cmd.Flags() only
	// merges inherited flags once cobra begins executing the command —
	// hence root.Execute() below rather than calling parseFlags directly.
	root := &cobra.Command{Use: "rpk"}
	pf := root.PersistentFlags()
	pf.String("config", "", "")
	pf.String("profile", "", "")
	pf.StringArrayP("config-opt", "X", nil, "")
	pf.BoolP("verbose", "v", false, "")

	var got []string
	var gotErr error
	aiCmd := &cobra.Command{
		Use:                "ai",
		DisableFlagParsing: true,
		Args:               cobra.MinimumNArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			got, gotErr = parseFlags(new(config.Params), cmd, args)
		},
	}
	aiCmd.Flags().BoolP("help", "h", false, "")
	root.AddCommand(aiCmd)

	root.SetArgs([]string{"ai", "--config", "/foo", "-X", "cloud_token=abc", "llm", "list", "--foo=bar"})
	require.NoError(t, root.Execute())
	require.NoError(t, gotErr)
	require.Equal(t, []string{"llm", "list", "--foo=bar"}, got)
}

func TestNeedsCloudContext(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want bool
	}{
		{"empty", nil, false},
		{"help long", []string{"--help"}, false},
		{"help short", []string{"-h"}, false},
		{"version", []string{"--version"}, false},
		{"only unknown flags", []string{"--foo", "--bar=baz"}, false},
		{"subcommand", []string{"llm", "list"}, true},
		{"subcommand after flags", []string{"--format", "json", "llm", "list"}, true},
		{"help with subcommand still skips", []string{"llm", "list", "--help"}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require.Equal(t, c.want, needsCloudContext(c.args))
		})
	}
}

func TestHasRpaiEndpointFlag(t *testing.T) {
	cases := []struct {
		args []string
		want bool
	}{
		{nil, false},
		{[]string{"llm", "list"}, false},
		{[]string{"--rpai-endpoint", "https://foo"}, true},
		{[]string{"--rpai-endpoint=https://foo"}, true},
		{[]string{"llm", "list", "--rpai-endpoint=https://foo"}, true},
		// Not a match: prefix only, same-flag-family shouldn't false-positive.
		{[]string{"--rpai-endpoint-other"}, false},
	}
	for _, c := range cases {
		t.Run(strings.Join(c.args, " "), func(t *testing.T) {
			require.Equal(t, c.want, hasRpaiEndpointFlag(c.args))
		})
	}
}

func TestGetTokenOrLogin_UsesDevOverride(t *testing.T) {
	t.Setenv("RPK_CLOUD_TOKEN", "dev-token-123")

	fs := afero.NewMemMapFs()
	p := new(config.Params)
	cfg, err := p.Load(fs)
	require.NoError(t, err)

	tok, err := getTokenOrLogin(t.Context(), fs, cfg)
	require.NoError(t, err)
	require.Equal(t, "dev-token-123", tok)
}

// clusterHandler stubs the public-API GetCluster endpoint to return the given
// cluster. If cluster is nil, it returns a cluster with no AI Gateway set.
func clusterHandler(t *testing.T, cluster *controlplanev1.Cluster) http.HandlerFunc {
	t.Helper()
	return func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "ClusterService/GetCluster") {
			http.NotFound(w, r)
			return
		}
		resp := &controlplanev1.GetClusterResponse{Cluster: cluster}
		marshal, err := proto.Marshal(resp)
		require.NoError(t, err)
		w.Header().Set("Content-Type", "application/proto")
		w.Write(marshal)
	}
}

func TestResolveAigwEndpoint_NoClusterSelected(t *testing.T) {
	ts := httptest.NewServer(clusterHandler(t, nil))
	defer ts.Close()

	// No profile written.
	t.Setenv("RPK_PUBLIC_API_URL", ts.URL)
	fs := afero.NewMemMapFs()
	p := new(config.Params)
	cfg, err := p.Load(fs)
	require.NoError(t, err)

	_, err = resolveAigwEndpoint(t.Context(), cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no cluster selected")
}

func TestResolveAigwEndpoint_NoAiGatewayV2(t *testing.T) {
	// Cluster exists but has no AiGateway attached.
	cluster := &controlplanev1.Cluster{Id: "clu-1"}
	ts := httptest.NewServer(clusterHandler(t, cluster))
	defer ts.Close()

	cfg := loadCloudProfile(t, ts.URL, "clu-1")
	_, err := resolveAigwEndpoint(t.Context(), cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "does not have an AI Gateway v2 endpoint")
}

func TestResolveAigwEndpoint_Happy(t *testing.T) {
	cluster := &controlplanev1.Cluster{
		Id: "clu-1",
		AiGateway: &controlplanev1.Cluster_AIGateway{
			V2Url: "https://aigw.example.com",
		},
	}
	ts := httptest.NewServer(clusterHandler(t, cluster))
	defer ts.Close()

	cfg := loadCloudProfile(t, ts.URL, "clu-1")
	endpoint, err := resolveAigwEndpoint(t.Context(), cfg)
	require.NoError(t, err)
	require.Equal(t, "https://aigw.example.com", endpoint)
}

// loadCloudProfile builds a *config.Config with a cloud profile selecting the
// given cluster ID, pointing at publicAPIURL.
func loadCloudProfile(t *testing.T, publicAPIURL, clusterID string) *config.Config {
	t.Helper()
	t.Setenv("RPK_PUBLIC_API_URL", publicAPIURL)
	t.Setenv("RPK_CLOUD_TOKEN", "test-token")

	fs := afero.NewMemMapFs()
	// DefaultRpkYamlPath resolves via os.UserConfigDir, which is
	// platform-dependent (~/.config/rpk on Linux, ~/Library/Application
	// Support/rpk on darwin). Writing to that exact path means the config
	// loader will actually find our yaml.
	path, err := config.DefaultRpkYamlPath()
	require.NoError(t, err)

	yaml := fmt.Sprintf(`version: 6
current_profile: dev
profiles:
  - name: dev
    from_cloud: true
    cloud_cluster:
      cluster_id: %q
`, clusterID)
	require.NoError(t, afero.WriteFile(fs, path, []byte(yaml), 0o600))

	p := new(config.Params)
	cfg, err := p.Load(fs)
	require.NoError(t, err)
	return cfg
}
