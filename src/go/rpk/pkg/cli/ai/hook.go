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
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cobraext"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/oauth"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/oauth/providers/auth0"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/publicapi"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

const (
	envRpaiToken    = "RPAI_TOKEN"
	envRpaiEndpoint = "RPAI_ENDPOINT"

	flagRpaiEndpoint = "rpai-endpoint"
)

// applyHook prepares a `rpk ai ...` invocation. It:
//   - strips rpk-global flags from args,
//   - loads the cloud token (refreshing via OAuth if needed),
//   - resolves the active cluster's AI Gateway v2 endpoint,
//   - sets RPAI_TOKEN and RPAI_ENDPOINT for the child plugin process,
//
// and returns the remaining args to pass to the plugin.
//
// The hook skips env var writes when the variable is already set in the
// caller's environment — an explicit RPAI_TOKEN / RPAI_ENDPOINT wins. If the
// user passed --rpai-endpoint on the command line, endpoint resolution is
// skipped as well (rpai itself will consume the flag).
//
// Token + endpoint work is skipped entirely when the invocation is a bare
// --help / --version / no-subcommand call: those need neither auth nor a
// live cluster.
func applyHook(fs afero.Fs, p *config.Params, cmd *cobra.Command, args []string) ([]string, error) {
	pluginArgs, err := parseFlags(p, cmd, args)
	if err != nil {
		return nil, err
	}
	if !needsCloudContext(pluginArgs) {
		return pluginArgs, nil
	}

	cfg, err := p.Load(fs)
	if err != nil {
		return nil, fmt.Errorf("unable to load rpk config: %w", err)
	}

	ctx := cmd.Context()
	if ctx == nil {
		ctx = context.Background()
	}

	// RPAI_TOKEN: inject unless the caller already provided one.
	if os.Getenv(envRpaiToken) == "" {
		token, err := getTokenOrLogin(ctx, fs, cfg)
		if err != nil {
			return nil, err
		}
		if err := os.Setenv(envRpaiToken, token); err != nil {
			return nil, fmt.Errorf("unable to set %s: %w", envRpaiToken, err)
		}
	}

	// RPAI_ENDPOINT: inject unless the caller already provided one via env or
	// via --rpai-endpoint on the plugin command line.
	if os.Getenv(envRpaiEndpoint) == "" && !hasRpaiEndpointFlag(pluginArgs) {
		endpoint, err := resolveAigwEndpoint(ctx, cfg)
		if err != nil {
			return nil, err
		}
		if err := os.Setenv(envRpaiEndpoint, endpoint); err != nil {
			return nil, fmt.Errorf("unable to set %s: %w", envRpaiEndpoint, err)
		}
	}

	return pluginArgs, nil
}

// needsCloudContext reports whether the plugin args imply that the user is
// actually invoking a cluster-touching subcommand. Pure flag-only invocations
// like `rpk ai --help` or `rpk ai --version` do not need a cloud token or an
// aigw endpoint and must not trigger the OAuth browser flow. The same holds
// for `rpk ai <subcommand> --help`: the plugin renders its own help without
// reaching the network.
func needsCloudContext(pluginArgs []string) bool {
	var hasSubcommand bool
	for _, a := range pluginArgs {
		switch {
		case a == "--help", a == "-h", a == "--version":
			return false
		case strings.HasPrefix(a, "-"):
			continue
		default:
			hasSubcommand = true
		}
	}
	return hasSubcommand
}

// parseFlags splits args into plugin args + rpk-global-flags consumed by rpk,
// and parses the rpk-global-flags so the logger and config loader pick them up.
func parseFlags(p *config.Params, cmd *cobra.Command, args []string) ([]string, error) {
	f := cmd.Flags()

	keepForPlugin, stripForRpk := cobraext.StripFlagset(args, f)
	if err := f.Parse(stripForRpk); err != nil {
		return nil, err
	}
	// Rebuild the logger since we manually parsed the flags.
	zap.ReplaceGlobals(p.BuildLogger())

	// StripFlagset removes --help / -h because they're attached to rpk too;
	// forward them to the plugin so the plugin can render its own help.
	if cobraext.LongFlagValue(args, f, "help", "h") == "true" && !slices.Contains(keepForPlugin, "--help") {
		keepForPlugin = append(keepForPlugin, "--help")
	}
	return keepForPlugin, nil
}

// hasRpaiEndpointFlag reports whether the plugin args carry an explicit
// --rpai-endpoint flag (in any supported form: --rpai-endpoint=..., or the
// flag followed by its value).
func hasRpaiEndpointFlag(args []string) bool {
	prefix := "--" + flagRpaiEndpoint
	for _, a := range args {
		if a == prefix || strings.HasPrefix(a, prefix+"=") {
			return true
		}
	}
	return false
}

// getTokenOrLogin returns a fresh cloud bearer token, refreshing or prompting
// for login as needed. It mirrors the byoc plugin's behavior for consistency.
func getTokenOrLogin(ctx context.Context, fs afero.Fs, cfg *config.Config) (string, error) {
	overrides := cfg.DevOverrides()
	if overrides.CloudToken != "" {
		return overrides.CloudToken, nil
	}

	priorProfile := cfg.ActualProfile()
	_, authVir, clearedProfile, _, err := oauth.LoadFlow(ctx, fs, cfg, auth0.NewClient(cfg.DevOverrides()), false, false)
	if err != nil {
		return "", fmt.Errorf("unable to refresh the cloud token: %w. Run 'rpk cloud login' and try again", err)
	}
	oauth.MaybePrintSwapMessage(clearedProfile, priorProfile, authVir)
	return authVir.AuthToken, nil
}

// resolveAigwEndpoint looks up the active rpk cloud profile's cluster, queries
// the public API for its AI Gateway v2 URL, and returns it.
func resolveAigwEndpoint(ctx context.Context, cfg *config.Config) (string, error) {
	prof := cfg.VirtualProfile()
	if prof == nil || !prof.FromCloud || prof.CloudCluster.ClusterID == "" {
		return "", errors.New("no cluster selected for this rpk profile; run 'rpk cloud cluster use <id>' or pass --rpai-endpoint")
	}
	clusterID := prof.CloudCluster.ClusterID

	token := os.Getenv(envRpaiToken)
	cl := publicapi.NewCloudClientSet(cfg.DevOverrides().PublicAPIURL, token)
	cluster, err := cl.ClusterForID(ctx, clusterID)
	if err != nil {
		return "", fmt.Errorf("unable to resolve aigw endpoint for cluster %s: %w", clusterID, err)
	}
	endpoint := cluster.GetAiGateway().GetV2Url()
	if endpoint == "" {
		return "", fmt.Errorf("cluster %s does not have an AI Gateway v2 endpoint; pick a cluster that does, or pass --rpai-endpoint", clusterID)
	}
	return endpoint, nil
}
