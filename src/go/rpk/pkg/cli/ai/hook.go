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

// resolveAndInjectEnv loads the cloud config, refreshes the token, resolves
// the active cluster's AI Gateway v2 endpoint, and exports both to the
// caller's environment so the child rpai process picks them up on exec.
//
// Env var writes are skipped when the variable is already present (explicit
// RPAI_TOKEN / RPAI_ENDPOINT from the user win). If the invocation carries
// --rpai-endpoint on the plugin command line, endpoint resolution is skipped
// — rpai itself will consume the flag.
//
// The two short-circuit decisions (help/version, top-level-without-subcommand)
// live in the call sites since they differ between top-level dispatch (where
// args carries the subcommand path) and leaf dispatch (where args is the
// leaf's positional-and-flag args only).
func resolveAndInjectEnv(ctx context.Context, fs afero.Fs, p *config.Params, pluginArgs []string) error {
	cfg, err := p.Load(fs)
	if err != nil {
		return fmt.Errorf("unable to load rpk config: %w", err)
	}

	if os.Getenv(envRpaiToken) == "" {
		token, err := getTokenOrLogin(ctx, fs, cfg)
		if err != nil {
			return err
		}
		if err := os.Setenv(envRpaiToken, token); err != nil {
			return fmt.Errorf("unable to set %s: %w", envRpaiToken, err)
		}
	}

	if os.Getenv(envRpaiEndpoint) == "" && !hasRpaiEndpointFlag(pluginArgs) {
		endpoint, err := resolveAigwEndpoint(ctx, cfg)
		if err != nil {
			return err
		}
		if err := os.Setenv(envRpaiEndpoint, endpoint); err != nil {
			return fmt.Errorf("unable to set %s: %w", envRpaiEndpoint, err)
		}
	}

	return nil
}

// skipCloudForHelp reports whether a --help / -h / --version flag is present,
// in which case we must not reach out to the cloud API or trigger OAuth. The
// rpai child process renders its own help/version output locally.
func skipCloudForHelp(args []string) bool {
	for _, a := range args {
		if a == "--help" || a == "-h" || a == "--version" {
			return true
		}
	}
	return false
}

// topLevelHasSubcommand reports whether the args passed to the top-level
// `rpk ai` dispatcher contain a positional (non-flag) token. This
// distinguishes `rpk ai llm list` (needs cloud context) from `rpk ai`
// (bare, just renders help).
//
// This check is only meaningful for the top-level Run in NewCommand. When
// cobra dispatches to a managed-plugin LEAF (e.g. `rpk ai llm list` routes
// to the `list` leaf registered via plugin_cmds.go), args == nil — cobra
// has already consumed the path tokens for routing, so "no positional" does
// not imply "no subcommand" at that call site.
func topLevelHasSubcommand(args []string) bool {
	for _, a := range args {
		if !strings.HasPrefix(a, "-") {
			return true
		}
	}
	return false
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
