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
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
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
