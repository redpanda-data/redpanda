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
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

func TestParseFlags_StripsRpkGlobals(t *testing.T) {
	root := &cobra.Command{Use: "rpk"}
	pf := root.PersistentFlags()
	pf.String("config", "", "")
	pf.String("profile", "", "")
	pf.StringArrayP("config-opt", "X", nil, "")
	pf.BoolP("verbose", "v", false, "")

	var got []string
	var gotErr error
	k8sCmd := &cobra.Command{
		Use:                "k8s",
		DisableFlagParsing: true,
		Args:               cobra.MinimumNArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			got, gotErr = parseFlags(new(config.Params), cmd, args)
		},
	}
	k8sCmd.Flags().BoolP("help", "h", false, "")
	root.AddCommand(k8sCmd)

	root.SetArgs([]string{"k8s", "--config", "/foo", "-X", "k=v", "multicluster", "status", "--kubeconfig=/kc"})
	require.NoError(t, root.Execute())
	require.NoError(t, gotErr)
	require.Equal(t, []string{"multicluster", "status", "--kubeconfig=/kc"}, got)
}
