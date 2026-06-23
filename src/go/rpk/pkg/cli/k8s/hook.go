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
	"slices"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cobraext"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

// parseFlags splits args into plugin args + rpk-global flags consumed by rpk,
// parses the rpk-global flags so the logger and config loader pick them up,
// and forwards --help to the plugin so it can render its own help. The k8s
// plugin reads kubeconfig itself (kubectl-style), so unlike the ai plugin we
// inject no cloud token/endpoint.
func parseFlags(p *config.Params, cmd *cobra.Command, args []string) ([]string, error) {
	f := cmd.Flags()
	keepForPlugin, stripForRpk := cobraext.StripFlagset(args, f)
	if err := f.Parse(stripForRpk); err != nil {
		return nil, err
	}
	zap.ReplaceGlobals(p.BuildLogger())

	if cobraext.LongFlagValue(args, f, "help", "h") == "true" && !slices.Contains(keepForPlugin, "--help") {
		keepForPlugin = append(keepForPlugin, "--help")
	}
	return keepForPlugin, nil
}
