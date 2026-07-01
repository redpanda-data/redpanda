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
	"slices"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cobraext"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

// parseFlags splits args into plugin args + rpk-global-flags consumed by rpk,
// and parses the rpk-global-flags so the logger and config loader pick them up.
//
// rpk does not inject any cloud context (token or endpoint) into the plugin:
// the rpk ai plugin owns its own login (`rpk ai auth login`) and environment
// selection (`rpk ai env use`), so it runs without a selected rpk cloud
// cluster. Everything except rpk's own globals is forwarded untouched.
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
