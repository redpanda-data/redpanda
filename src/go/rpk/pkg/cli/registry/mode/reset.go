// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package mode

import (
	"os"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/schemaregistry"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func resetCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	return &cobra.Command{
		Use:   "reset [SUBJECT...]",
		Short: "Reset schema registry mode",
		Long: `Reset schema registry mode
	
This command deletes any subject modes and reverts to the global default. 

The command also prints the subject mode before reverting to the global default.
`,
		Run: func(cmd *cobra.Command, subjects []string) {
			f := p.Formatter
			if h, ok := f.Help([]modeResult{}); ok {
				out.Exit(h)
			}
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)

			cl, err := schemaregistry.NewClient(fs, p)
			out.MaybeDie(err, "unable to initialize schema registry client: %v", err)

			resetModeResult := cl.ResetMode(cmd.Context(), subjects...)
			exit1, err := printModeResult(f, resetModeResult)
			out.MaybeDieErr(err)
			if exit1 {
				os.Exit(1)
			}
		},
	}
}
