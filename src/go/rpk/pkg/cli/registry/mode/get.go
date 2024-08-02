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
	"github.com/twmb/franz-go/pkg/sr"
)

func getCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var global bool
	cmd := &cobra.Command{
		Use:   "get [SUBJECT...]",
		Short: "Get schema registry mode",
		Long: `Get schema registry mode

Running this command with no subject returns the global mode, alternatively
you can use the --global flag to get the global mode at the same time as
per-subject modes.
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

			if len(subjects) > 0 && global {
				subjects = append(subjects, sr.GlobalSubject)
			}
			ctx := sr.WithParams(cmd.Context(), sr.DefaultToGlobal)
			modeResult := cl.Mode(ctx, subjects...)
			exit1, err := printModeResult(f, modeResult)
			out.MaybeDieErr(err)
			if exit1 {
				os.Exit(1)
			}
		},
	}
	cmd.Flags().BoolVar(&global, "global", false, "Return the global mode in addition to subject modes")

	return cmd
}
