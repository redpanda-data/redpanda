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
	"fmt"
	"os"
	"slices"
	"strings"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/schemaregistry"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/sr"
)

var supportedModes = []string{"READONLY", "READWRITE"}

func setCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var modeFlag string
	var global bool
	cmd := &cobra.Command{
		Use:   "set [SUBJECT...]",
		Short: "Set schema registry mode",
		Long: `Set schema registry mode

Running this command with no subject sets the global schema registry mode, 
alternatively you can use the --global flag to set the global schema registry 
mode at the same time as per-subject mode.

Acceptable mode values: 
  - READONLY
  - READWRITE
`,
		Example: `
Set the global schema registry mode to READONLY
  rpk registry mode set --mode READONLY

Set the schema registry mode to READWRITE in subjects foo and bar
  rpk registry mode set foo bar --mode READWRITE
`,
		Run: func(cmd *cobra.Command, subjects []string) {
			f := p.Formatter
			if h, ok := f.Help([]modeResult{}); ok {
				out.Exit(h)
			}
			isValidMode := slices.ContainsFunc(supportedModes, func(s string) bool {
				return strings.ToLower(s) == strnorm(modeFlag)
			})
			if !isValidMode {
				out.Die("invalid mode %q; supported modes: %v", modeFlag, strings.Join(supportedModes, ", "))
			}
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)

			cl, err := schemaregistry.NewClient(fs, p)
			out.MaybeDie(err, "unable to initialize schema registry client: %v", err)

			mode := new(sr.Mode)
			err = mode.UnmarshalText([]byte(modeFlag))
			out.MaybeDie(err, "unable to parse mode flag: %v", err)

			if len(subjects) > 0 && global {
				subjects = append(subjects, sr.GlobalSubject)
			}
			setModeResult := cl.SetMode(cmd.Context(), *mode, subjects...)

			exit1, err := printModeResult(f, setModeResult)
			out.MaybeDieErr(err)
			if exit1 {
				os.Exit(1)
			}
		},
	}

	cmd.Flags().BoolVar(&global, "global", false, "Set the global schema registry mode in addition to subject modes")
	cmd.Flags().StringVar(&modeFlag, "mode", "", fmt.Sprintf("Schema registry mode to set. Supported values: %v (case insensitive)", strings.Join(supportedModes, ", ")))
	cmd.MarkFlagRequired("mode")
	cmd.RegisterFlagCompletionFunc("mode", func(_ *cobra.Command, _ []string, _ string) ([]string, cobra.ShellCompDirective) {
		return supportedModes, cobra.ShellCompDirectiveDefault
	})
	return cmd
}

func strnorm(s string) string {
	s = strings.ReplaceAll(s, ".", "")
	s = strings.ReplaceAll(s, "_", "")
	s = strings.ReplaceAll(s, "-", "")
	s = strings.TrimSpace(s)
	s = strings.ToLower(s)
	return s
}
