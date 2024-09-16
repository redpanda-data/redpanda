// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package generate

import (
	"fmt"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func NewCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "generate [template]",
		Short: "Generate a configuration template for related services",
	}
	cmd.AddCommand(
		newAppCmd(fs, p),
		newGrafanaDashboardCmd(p),
		newPrometheusConfigCmd(fs, p),
		newShellCompletionCommand(),
	)
	return cmd
}

// fileSpec are the spec of a vendored file that can also be downloaded from GH.
type fileSpec struct {
	Location    string // The file location (GH or local path).
	Description string // The file Description. Used for help text.
	Hash        string // The SHA256 hash of the file.
}

func validFiles(fileMap map[string]*fileSpec) func(*cobra.Command, []string, string) ([]string, cobra.ShellCompDirective) {
	return func(_ *cobra.Command, _ []string, _ string) ([]string, cobra.ShellCompDirective) {
		opts := make([]string, 0, len(fileMap))
		for k, v := range fileMap {
			// Cobra provides support for completion descriptions:
			//     flagName -- Description of flag
			// to do so we must add a \t between the flag and the Description.
			s := fmt.Sprintf("%v\t%v", k, v.Description)
			opts = append(opts, s)
		}
		return opts, cobra.ShellCompDirectiveDefault
	}
}
