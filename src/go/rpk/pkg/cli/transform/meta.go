/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package transform

import (
	"fmt"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newPauseCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "pause [NAME]",
		Short: "Pause a data transform",
		Long: `Pause a data transform.

This command suspends execution of the specified transform without removing
it from the system. In this way, a transform may resume at a later time, with
each new processor picking up processing from the last committed offset on the
corresponding input partition.

Subsequent 'rpk transform list' operations will show transform processors as
"inactive".

To resume a paused transform, use 'rpk transform resume'.
`,
		Args: cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			config.CheckExitServerlessAdmin(p)
			api, err := adminapi.NewClient(cmd.Context(), fs, p)
			out.MaybeDie(err, "unable to initialize admin api client: %v", err)
			functionName := args[0]
			err = api.PauseTransform(cmd.Context(), functionName)
			out.MaybeDie(err, "unable to pause transform %q: %v", functionName, err)
			fmt.Println("Transform paused!")
		},
	}
	return cmd
}

func newResumeCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "resume [NAME]",
		Short: "Resume a data transform",
		Long: `Resume a data transform.

This command resumes execution of the specified data transform, if it was
previously paused. Transform processors are restarted and resume processing
from the last committed offset on the corresponding input partition.

Subsequent 'rpk transform list' operations will show transform processors as
"running".
`,
		Args: cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			config.CheckExitServerlessAdmin(p)
			api, err := adminapi.NewClient(cmd.Context(), fs, p)
			out.MaybeDie(err, "unable to initialize admin api client: %v", err)
			functionName := args[0]
			err = api.ResumeTransform(cmd.Context(), functionName)
			out.MaybeDie(err, "unable to resume transform %q: %v", functionName, err)
			fmt.Println("Transform resumed!")
		},
	}
	return cmd
}
