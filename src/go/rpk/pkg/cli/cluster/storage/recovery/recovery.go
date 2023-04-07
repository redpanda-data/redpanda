// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package recovery

import (
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func NewCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "recovery",
		Short: "Interact with the topic recovery process",
		Long: `Interact with the topic recovery process.
		
This command is used to restore topics from the archival bucket, which can be 
useful for disaster recovery or if a topic was accidentally deleted.

To begin the recovery process, use the "recovery start" command. Note that this 
process can take a while to complete, so the command will exit after starting 
it. If you want the command to wait for the process to finish, use the "--wait"
or "-w" flag.

You can check the status of the recovery process with the "recovery status" 
command after it has been started.
`,
	}
	p.InstallAdminFlags(cmd)
	cmd.AddCommand(
		newStartCommand(fs, p),
		newStatusCommand(fs, p),
	)

	return cmd
}
