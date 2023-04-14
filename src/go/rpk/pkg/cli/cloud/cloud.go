// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package cloud

import (
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cloud/byoc"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func NewCommand(fs afero.Fs, execFn func(string, []string) error) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cloud",
		Short: "Interact with Redpanda cloud",
	}

	cmd.AddCommand(
		byoc.NewCommand(fs, execFn),
		newLoginCommand(fs),
		newLogoutCommand(fs),
	)

	return cmd
}
