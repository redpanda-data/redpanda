// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package cmd

import (
	"vectorized/pkg/cli/cmd/generate"
	"vectorized/pkg/config"

	"github.com/spf13/cobra"
)

func NewGenerateCommand(mgr config.Manager) *cobra.Command {
	command := &cobra.Command{
		Use:   "generate [template]",
		Short: "Generate a configuration template for related services.",
	}
	command.AddCommand(generate.NewGrafanaDashboardCmd())
	command.AddCommand(generate.NewPrometheusConfigCmd(mgr))
	return command
}
