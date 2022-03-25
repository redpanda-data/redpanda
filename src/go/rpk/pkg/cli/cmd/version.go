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
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cmd/version"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

func NewVersionCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "version",
		Short: "Check the current version.",
		Long:  "",
		Run: func(_ *cobra.Command, _ []string) {
			log.SetFormatter(cli.NewNoopFormatter())
			log.Infof("%s\n", version.Pretty())
		},
	}
	return command
}
