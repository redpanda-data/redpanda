// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package acl

import (
	newacl "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/security/acl"
	newuser "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/security/user"
	"go.uber.org/zap"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

// NewCommand is kept here for backcompat reasons, it is soft deprecated and now
// lives in the /security package.
func NewCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := newacl.NewCommand(fs, p)
	cmd.Hidden = true
	cmd.PreRun = func(_ *cobra.Command, _ []string) {
		zap.L().Sugar().Warn("this command is deprecated; use 'rpk security acl' instead")
	}
	for _, children := range cmd.Commands() {
		children.PreRun = func(cmd *cobra.Command, _ []string) {
			zap.L().Sugar().Warnf("this command is deprecated; use 'rpk security acl %v' instead", cmd.Name())
		}
	}

	userCmd := newuser.NewCommand(fs, p)
	userCmd.Hidden = true
	userCmd.PreRun = func(_ *cobra.Command, _ []string) {
		zap.L().Sugar().Warnf("this command is deprecated; use 'rpk security user' instead")
	}
	for _, children := range userCmd.Commands() {
		children.PreRun = func(cmd *cobra.Command, _ []string) {
			zap.L().Sugar().Warnf("this command is deprecated; use 'rpk security user %v' instead", cmd.Name())
		}
	}

	cmd.AddCommand(userCmd)
	return cmd
}
