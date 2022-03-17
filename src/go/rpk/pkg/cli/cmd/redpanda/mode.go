// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

//go:build linux
// +build linux

package redpanda

import (
	"fmt"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
)

func NewModeCommand(mgr config.Manager) *cobra.Command {
	var configFile string
	command := &cobra.Command{
		Use:   "mode <mode>",
		Short: "Enable a default configuration mode.",
		Long:  "",
		Args: func(_ *cobra.Command, args []string) error {
			if len(args) < 1 {
				return fmt.Errorf("requires a mode [%s]", strings.Join(config.AvailableModes(), ", "))
			}
			return nil
		},
		RunE: func(_ *cobra.Command, args []string) error {
			// Safe to access args[0] because it was validated in Args
			return executeMode(mgr, configFile, args[0])
		},
	}
	command.Flags().StringVar(
		&configFile,
		"config",
		"",
		"Redpanda config file, if not set the file will be searched for"+
			" in the default locations.",
	)
	return command
}

func executeMode(mgr config.Manager, configFile string, mode string) error {
	conf, err := mgr.FindOrGenerate(configFile)
	if err != nil {
		return err
	}
	conf, err = config.SetMode(mode, conf)
	if err != nil {
		return err
	}
	log.Infof("Writing '%s' mode defaults to '%s'", mode, conf.ConfigFile)
	return mgr.Write(conf)
}
