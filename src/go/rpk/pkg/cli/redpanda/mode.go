// Copyright 2021 Redpanda Data, Inc.
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

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func NewModeCommand(fs afero.Fs) *cobra.Command {
	var configFile string
	command := &cobra.Command{
		Use:   "mode <mode>",
		Short: "Enable a default configuration mode",
		Long:  "",
		Args: func(_ *cobra.Command, args []string) error {
			if len(args) < 1 {
				return fmt.Errorf("requires a mode [%s]", strings.Join(config.AvailableModes(), ", "))
			}
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			// Safe to access args[0] because it was validated in Args
			err := executeMode(fs, cmd, args[0])
			out.MaybeDieErr(err)
		},
	}
	command.Flags().StringVar(
		&configFile,
		"config",
		"",
		"Redpanda config file, if not set the file will be searched for"+
			" in $PWD or /etc/redpanda/redpanda.yaml.",
	)
	return command
}

func executeMode(fs afero.Fs, cmd *cobra.Command, mode string) error {
	p := config.ParamsFromCommand(cmd)
	cfg, err := p.Load(fs)
	if err != nil {
		return fmt.Errorf("unable to load config: %v", err)
	}
	cfg = cfg.FileOrDefaults() // we modify fields in the raw file without writing env / flag overrides
	cfg, err = config.SetMode(mode, cfg)
	if err != nil {
		return err
	}

	fmt.Printf("Writing %q mode defaults to %q\n", mode, cfg.FileLocation())
	err = cfg.Write(fs)
	if err != nil {
		return err
	}
	return nil
}
