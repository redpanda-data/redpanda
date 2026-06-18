// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

//go:build linux

package redpanda

import (
	"fmt"
	"strings"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func NewModeCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "mode [MODE]",
		Short: "Enable or display the configuration mode",
		Long:  modeHelpText,
		Args:  cobra.MaximumNArgs(1),
		ValidArgsFunction: func(_ *cobra.Command, _ []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			// We complete "dev" and "prod", but if the user is typing
			// the full word, we switch to completing that.
			var complete []string
			for _, s := range []string{config.ModeDev, config.ModeProd, config.ModeRecovery} {
				if strings.HasPrefix(s, toComplete) {
					complete = append(complete, s)
				}
			}
			if len(complete) > 0 {
				return complete, cobra.ShellCompDirectiveDefault
			}
			for _, s := range []string{"development", "production"} {
				if strings.HasPrefix(s, toComplete) {
					complete = append(complete, s)
				}
			}
			return complete, cobra.ShellCompDirectiveDefault
		},
		Run: func(_ *cobra.Command, args []string) {
			// With no argument we report the currently active mode
			// instead of changing it.
			if len(args) == 0 {
				mode, err := currentMode(fs, p)
				out.MaybeDieErr(err)
				fmt.Printf("Current mode: %q.\n", mode)
				return
			}
			err := executeMode(fs, p, args[0])
			out.MaybeDieErr(err)
			fmt.Printf("Successfully set mode to %q.\n", args[0])
		},
	}
	return cmd
}

func executeMode(fs afero.Fs, p *config.Params, mode string) error {
	cfg, err := p.Load(fs)
	if err != nil {
		return fmt.Errorf("rpk unable to load config: %v", err)
	}
	return cfg.SetMode(fs, mode)
}

func currentMode(fs afero.Fs, p *config.Params) (string, error) {
	cfg, err := p.Load(fs)
	if err != nil {
		return "", fmt.Errorf("rpk unable to load config: %v", err)
	}
	return cfg.ActualRedpandaYamlOrDefaults().Mode(), nil
}

const modeHelpText = `Enable or display the configuration mode

This command allows you to set one of the following modes: Development,
Production, or Recovery.

Running the command without a MODE argument prints the mode that is currently
set in your redpanda.yaml.

PRODUCTION

  - Enforces optimal runtime checks.
  - Enables all rpk tuners, for you to run 'rpk redpanda tune'.

DEVELOPMENT

  - Disables optimal checks and bypasses fsync, which results in unrealistically
    fast clusters and may result in data loss.
  - Disables all rpk tuners. The tuners are intended to run only for production 
    mode.

RECOVERY

Sets the broker configuration property 'redpanda.recovery_mode_enabled=true' in 
your redpanda.yaml. This provides a stable environment for troubleshooting and 
restoring a failed cluster. Restart required.
`
