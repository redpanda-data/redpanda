// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package shadow

import (
	"fmt"

	"connectrpc.com/connect"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v2"
)

func newCreateCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var (
		noConfirm   bool
		cfgLocation string
	)
	cmd := &cobra.Command{
		Use:   "create",
		Args:  cobra.NoArgs,
		Short: "Create a Redpanda Shadow Link",
		Long: `Create a Shadow Link

Create a Shadow Link using a configuration file. The configuration file defines
the connection details and settings for the Shadow Link. For details on the
configuration file format, see:
  rpk shadow config --help

The command prompts for confirmation by default. Use the --no-confirm flag to
skip the confirmation prompt.
`,
		Run: func(cmd *cobra.Command, _ []string) {
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "unable to load rpk config: %v", err)
			config.CheckExitCloudAdmin(p) // TODO: remove, for now is there because we don't support it in cloud yet.

			cl, err := adminapi.NewClient(cmd.Context(), fs, p)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			slCfg, err := parseShadowLinkConfig(fs, cfgLocation)
			out.MaybeDie(err, "unable to parse Shadow Link configuration file: %v", err)

			// This is a naive client side validation, the server will do a full
			// validation and return proper errors if something is wrong.
			err = validateParsedShadowLinkConfig(slCfg)
			out.MaybeDie(err, "invalid Shadow Link configuration: %v", err)

			printShadowLinkCfgOverview(slCfg)
			if !noConfirm {
				ok, err := out.Confirm("Do you want to create this shadow link?")
				out.MaybeDie(err, "unable to confirm Shadow Link creation: %v", err)
				if !ok {
					out.Exit("Shadow Link creation cancelled")
				}
			}
			fmt.Println()
			createReq := shadowLinkConfigToCreateReq(slCfg)

			link, err := cl.ShadowLinkService().CreateShadowLink(cmd.Context(), connect.NewRequest(createReq))
			out.MaybeDie(err, "unable to create shadow link: %v", err)

			fmt.Printf("Successfully created shadow link %q with ID %q. To query the status, run:\n  'rpk shadow status %[1]v'\n", link.Msg.GetShadowLink().GetName(), link.Msg.GetShadowLink().GetUid())
		},
	}

	cmd.Flags().BoolVar(&noConfirm, "no-confirm", false, "Disable confirmation prompt")
	cmd.Flags().StringVar(&cfgLocation, "config-file", "", "Path to configuration file to use for the shadow link; use --help for details")
	cmd.MarkFlagRequired("config-file")
	return cmd
}

func parseShadowLinkConfig(fs afero.Fs, path string) (*ShadowLinkConfig, error) {
	file, err := afero.ReadFile(fs, path)
	if err != nil {
		return nil, fmt.Errorf("unable to read Shadow Link config file %q: %w", path, err)
	}

	var slCfg ShadowLinkConfig
	err = yaml.Unmarshal(file, &slCfg)
	if err != nil {
		return nil, fmt.Errorf("unable to parse Shadow Link config file %q: %w", path, err)
	}
	return &slCfg, nil
}

func printShadowLinkCfgOverview(slCfg *ShadowLinkConfig) {
	tw := out.NewTable()
	defer tw.Flush()
	tw.Print("Link Name:", slCfg.Name)
	tw.Print("Bootstrap Servers:")
	for _, srv := range slCfg.ClientOptions.BootstrapServers {
		tw.Print("", fmt.Sprintf("- %s", srv))
	}
}

func validateParsedShadowLinkConfig(slCfg *ShadowLinkConfig) error {
	if slCfg == nil {
		return fmt.Errorf("provided configuration file generated an empty configuration")
	}
	if slCfg.Name == "" {
		return fmt.Errorf("the Shadow Link name is required")
	}
	if len(slCfg.ClientOptions.BootstrapServers) == 0 {
		return fmt.Errorf("at least one bootstrap server is required")
	}
	return nil
}
