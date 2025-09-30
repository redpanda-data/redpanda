// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package profile

import (
	"fmt"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

const (
	defaultFileAnnotation     = "--- Loaded from file: rpk.yaml"
	effectiveConfigAnnotation = "--- Effective configuration"
)

// renderProfile loads a profile based on its name, redacts sensitive fields, and
// outputs the marshalled yaml.
func renderProfile(rpkYaml *config.RpkYaml, name string) (string, error) {
	profile := rpkYaml.Profile(name)
	if profile == nil {
		return "", fmt.Errorf("profile %s does not exist", name)
	}

	// remove the license check and redacts the SASL password, if present
	profile.LicenseCheck = nil
	if profile.KafkaAPI.SASL != nil && profile.KafkaAPI.SASL.Password != "" {
		profile.KafkaAPI.SASL.Password = "[REDACTED]"
	}

	marshalled, err := yaml.Marshal(profile)
	if err != nil {
		return "", fmt.Errorf("unable to encode profile: %w", err)
	}

	return string(marshalled), nil
}

func renderCommand(fs afero.Fs, p *config.Params, args []string) (string, error) {
	cfg, err := p.Load(fs)
	if err != nil {
		return "", fmt.Errorf("rpk unable to load config: %v", err)
	}

	actual, ok := cfg.ActualRpkYaml()
	if !ok {
		return "", fmt.Errorf("rpk.yaml file does not exist")
	}

	// If they haven't provided a profile, use the default
	profileName := actual.CurrentProfile
	if len(args) > 0 {
		profileName = args[0]
	}

	defaultMarshalled, err := renderProfile(actual, profileName)
	if err != nil {
		return "", err
	}

	// If the -v/--verbose flag is set, annotate the file config
	// and also output the computed config, like:
	//
	// --- Loaded from file: rpk.yaml
	// ...
	//
	// --- Effective configuration
	// ...

	if p.DebugLogs {
		effective := cfg.VirtualRpkYaml()
		effectiveMarshalled, err := renderProfile(effective, profileName)
		if err != nil {
			return "", err
		}

		return fmt.Sprintf(`%s
%s

%s
%s
`, defaultFileAnnotation, defaultMarshalled, effectiveConfigAnnotation, effectiveMarshalled), nil
	} else {
		return defaultMarshalled, nil
	}
}

func newPrintCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	return &cobra.Command{
		Use:   "print [NAME]",
		Short: "Print rpk profile configuration",
		Long: `Print rpk profile configuration.

If no name is specified, this command prints the current profile as it exists
in the rpk.yaml file.

To print both the profile as it exists in the rpk.yaml file and the current
profile as it is loaded in rpk with internal defaults, user-specified flags,
and environment variables applied, use the -v/--verbose flag.
`,
		Args:              cobra.MaximumNArgs(1),
		ValidArgsFunction: ValidProfiles(fs, p),
		Run: func(_ *cobra.Command, args []string) {
			output, err := renderCommand(fs, p, args)
			out.MaybeDieErr(err)
			fmt.Println(output)
		},
	}
}
