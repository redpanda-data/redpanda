// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package registry

import (
	"context"
	"fmt"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/schemaregistry"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/sr"
)

func compatibilityLevelCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "compatibility-level",
		Args:  cobra.ExactArgs(0),
		Short: "Manage global or per-subject compatibility levels",
	}
	cmd.AddCommand(
		compatGetCommand(fs, p),
		compatSetCommand(fs, p),
	)
	p.InstallFormatFlag(cmd)
	return cmd
}

type compatibilityLevelResponse struct {
	Subject string `json:"subject" yaml:"subject"`
	Level   string `json:"level,omitempty" yaml:"level,omitempty"`
	Err     string `json:"error,omitempty" yaml:"error,omitempty"`
}

func compatGetCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var global bool
	cmd := &cobra.Command{
		Use:   "get [SUBJECT...]",
		Short: "Get the global or per-subject compatibility levels",
		Long: `Get the global or per-subject compatibility levels.

Running this command with no subject returns the global level, alternatively
you can use the --global flag to get the global level at the same time as
per-subject levels.
`,
		Run: func(_ *cobra.Command, subjects []string) {
			f := p.Formatter
			if h, ok := f.Help([]compatibilityLevelResponse{}); ok {
				out.Exit(h)
			}
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)

			cl, err := schemaregistry.NewClient(fs, p)
			out.MaybeDie(err, "unable to initialize schema registry client: %v", err)

			if len(subjects) > 0 && global {
				subjects = append(subjects, sr.GlobalSubject)
			}
			results := cl.Compatibility(context.Background(), subjects...)

			err = printCompatibilityResult(results, f)
			out.MaybeDieErr(err)
		},
	}

	cmd.Flags().BoolVar(&global, "global", false, "Return the global level in addition to subject levels")
	return cmd
}

func compatSetCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var global bool
	var level string
	cmd := &cobra.Command{
		Use:   "set [SUBJECT...]",
		Short: "Set the global or per-subject compatibility levels",
		Long:  compatHelpText,
		Run: func(_ *cobra.Command, subjects []string) {
			f := p.Formatter
			if h, ok := f.Help([]compatibilityLevelResponse{}); ok {
				out.Exit(h)
			}
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)

			cl, err := schemaregistry.NewClient(fs, p)
			out.MaybeDie(err, "unable to initialize schema registry client: %v", err)
			if len(subjects) > 0 && global {
				subjects = append(subjects, sr.GlobalSubject)
			}
			var l sr.CompatibilityLevel
			err = l.UnmarshalText([]byte(level))
			out.MaybeDieErr(err)

			results := cl.SetCompatibility(context.Background(), sr.SetCompatibility{Level: l}, subjects...)
			err = printCompatibilityResult(results, f)
			out.MaybeDieErr(err)
		},
	}

	cmd.Flags().BoolVar(&global, "global", false, "Set the global level in addition to subject levels")
	cmd.Flags().StringVar(&level, "level", "", "Level to set, see the 'help' text of this command for the full list")
	cmd.MarkFlagRequired("level")
	return cmd
}

func printCompatibilityResult(results []sr.CompatibilityResult, f config.OutFormatter) error {
	var response []compatibilityLevelResponse
	for _, r := range results {
		if r.Subject == "" {
			r.Subject = "{GLOBAL}"
		}
		var err string
		if r.Err != nil {
			err = r.Err.Error()
		}
		response = append(response, compatibilityLevelResponse{
			r.Subject,
			r.Level.String(),
			err,
		})
	}
	if isText, _, s, err := f.Format(response); !isText {
		if err != nil {
			return fmt.Errorf("unable to print in the required format %q: %v", f.Kind, err)
		}
		fmt.Println(s)
		return nil
	}
	tw := out.NewTable("subject", "level", "error")
	defer tw.Flush()
	for _, r := range response {
		tw.PrintStructFields(compatibilityLevelResponse{
			r.Subject,
			r.Level,
			r.Err,
		})
	}
	return nil
}

const compatHelpText = `Set the global or per-subject compatibility levels.

Running this command without a subject sets the global compatibility level. To
set the global level at the same time as per-subject levels, use the --global
flag.

LEVELS:

  - BACKWARD (default): Consumers using the new schema (for example, version 10)
    can read data from producers using the previous schema (for example, version
    9).

  - BACKWARD_TRANSITIVE: Consumers using the new schema (for example, version
    10) can read data from producers using all previous schemas (for example,
    versions 1-9).

  - FORWARD: Consumers using the previous schema (for example, version 9) can
    read data from producers using the new schema (for example, version 10).

  - FORWARD_TRANSITIVE: Consumers using any previous schema (for example,
    versions 1-9) can read data from producers using the new schema (for example,
    version 10).

  - FULL: A new schema and the previous schema (for example, versions 10 and 9)
    are both backward and forward compatible with each other.

  - FULL_TRANSITIVE: Each schema is both backward and forward compatible with
    all registered schemas.

  - NONE: No schema compatibility checks are done.
`
