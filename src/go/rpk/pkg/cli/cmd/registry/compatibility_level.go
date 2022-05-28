// Copyright 2022 Redpanda Data, Inc.
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

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/sr"
)

func compatibilityLevelCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "compatibility-level",
		Args:  cobra.ExactArgs(0),
		Short: "Manage global or per-subject compatibility levels.",
	}

	cmd.AddCommand(
		compatGetCommand(),
		compatSetCommand(),
	)
	return cmd
}

func compatGetCommand() *cobra.Command {
	var urls []string
	var global bool
	cmd := &cobra.Command{
		Use:   "get",
		Short: "Get the global or per-subject compatibility levels",
		Long: `Get the global or per-subject compatibility levels.

Running this command with no subject returns the global level, alternatively
you can use the --global flag to get the global level at the same time as
per-subject levels.
`,
		Run: func(_ *cobra.Command, subjects []string) {
			cl, err := sr.NewClient(sr.URLs(urls...))
			out.MaybeDie(err, "unable to initialize schema registry client: %v", err)
			if len(subjects) > 0 && global {
				subjects = append(subjects, sr.GlobalSubject)
			}
			results := cl.CompatibilityLevel(context.Background(), subjects...)

			tw := out.NewTable("subject", "level", "error")
			defer tw.Flush()
			for _, r := range results {
				if r.Subject == "" {
					r.Subject = "{GLOBAL}"
				}
				var err string
				if r.Err != nil {
					err = r.Err.Error()
				}
				tw.PrintStructFields(struct {
					Subject string
					Level   string
					Err     string
				}{
					r.Subject,
					r.Level.String(),
					err,
				})
			}
		},
	}
	flagURLs(cmd, &urls)
	cmd.Flags().BoolVar(&global, "global", false, "return the global level in addition to subject levels")
	return cmd
}

/* DELETE /config/{subject} not implemented by Redpanda yet
func compatResetCommand() *cobra.Command {
	var urls []string
	cmd := &cobra.Command{
		Use:   "reset",
		Short: "Reset subject compatibility levels to the global level",
		Long:  `Reset subject compatibility levels to the global level.`,
		Args:  cobra.MinimumNArgs(1),
		Run: func(_ *cobra.Command, subjects []string) {
			cl, err := sr.NewClient(sr.URLs(urls...))
			out.MaybeDie(err, "unable to initialize schema registry client: %v", err)
			results := cl.ResetCompatibilityLevel(context.Background(), subjects...)

			tw := out.NewTable("subject", "level", "error")
			defer tw.Flush()
			for _, r := range results {
				var err string
				if r.Err != nil {
					err = r.Err.Error()
				}
				tw.PrintStructFields(struct {
					Subject string
					Level   string
					Err     string
				}{
					r.Subject,
					r.Level.String(),
					err,
				})
			}
		},
	}
	flagURLs(cmd, &urls)
	return cmd
}
*/

func compatSetCommand() *cobra.Command {
	var urls []string
	var global bool
	var level string
	cmd := &cobra.Command{
		Use:   "set",
		Short: "Set the global or per-subject compatibility levels",
		Long: `Set the global or per-subject compatibility levels.

Running this command with no subject returns the global level, alternatively
you can use the --global flag to set the global level at the same time as
per-subject levels.
`,
		Run: func(_ *cobra.Command, subjects []string) {
			cl, err := sr.NewClient(sr.URLs(urls...))
			out.MaybeDie(err, "unable to initialize schema registry client: %v", err)
			if len(subjects) > 0 && global {
				subjects = append(subjects, sr.GlobalSubject)
			}
			var l sr.CompatibilityLevel
			err = l.UnmarshalText([]byte(level))
			out.MaybeDieErr(err)
			results := cl.SetCompatibilityLevel(context.Background(), l, subjects...)

			tw := out.NewTable("subject", "level", "error")
			defer tw.Flush()
			for _, r := range results {
				if r.Subject == "" {
					r.Subject = "{GLOBAL}"
				}
				var err string
				if r.Err != nil {
					err = r.Err.Error()
				}
				tw.PrintStructFields(struct {
					Subject string
					Level   string
					Err     string
				}{
					r.Subject,
					r.Level.String(),
					err,
				})
			}
		},
	}
	flagURLs(cmd, &urls)
	cmd.Flags().BoolVar(&global, "global", false, "set the global level in addition to subject levels")
	cmd.Flags().StringVar(&level, "level", "", "level to set, one of NONE, {BACKWARD,FORWARD,FULL}{,_TRANSITIVE}")
	cmd.MarkFlagRequired("level")
	return cmd
}
