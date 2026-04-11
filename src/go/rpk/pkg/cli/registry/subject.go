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
	"fmt"
	"slices"
	"sync"

	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/sr"

	srcontext "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/registry/context"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/schemaregistry"
)

func subjectCommand(fs afero.Fs, p *config.Params, schemaCtx *string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "subject",
		Args:  cobra.ExactArgs(0),
		Short: "List or delete schema registry subjects",
	}
	cmd.AddCommand(
		subjectListCommand(fs, p, schemaCtx),
		subjectDeleteCommand(fs, p, schemaCtx),
	)
	p.InstallFormatFlag(cmd)
	return cmd
}

type subjectWithContext struct {
	Context string `json:"context" yaml:"context"`
	Subject string `json:"subject" yaml:"subject"`
}

func subjectListCommand(fs afero.Fs, p *config.Params, schemaCtx *string) *cobra.Command {
	var deleted bool
	cmd := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "Display all subjects",
		Args:    cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, _ []string) {
			f := p.Formatter
			if h, ok := f.Help([]string{}); ok {
				out.Exit(h)
			}
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)

			cl, err := schemaregistry.NewClient(fs, p)
			out.MaybeDie(err, "unable to initialize schema registry client: %v", err)

			skipCheck, _ := cmd.Flags().GetBool("skip-context-check")
			showCtxCol := *schemaCtx == "" && srcontext.IsContextSupported(cmd.Context(), fs, p, skipCheck) == nil

			var params []sr.Param
			if deleted {
				params = append(params, sr.ShowDeleted)
			}
			if pfx := schemaregistry.ContextSubjectPrefix(*schemaCtx); pfx != "" {
				params = append(params, sr.SubjectPrefix(pfx))
			}
			ctx := cmd.Context()
			if len(params) > 0 {
				ctx = sr.WithParams(ctx, params...)
			}

			subjects, err := cl.Subjects(ctx)
			out.MaybeDieErr(err)
			slices.Sort(subjects)

			if showCtxCol {
				rows := make([]subjectWithContext, 0, len(subjects))
				for _, s := range subjects {
					sCtx, bare := schemaregistry.ParseSubjectContext(s)
					rows = append(rows, subjectWithContext{
						Context: schemaregistry.DisplayContext(sCtx),
						Subject: bare,
					})
				}
				if isText, _, s, err := f.Format(rows); !isText {
					out.MaybeDie(err, "unable to print in the required format %q: %v", f.Kind, err)
					out.Exit(s)
				}
				tw := out.NewTable("context", "subject")
				defer tw.Flush()
				for _, r := range rows {
					tw.PrintStructFields(r)
				}
				return
			}

			for i, s := range subjects {
				subjects[i] = schemaregistry.StripContextQualifier(*schemaCtx, s)
			}

			if isText, _, s, err := f.Format(subjects); !isText {
				out.MaybeDie(err, "unable to print in the required format %q: %v", f.Kind, err)
				out.Exit(s)
			}
			for _, s := range subjects {
				fmt.Println(s)
			}
		},
	}
	cmd.Flags().BoolVar(&deleted, "deleted", false, "If true, list deleted subjects as well")
	return cmd
}

type deleteResponse struct {
	Subject  string `json:"subject" yaml:"subject"`
	Versions []int  `json:"versions_deleted,omitempty" yaml:"versions_deleted,omitempty"`
	Err      string `json:"error,omitempty" yaml:"error,omitempty"`
}

func subjectDeleteCommand(fs afero.Fs, p *config.Params, schemaCtx *string) *cobra.Command {
	var isPermanent bool
	cmd := &cobra.Command{
		Use:   "delete [SUBJECT...]",
		Short: "Soft or hard deletion of subjects",
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, subjects []string) {
			f := p.Formatter
			if h, ok := f.Help([]deleteResponse{}); ok {
				out.Exit(h)
			}
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)

			cl, err := schemaregistry.NewClient(fs, p)
			out.MaybeDie(err, "unable to initialize schema registry client: %v", err)

			var (
				wg      sync.WaitGroup
				mu      sync.Mutex
				results []deleteResponse
			)

			for _, subject := range subjects {
				qualified := schemaregistry.QualifySubject(*schemaCtx, subject)
				wg.Add(1)
				go func() {
					defer wg.Done()
					var versions []int
					var err error
					if isPermanent {
						versions, err = cl.DeleteSubject(cmd.Context(), qualified, sr.SoftDelete)
						if err == nil || schemaregistry.IsSubjectNotFoundError(err) {
							versions, err = cl.DeleteSubject(cmd.Context(), qualified, sr.HardDelete)
							if err != nil {
								err = fmt.Errorf("unable to perform hard-deletion: %w", err)
							}
						} else {
							err = fmt.Errorf("unable to perform initial soft-deletion that is required for hard-deletion: %w", err)
						}
					} else {
						versions, err = cl.DeleteSubject(cmd.Context(), qualified, sr.SoftDelete)
					}
					mu.Lock()
					defer mu.Unlock()
					var errStr string
					if err != nil {
						errStr = err.Error()
					}
					results = append(results, deleteResponse{
						subject,
						versions,
						errStr,
					})
				}()
			}
			wg.Wait()
			if isText, _, s, err := f.Format(results); !isText {
				out.MaybeDie(err, "unable to print in the required format %q: %v", f.Kind, err)
				out.Exit(s)
			}
			tw := out.NewTable("subject", "versions-deleted", "error")
			defer tw.Flush()
			for _, r := range results {
				tw.PrintStructFields(struct {
					Subject  string
					Versions []int
					Err      string
				}{
					r.Subject,
					r.Versions,
					r.Err,
				})
			}
		},
	}
	cmd.Flags().BoolVar(&isPermanent, "permanent", false, "Perform a hard (permanent) delete of the subject")
	return cmd
}
