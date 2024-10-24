// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package schema

import (
	"sync"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/schemaregistry"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/sr"
	"github.com/twmb/types"
)

type schemaResponse struct {
	Subject string `json:"subject" yaml:"subject"`
	Version int    `json:"version,omitempty" yaml:"version,omitempty"`
	ID      int    `json:"id,omitempty" yaml:"id,omitempty"`
	Type    string `json:"type,omitempty" yaml:"type,omitempty"`
	Err     string `json:"error,omitempty" yaml:"error,omitempty"`
}

func newListCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var deleted bool
	cmd := &cobra.Command{
		Use:     "list [SUBJECT...]",
		Aliases: []string{"ls"},
		Short:   "List the schemas for the requested subjects, or list all schemas",
		Run: func(cmd *cobra.Command, subjects []string) {
			f := p.Formatter
			if h, ok := f.Help([]schemaResponse{}); ok {
				out.Exit(h)
			}
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)

			cl, err := schemaregistry.NewClient(fs, p)
			out.MaybeDie(err, "unable to initialize schema registry client: %v", err)

			ctx := cmd.Context()
			if deleted {
				ctx = sr.WithParams(cmd.Context(), sr.ShowDeleted)
			}
			if len(subjects) == 0 {
				subjects, err = cl.Subjects(ctx)
				out.MaybeDieErr(err)
			}

			type res struct {
				subject string
				ss      []sr.SubjectSchema
				err     error
			}
			var (
				wg      sync.WaitGroup
				mu      sync.Mutex
				results []res
			)
			for i := range subjects {
				subject := subjects[i]
				wg.Add(1)
				go func() {
					defer wg.Done()
					ss, err := cl.Schemas(ctx, subject)
					mu.Lock()
					defer mu.Unlock()
					results = append(results, res{
						subject: subject,
						ss:      ss,
						err:     err,
					})
				}()
			}
			wg.Wait()

			types.Sort(results)

			// We use literal here to display an empty array when unmarshalling
			// an empty list.
			response := []schemaResponse{}
			for _, res := range results {
				if res.err != nil {
					sc := schemaResponse{Subject: res.subject, Err: res.err.Error()}
					response = append(response, sc)
					continue
				}
				for _, s := range res.ss {
					sc := schemaResponse{
						Subject: s.Subject,
						Version: s.Version,
						ID:      s.ID,
						Type:    s.Type.String(),
					}
					response = append(response, sc)
				}
			}
			if isText, _, s, err := f.Format(response); !isText {
				out.MaybeDie(err, "unable to print in the required format %q: %v", f.Kind, err)
				out.Exit(s)
			}
			tw := out.NewTable("subject", "version", "id", "type", "error")
			defer tw.Flush()
			for _, r := range response {
				tw.PrintStructFields(r)
			}
		},
	}

	cmd.Flags().BoolVar(&deleted, "deleted", false, "If true, list deleted schemas as well")
	return cmd
}
