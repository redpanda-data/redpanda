// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package context

import (
	"slices"
	"strings"
	"sync"

	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/sr"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/schemaregistry"
)

func listCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List schema registry contexts",
		Args:    cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, _ []string) {
			f := p.Formatter
			if h, ok := f.Help([]contextResponse{}); ok {
				out.Exit(h)
			}
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)

			cl, err := schemaregistry.NewClient(fs, p)
			out.MaybeDie(err, "unable to initialize schema registry client: %v", err)

			contexts, err := ListContexts(cmd.Context(), cl.Client)
			out.MaybeDie(err, "%v", err)

			slices.Sort(contexts)

			// Fetch mode and compatibility for each context in parallel.
			type ctxResult struct {
				name          string
				mode          string
				compatibility string
			}
			var (
				wg      sync.WaitGroup
				mu      sync.Mutex
				results []ctxResult
			)
			for _, c := range contexts {
				wg.Add(1)
				go func() {
					defer wg.Done()

					subject := schemaregistry.QualifySubject(c, sr.GlobalSubject)

					modeCtx := sr.WithParams(cmd.Context(), sr.DefaultToGlobal)
					modeStr := "-"
					modeResults := cl.Mode(modeCtx, subject)
					if len(modeResults) > 0 && modeResults[0].Err == nil {
						modeStr = modeResults[0].Mode.String()
					}

					compatStr := "-"
					compatResults := cl.Compatibility(cmd.Context(), subject)
					if len(compatResults) > 0 && compatResults[0].Err == nil {
						compatStr = compatResults[0].Level.String()
					}

					mu.Lock()
					defer mu.Unlock()
					results = append(results, ctxResult{
						name:          c,
						mode:          modeStr,
						compatibility: compatStr,
					})
				}()
			}
			wg.Wait()

			slices.SortFunc(results, func(a, b ctxResult) int {
				return strings.Compare(a.name, b.name)
			})

			rows := make([]contextResponse, 0, len(results))
			for _, r := range results {
				rows = append(rows, contextResponse{
					Name:          r.name,
					Mode:          r.mode,
					Compatibility: r.compatibility,
				})
			}

			if isText, _, s, err := f.Format(rows); !isText {
				out.MaybeDie(err, "unable to print in the required format %q: %v", f.Kind, err)
				out.Exit(s)
			}
			tw := out.NewTable("context", "mode", "compatibility")
			defer tw.Flush()
			for _, r := range rows {
				tw.PrintStructFields(r)
			}
		},
	}
	p.InstallFormatFlag(cmd)
	return cmd
}
