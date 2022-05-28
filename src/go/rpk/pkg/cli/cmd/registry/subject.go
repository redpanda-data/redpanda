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
	"fmt"
	"sync"

	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/sr"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
)

func subjectCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "subject",
		Args:  cobra.ExactArgs(0),
		Short: "List or delete schema registry subjects.",
	}

	cmd.AddCommand(
		subjectListCommand(),
		subjectDeleteCommand(),
	)
	return cmd
}

func subjectListCommand() *cobra.Command {
	var urls []string
	var deleted bool
	cmd := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "Display all subjects.",
		Args:    cobra.ExactArgs(0),
		Run: func(*cobra.Command, []string) {
			cl, err := sr.NewClient(sr.URLs(urls...))
			out.MaybeDie(err, "unable to initialize schema registry client: %v", err)
			subjects, err := cl.Subjects(context.Background(), sr.HideShowDeleted(deleted))
			out.MaybeDieErr(err)
			for _, s := range subjects {
				fmt.Println(s)
			}
		},
	}
	flagURLs(cmd, &urls)
	cmd.Flags().BoolVar(&deleted, "deleted", false, "if true, list deleted subjects as well")
	return cmd
}

func subjectDeleteCommand() *cobra.Command {
	var urls []string
	var permanent bool
	cmd := &cobra.Command{
		Use:   "delete [SUBJECT...]",
		Short: "Soft or hard delete subjects.",
		Args:  cobra.MinimumNArgs(1),
		Run: func(_ *cobra.Command, subjects []string) {
			cl, err := sr.NewClient(sr.URLs(urls...))
			out.MaybeDie(err, "unable to initialize schema registry client: %v", err)

			type res struct {
				Subject  string
				Versions []int
				Err      error
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
					versions, err := cl.DeleteSubject(context.Background(), subject, sr.DeleteHow(permanent))
					mu.Lock()
					defer mu.Unlock()
					results = append(results, res{
						subject,
						versions,
						err,
					})
				}()
			}
			wg.Wait()

			tw := out.NewTable("subject", "versions-deleted", "error")
			defer tw.Flush()
			for _, r := range results {
				var err string
				if r.Err != nil {
					err = r.Err.Error()
				}
				tw.PrintStructFields(struct {
					Subject  string
					Versions []int
					Err      string
				}{
					r.Subject,
					r.Versions,
					err,
				})
			}
		},
	}
	flagURLs(cmd, &urls)
	cmd.Flags().BoolVar(&permanent, "permanent", false, "perform a hard (permanent) delete of the subject; requires a soft-delete first")
	return cmd
}
