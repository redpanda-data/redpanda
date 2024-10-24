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
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/schemaregistry"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/sr"
)

func newReferencesCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var sversion string
	var deleted bool
	cmd := &cobra.Command{
		Use:   "references SUBJECT --schema-version {version}",
		Short: "Retrieve a list of schemas that reference the subject",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			f := p.Formatter
			if h, ok := f.Help([]subjectSchema{}); ok {
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

			version, err := parseVersion(sversion)
			out.MaybeDieErr(err)

			subject := args[0]
			references, err := cl.SchemaReferences(ctx, subject, version)
			out.MaybeDie(err, "unable to check for references of subject %q and version %q: %v", subject, sversion, err)

			err = printSubjectSchemaTable(f, false, references...)
			out.MaybeDieErr(err)
		},
	}

	cmd.Flags().StringVar(&sversion, "schema-version", "", "Schema version to check references with (latest, 0, 1...)")
	cmd.Flags().BoolVar(&deleted, "deleted", false, "If true, list deleted schemas as well")
	cmd.MarkFlagRequired("schema-version")

	return cmd
}
