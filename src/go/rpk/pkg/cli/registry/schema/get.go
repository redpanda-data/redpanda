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
	"fmt"
	"os"
	"strings"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/schemaregistry"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/sr"
)

func newGetCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var (
		deleted    bool
		id         int
		schemaFile string
		schemaType string
		sversion   string
	)
	cmd := &cobra.Command{
		Use:   "get",
		Short: "Get a schema by version, ID, or by an existing schema",
		Long: `Get a schema by version, ID, or by an existing schema.

This returns a lookup of an existing schema or schemas in one of a few
potential (mutually exclusive) ways:

* By version, returning a schema for a required subject and version
* By ID, returning all subjects using the schema, or filtering for one subject
* By schema, checking if the schema has been created in the subject
`,
		Args: cobra.MaximumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			f := p.Formatter
			if h, ok := f.Help([]subjectSchema{}); ok {
				out.Exit(h)
			}
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			cl, err := schemaregistry.NewClient(fs, p)
			out.MaybeDie(err, "unable to initialize schema registry client: %v", err)

			var n int
			if sversion != "" {
				n++
			}
			if id != 0 {
				n++
			}
			if schemaFile != "" {
				n++
			}
			switch {
			case n == 0:
				out.Die("Must specify at least one of --schema-version, --id, or --schema.")
			case n == 1:
			default:
				out.Die("Must specify only one of --schema-version, --id, or --schema.")
			}
			if len(args) == 0 && (sversion != "" || schemaFile != "") {
				out.Die("Subject must be specified for --schema-version or --schema.")
			}

			var ss []sr.SubjectSchema
			switch {
			case sversion != "":
				version, err := parseVersion(sversion)
				out.MaybeDieErr(err)
				s, err := cl.SchemaByVersion(cmd.Context(), args[0], version, sr.HideShowDeleted(deleted))
				out.MaybeDieErr(err)
				ss = []sr.SubjectSchema{s}

			case id != 0:
				ss, err = cl.SchemaUsagesByID(cmd.Context(), id, sr.HideShowDeleted(deleted))
				out.MaybeDieErr(err)
				if len(args) == 0 {
					break
				}
				for _, s := range ss {
					if s.Subject == args[0] {
						ss = []sr.SubjectSchema{s}
					}
				}

			case schemaFile != "":
				file, err := os.ReadFile(schemaFile)
				out.MaybeDie(err, "unable to read %q: %v", err)
				t, err := resolveSchemaType(schemaType, schemaFile)
				out.MaybeDieErr(err)
				s, err := cl.LookupSchema(cmd.Context(), args[0], sr.Schema{
					Schema: string(file),
					Type:   t,
				})
				out.MaybeDieErr(err)
				ss = []sr.SubjectSchema{s}
			}
			err = printSubjectSchemaTable(f, false, ss...)
			out.MaybeDieErr(err)
		},
	}

	cmd.Flags().StringVar(&sversion, "schema-version", "", "Schema version to lookup (latest, 0, 1...); subject required")
	cmd.Flags().IntVar(&id, "id", 0, "ID to lookup schemas usages of; subject optional")
	cmd.Flags().StringVar(&schemaFile, "schema", "", "Schema file to check existence of, must be .avro or .proto; subject required")
	cmd.Flags().StringVar(&schemaType, "type", "", fmt.Sprintf("Schema type of the file used to lookup (%v); overrides schema file extension", strings.Join(supportedTypes, ",")))
	cmd.Flags().BoolVar(&deleted, "deleted", false, "If true, also return deleted schemas")

	cmd.RegisterFlagCompletionFunc("type", validTypes())
	return cmd
}
