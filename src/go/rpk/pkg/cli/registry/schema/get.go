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
	"bytes"
	"encoding/json"
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

func newGetCommand(fs afero.Fs, p *config.Params, schemaCtx *string) *cobra.Command {
	var (
		deleted       bool
		printSchema   bool
		printMetadata bool
		id            int
		schemaFile    string
		schemaType    string
		sversion      string
	)
	cmd := &cobra.Command{
		Use:   "get [SUBJECT]",
		Short: "Get a schema by version, ID, or by an existing schema",
		Long: `Get a schema by version, ID, or by an existing schema.

This returns a lookup of an existing schema or schemas in one of a few
potential (mutually exclusive) ways:

* By version, returning a schema for a required subject and version
* By ID, returning all subjects using the schema, or filtering for one subject
* By schema, checking if the schema has been created in the subject

To print the schema, use the '--print-schema' flag.

To print schema metadata properties, use the '--print-metadata' flag.
`,
		Args: cobra.MaximumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			f := p.Formatter
			if printSchema && f.Kind != "text" {
				out.Die("--print-schema cannot be used along with --format %v", f.Kind)
			}
			if h, ok := f.Help([]subjectSchemaMetadata{}); ok {
				out.Exit(h)
			}
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)

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
			switch n {
			case 0:
				out.Die("must specify at least one of --schema-version, --id, or --schema")
			case 1:
			default:
				out.Die("must specify only one of --schema-version, --id, or --schema")
			}
			if len(args) == 0 && (sversion != "" || schemaFile != "") {
				out.Die("subject must be specified for --schema-version or --schema")
			}

			var ss []sr.SubjectSchema
			switch {
			case sversion != "":
				version, err := parseVersion(sversion)
				out.MaybeDieErr(err)
				qualified := schemaregistry.QualifySubject(*schemaCtx, args[0])
				var params []sr.Param
				if deleted {
					params = append(params, sr.ShowDeleted)
				}
				ctx := cmd.Context()
				if len(params) > 0 {
					ctx = sr.WithParams(ctx, params...)
				}
				s, err := cl.SchemaByVersion(ctx, qualified, version)
				out.MaybeDieErr(err)
				ss = []sr.SubjectSchema{s}

			case id != 0:
				// For ID lookup, combine ShowDeleted and Subject params.
				var params []sr.Param
				if deleted {
					params = append(params, sr.ShowDeleted)
				}
				if len(args) > 0 {
					params = append(params, sr.Subject(schemaregistry.QualifySubject(*schemaCtx, args[0])))
				} else if pfx := schemaregistry.ContextSubjectPrefix(*schemaCtx); pfx != "" {
					params = append(params, sr.Subject(pfx))
				}
				ctx := cmd.Context()
				if len(params) > 0 {
					ctx = sr.WithParams(ctx, params...)
				}
				ss, err = cl.SchemaUsagesByID(ctx, id)
				out.MaybeDieErr(err)
				if len(args) > 0 {
					qualified := schemaregistry.QualifySubject(*schemaCtx, args[0])
					for _, s := range ss {
						if s.Subject == qualified {
							ss = []sr.SubjectSchema{s}
							break
						}
					}
				}

			case schemaFile != "":
				file, err := os.ReadFile(schemaFile)
				out.MaybeDie(err, "unable to read %q: %v", err)
				t, err := resolveSchemaType(schemaType, schemaFile)
				out.MaybeDieErr(err)
				qualified := schemaregistry.QualifySubject(*schemaCtx, args[0])
				s, err := cl.LookupSchema(cmd.Context(), qualified, sr.Schema{
					Schema: string(file),
					Type:   t,
				})
				out.MaybeDieErr(err)
				ss = []sr.SubjectSchema{s}
			}

			for i := range ss {
				ss[i].Subject = schemaregistry.StripContextQualifier(*schemaCtx, ss[i].Subject)
			}
			if printSchema {
				printSchemaString(ss)
				return
			}
			err = printSubjectSchemaWithMetadata(f, false, printMetadata, ss...)
			out.MaybeDieErr(err)
		},
	}

	cmd.Flags().StringVar(&sversion, "schema-version", "", "Schema version to lookup (latest, 0, 1...); subject required")
	cmd.Flags().IntVar(&id, "id", 0, "ID to lookup schemas usages of; subject optional")
	cmd.Flags().StringVar(&schemaFile, "schema", "", "Schema file to check existence of, must be .avro, .json or .proto; subject required")
	cmd.Flags().StringVar(&schemaType, "type", "", fmt.Sprintf("Schema type of the file used to lookup (%v); overrides schema file extension", strings.Join(supportedTypes, ",")))
	cmd.Flags().BoolVar(&deleted, "deleted", false, "If true, also return deleted schemas")
	cmd.Flags().BoolVar(&printSchema, "print-schema", false, "Prints the schema in JSON format")
	cmd.Flags().BoolVar(&printMetadata, "print-metadata", false, "Print the schema metadata properties")

	cmd.MarkFlagsMutuallyExclusive("print-schema", "print-metadata")
	cmd.RegisterFlagCompletionFunc("type", validTypes())
	return cmd
}

func printSchemaString(ss []sr.SubjectSchema) {
	if len(ss) == 0 {
		fmt.Println("No schema was found")
	} else {
		// This command finds either a specific schema or all subjects using
		// the same schema. The first element in 'ss' always provides the
		// desired schema.
		s := ss[0].Schema
		if s.Type == sr.TypeProtobuf {
			fmt.Println(s.Schema)
			return
		}
		var prettySchema bytes.Buffer
		err := json.Indent(&prettySchema, []byte(s.Schema), "", "  ")
		if err != nil {
			// This is the best effort to print the pretty version. If it does
			// not succeed, we print the schema as it was saved.
			fmt.Println(s.Schema)
			return
		}
		fmt.Println(prettySchema.String())
	}
}
