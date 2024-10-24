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

func newCreateCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var (
		refs       string
		schemaFile string
		schemaType string
	)
	cmd := &cobra.Command{
		Use:   "create SUBJECT --schema {filename}",
		Short: "Create a schema for the given subject",
		Long: `Create a schema for the given subject.

This uploads a schema to the registry, creating the schema if it does not
exist. The schema type is detected by the filename extension: ".avro" or ".avsc"
for Avro, ".json" for JSON, and ".proto" for Protobuf. You can manually specify 
the type with the --type flag.

You may pass the references using the --reference flag, which accepts either a
comma separated list of <name>:<subject>:<version> or a path to a file. The file 
must contain lines of name, subject, and version separated by a tab or space, or 
the equivalent in json / yaml format.

EXAMPLES

Create a protobuf schema with subject 'foo':
  rpk registry schema create foo --schema path/to/file.proto

Create an avro schema, passing the type via flags:
  rpk registry schema create foo --schema /path/to/file --type avro

Create a protobuf schema that references the schema in subject 'my_subject', 
version 1:
  rpk registry schema create foo --schema /path/to/file.proto --references my_name:my_subject:1
`,
		Args: cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			f := p.Formatter
			if h, ok := f.Help(subjectSchema{}); ok {
				out.Exit(h)
			}
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)

			cl, err := schemaregistry.NewClient(fs, p)
			out.MaybeDie(err, "unable to initialize schema registry client: %v", err)

			t, err := resolveSchemaType(schemaType, schemaFile)
			out.MaybeDieErr(err)

			file, err := os.ReadFile(schemaFile)
			out.MaybeDie(err, "unable to read %q: %v", schemaFile, err)

			references, err := parseReferenceFlag(fs, refs)
			out.MaybeDie(err, "unable to parse reference flag %q: %v", refs, err)

			subject := args[0]
			schema := sr.Schema{
				Schema:     string(file),
				Type:       t,
				References: references,
			}
			s, err := cl.CreateSchema(cmd.Context(), subject, schema)
			out.MaybeDie(err, "unable to create schema: %v", err)

			err = printSubjectSchemaTable(f, true, s)
			out.MaybeDieErr(err)
		},
	}

	cmd.Flags().StringVar(&schemaType, "type", "", fmt.Sprintf("Schema type (%v); overrides schema file extension", strings.Join(supportedTypes, ",")))
	cmd.Flags().StringVar(&schemaFile, "schema", "", "Schema filepath to upload, must be .avro, .avsc, or .proto")
	cmd.Flags().StringVar(&refs, "references", "", "Comma-separated list of references (name:subject:version) or path to reference file")
	cmd.MarkFlagRequired("schema")

	cmd.RegisterFlagCompletionFunc("type", validTypes())
	return cmd
}
