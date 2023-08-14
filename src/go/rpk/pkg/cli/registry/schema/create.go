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
	"os"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/schemaregistry"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/sr"
)

func newCreateCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var schemaFile string
	var schemaType string
	cmd := &cobra.Command{
		Use:   "create SUBJECT --schema {filename}",
		Short: "Create a schema for the given subject",
		Long: `Create a schema for the given subject.

This uploads a schema to the registry, creating the schema if it does not
exist. The schema type is detected by the filename extension: ".avro" or ".avsc"
for Avro and ".proto" for Protobuf. You can manually specify the type with the 
--type flag.
`,
		Args: cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			f := p.Formatter
			if h, ok := f.Help(subjectSchema{}); ok {
				out.Exit(h)
			}
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			cl, err := schemaregistry.NewClient(fs, p)
			out.MaybeDie(err, "unable to initialize schema registry client: %v", err)

			file, err := os.ReadFile(schemaFile)
			out.MaybeDie(err, "unable to read %q: %v", schemaFile, err)

			t, err := resolveSchemaType(schemaType, schemaFile)
			out.MaybeDieErr(err)

			subject := args[0]
			schema := sr.Schema{
				Schema: string(file),
				Type:   t,
			}
			s, err := cl.CreateSchema(cmd.Context(), subject, schema)
			out.MaybeDie(err, "unable to create schema: %v", err)

			err = printSubjectSchemaTable(f, true, s)
			out.MaybeDieErr(err)
		},
	}

	cmd.Flags().StringVar(&schemaFile, "schema", "", "Schema filepath to upload, must be .avro, .avsc, or .proto")
	cmd.Flags().StringVar(&schemaType, "type", "", "Schema type, one of protobuf or avro; overrides schema file extension")
	cmd.MarkFlagRequired("schema")

	return cmd
}
