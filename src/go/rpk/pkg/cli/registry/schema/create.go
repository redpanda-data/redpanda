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

func newCreateCommand(fs afero.Fs, p *config.Params, schemaCtx *string) *cobra.Command {
	var (
		refs               string
		schemaFile         string
		schemaType         string
		id                 int
		schemaVersion      int
		metadataProperties []string
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

In import mode, you can specify a schema ID and version using the --id and
--schema-version flags to assign specific values when creating the schema.

EXAMPLES

Create a protobuf schema with subject 'foo':
  rpk registry schema create foo --schema path/to/file.proto

Create an avro schema, passing the type via flags:
  rpk registry schema create foo --schema /path/to/file --type avro

Create a protobuf schema that references the schema in subject 'my_subject', 
version 1:
  rpk registry schema create foo --schema /path/to/file.proto --references my_name:my_subject:1

Create a schema with a specific ID and version in import mode:
  rpk registry schema create foo --schema /path/to/file.proto --id 42 --schema-version 3

Create a schema with metadata properties:
  rpk registry schema create foo --schema /path/to/file.proto --metadata-properties owner=team-a --metadata-properties env=prod

Create a schema with metadata properties using JSON format (useful for special characters):
  rpk registry schema create foo --schema /path/to/file.proto --metadata-properties '{"owner":"team-a","env":"prod"}'
`,
		Args: cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			f := p.Formatter
			if h, ok := f.Help(subjectSchemaMetadata{}); ok {
				out.Exit(h)
			}

			if schemaVersion != -1 && id == -1 {
				out.Die("--schema-version requires --id to be specified")
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

			mProperties, err := parseMetadataProperties(metadataProperties)
			out.MaybeDie(err, "unable to parse metadata properties flag %q: %v", metadataProperties, err)

			var schemaMetadata *sr.SchemaMetadata
			if mProperties != nil {
				schemaMetadata = &sr.SchemaMetadata{Properties: mProperties}
			}
			subject := schemaregistry.QualifySubject(*schemaCtx, args[0])
			schema := sr.Schema{
				Schema:         string(file),
				Type:           t,
				References:     references,
				SchemaMetadata: schemaMetadata,
			}

			// Pass the subject param so the post-create lookup
			// (SchemaUsagesByID) is scoped to the right context.
			// This is needed both when --schema-context is set and
			// when the user passes a raw qualified subject like
			// ":.ctx:foo".
			ctx := cmd.Context()
			if *schemaCtx != "" || strings.HasPrefix(subject, ":") {
				ctx = sr.WithParams(ctx, sr.Subject(subject))
			}
			s, err := cl.CreateSchemaWithIDAndVersion(ctx, subject, schema, id, schemaVersion)
			out.MaybeDie(err, "unable to create schema: %v", err)
			s.Subject = schemaregistry.StripContextQualifier(*schemaCtx, s.Subject)

			err = printSubjectSchemaWithMetadata(f, true, len(mProperties) > 0, s)
			out.MaybeDieErr(err)
		},
	}

	cmd.Flags().StringVar(&schemaType, "type", "", fmt.Sprintf("Schema type (%v); overrides schema file extension", strings.Join(supportedTypes, ",")))
	cmd.Flags().StringVar(&schemaFile, "schema", "", "Schema filepath to upload, must be .avro, .avsc, or .proto")
	cmd.Flags().StringVar(&refs, "references", "", "Comma-separated list of references (name:subject:version) or path to reference file")
	cmd.Flags().IntVar(&id, "id", -1, "Optional schema ID to use when creating the schema in import mode")
	cmd.Flags().IntVar(&schemaVersion, "schema-version", -1, "Optional schema version to use when creating the schema in import mode (requires --id)")
	// This has to be a ArrayVar to allow both key=values AND JSON, as JSON
	// contains "," and a StringSliceVar flag will use "," to separate values.
	cmd.Flags().StringArrayVarP(&metadataProperties, "metadata-properties", "p", nil, "Schema metadata properties as key=value pairs or JSON (e.g., '{\"key\":\"value\"}')")

	cmd.MarkFlagRequired("schema")
	cmd.RegisterFlagCompletionFunc("type", validTypes())
	return cmd
}

func parseMetadataProperties(metadataProperties []string) (map[string]string, error) {
	if len(metadataProperties) == 0 {
		return nil, nil
	}
	result := make(map[string]string)
	for _, prop := range metadataProperties {
		m, err := parseMetadataProperty(prop)
		if err != nil {
			return nil, err
		}
		for k, v := range m {
			result[k] = v
		}
	}
	return result, nil
}

func parseMetadataProperty(prop string) (map[string]string, error) {
	trimmed := strings.TrimSpace(prop)
	// If it looks like JSON, try JSON first
	if strings.HasPrefix(trimmed, "{") && strings.HasSuffix(trimmed, "}") {
		m := make(map[string]string)
		jsonErr := json.Unmarshal([]byte(trimmed), &m)
		if jsonErr == nil {
			return m, nil
		}
		// JSON failed, try key=value
		kv, kvErr := parseMetadataPropertyKV(trimmed)
		if kvErr != nil {
			return nil, fmt.Errorf("unable to parse %q as JSON (%v) or as key=value (%v)", prop, jsonErr, kvErr)
		}
		return kv, nil
	}

	return parseMetadataPropertyKV(trimmed)
}

func parseMetadataPropertyKV(prop string) (map[string]string, error) {
	parts := strings.SplitN(prop, "=", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("expected key=value format, got %q", prop)
	}
	k := strings.TrimSpace(parts[0])
	v := strings.TrimSpace(parts[1])
	if k == "" {
		return nil, fmt.Errorf("empty key in %q", prop)
	}
	if v == "" {
		return nil, fmt.Errorf("empty value in %q", prop)
	}
	return map[string]string{k: v}, nil
}
