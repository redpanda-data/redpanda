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
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/sr"
	"github.com/twmb/types"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
)

// TODO: add references flag, `references` command
// rpk registry schema check-compatibility {subject} --version {version} --schema {schema}

func schemaCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "schema",
		Args:  cobra.ExactArgs(0),
		Short: "Manage your schema registry schemas.",
	}

	cmd.AddCommand(
		schemaCreateCommand(),
		schemaCheckCompatibilityCommand(),
		schemaDeleteCommand(),
		schemaGetCommand(),
		schemaListCommand(),
	)
	return cmd
}

func printSubjectSchemaTable(ss ...sr.SubjectSchema) {
	tw := out.NewTable("subject", "version", "id", "type")
	defer tw.Flush()
	for _, s := range ss {
		printSubjectSchema(tw, s)
	}
}

func printSubjectSchema(tw *out.TabWriter, s sr.SubjectSchema) {
	tw.PrintStructFields(struct {
		Subject string
		Version int
		ID      int
		Type    string
	}{
		s.Subject,
		s.Version,
		s.ID,
		s.Type.String(),
	})
}

func parseVersion(version string) (int, error) {
	if version == "latest" {
		return -1, nil
	}
	i, err := strconv.Atoi(version)
	if err != nil {
		return 0, fmt.Errorf("unable to parse version %q", version)
	}
	if i < -1 {
		return 0, fmt.Errorf("invalid version %d", i)
	}
	return i, nil
}

func schemaCheckCompatibilityCommand() *cobra.Command {
	var urls []string
	var schemaFile string
	var sversion string
	cmd := &cobra.Command{
		Use:   "check-compatibility SUBJECT",
		Short: "Check schema compatibility with existing schemas in the subject",
		Args:  cobra.ExactArgs(1),
		Run: func(_ *cobra.Command, args []string) {
			cl, err := sr.NewClient(sr.URLs(urls...))
			out.MaybeDie(err, "unable to initialize schema registry client: %v", err)

			version, err := parseVersion(sversion)
			out.MaybeDieErr(err)

			f, err := os.ReadFile(schemaFile)
			out.MaybeDie(err, "unable to read %q: %v", schemaFile, err)

			var t sr.SchemaType
			switch {
			case strings.HasSuffix(schemaFile, ".avro"):
				t = sr.TypeAvro
			case strings.HasSuffix(schemaFile, ".proto"):
				t = sr.TypeProtobuf
			default:
				out.Die("unable to determine the schema type from %q", schemaFile)
			}

			subject := args[0]
			schema := sr.Schema{
				Schema: string(f),
				Type:   t,
			}
			compatible, err := cl.CheckCompatibility(context.Background(), subject, version, schema)
			out.MaybeDie(err, "unable to create schema: %v", err)
			if compatible {
				fmt.Println("Schema is compatible.")
			} else {
				fmt.Println("Schema is not compatible.")
			}
		},
	}

	flagURLs(cmd, &urls)
	cmd.Flags().StringVar(&schemaFile, "schema", "", "schema filepath to upload, must be .avro or .proto")
	cmd.Flags().StringVar(&sversion, "version", "", "version to check compatibility with")
	cmd.MarkFlagRequired("schema")
	cmd.MarkFlagRequired("version")
	return cmd
}

func schemaCreateCommand() *cobra.Command {
	var urls []string
	var schemaFile string
	cmd := &cobra.Command{
		Use:   "create SUBJECT --schema {filename}",
		Short: "Create a schema for the given subject.",
		Long: `Create a schema for the given subject.

This uploads a schema to the registry, creating the schema if it does not
exist. The schema type is detected by the filename extension: ".avro" for Avro
and ".proto" for Protobuf. You can manually specify the type with the --type
flag.

`,

		Args: cobra.ExactArgs(1),
		Run: func(_ *cobra.Command, args []string) {
			cl, err := sr.NewClient(sr.URLs(urls...))
			out.MaybeDie(err, "unable to initialize schema registry client: %v", err)

			f, err := os.ReadFile(schemaFile)
			out.MaybeDie(err, "unable to read %q: %v", schemaFile, err)

			var t sr.SchemaType
			switch {
			case strings.HasSuffix(schemaFile, ".avro"):
				t = sr.TypeAvro
			case strings.HasSuffix(schemaFile, ".proto"):
				t = sr.TypeProtobuf
			default:
				out.Die("unable to determine the schema type from %q", schemaFile)
			}

			subject := args[0]
			schema := sr.Schema{
				Schema: string(f),
				Type:   t,
			}
			s, err := cl.CreateSchema(context.Background(), subject, schema)
			out.MaybeDie(err, "unable to create schema: %v", err)

			printSubjectSchemaTable(s)
		},
	}

	flagURLs(cmd, &urls)
	cmd.Flags().StringVar(&schemaFile, "schema", "", "schema filepath to upload, must be .avro or .proto")

	cmd.MarkFlagRequired("schema")
	return cmd
}

func schemaDeleteCommand() *cobra.Command {
	var urls []string
	var sversion string
	var permanent bool
	cmd := &cobra.Command{
		Use:   "delete SUBJECT --version {version}",
		Short: "Delete a specific schema for the given subject.",
		Args:  cobra.ExactArgs(1),

		Run: func(_ *cobra.Command, args []string) {
			cl, err := sr.NewClient(sr.URLs(urls...))
			out.MaybeDie(err, "unable to initialize schema registry client: %v", err)
			version, err := parseVersion(sversion)
			out.MaybeDieErr(err)
			subject := args[0]
			err = cl.DeleteSchema(context.Background(), subject, version, sr.DeleteHow(permanent))
			out.MaybeDieErr(err)
			fmt.Println("OK")
		},
	}
	flagURLs(cmd, &urls)
	cmd.Flags().StringVar(&sversion, "version", "", "version to delete, -1 or latest deletes the latest version")
	cmd.Flags().BoolVar(&permanent, "permanent", false, "perform a hard (permanent) delete of the schema; requires a soft-delete first")
	cmd.MarkFlagRequired("version")
	return cmd
}

func schemaGetCommand() *cobra.Command {
	var (
		urls       []string
		sversion   string
		id         int
		schemaFile string
		deleted    bool
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

		Run: func(_ *cobra.Command, args []string) {
			cl, err := sr.NewClient(sr.URLs(urls...))
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
				out.Die("Must specify at least one of --version, --id, or --schema.")
			case n == 1:
			default:
				out.Die("Must specify only one of --version, --id, or --schema.")
			}
			if len(args) == 0 && (sversion != "" || schemaFile != "") {
				out.Die("Subject must be specified for --version or --schema.")
			}

			switch {
			case sversion != "":
				version, err := parseVersion(sversion)
				out.MaybeDieErr(err)
				s, err := cl.SchemaByVersion(context.Background(), args[0], version, sr.HideShowDeleted(deleted))
				out.MaybeDieErr(err)
				printSubjectSchemaTable(s)

			case id != 0:
				ss, err := cl.SchemaUsagesByID(context.Background(), id, sr.HideShowDeleted(deleted))
				out.MaybeDieErr(err)
				if len(args) == 0 {
					printSubjectSchemaTable(ss...)
					return
				}
				for _, s := range ss {
					if s.Subject == args[0] {
						printSubjectSchemaTable(s)
						return
					}
				}

			case schemaFile != "":
				f, err := os.ReadFile(schemaFile)
				out.MaybeDie(err, "unable to read %q: %v", err)
				var t sr.SchemaType
				switch {
				case strings.HasSuffix(schemaFile, ".avro"):
					t = sr.TypeAvro
				case strings.HasSuffix(schemaFile, ".proto"):
					t = sr.TypeProtobuf
				default:
					out.Die("unable to determine the schema type from %q", schemaFile)
				}
				s, err := cl.LookupSchema(context.Background(), args[0], sr.Schema{
					Schema: string(f),
					Type:   t,
				})
				out.MaybeDieErr(err)
				printSubjectSchemaTable(s)
			}
		},
	}
	flagURLs(cmd, &urls)
	cmd.Flags().StringVar(&sversion, "version", "", "version to lookup, or -1 or latest for the latest version, requires subject")
	cmd.Flags().IntVar(&id, "id", 0, "ID to lookup schemas usages of, subject optional")
	cmd.Flags().StringVar(&schemaFile, "schema", "", "schema file to check existence of, must be .avro or .proto, requires subject")
	cmd.Flags().BoolVar(&deleted, "deleted", false, "if true, allow getting deleted schemas")
	return cmd
}

func schemaListCommand() *cobra.Command {
	var urls []string
	var deleted bool
	cmd := &cobra.Command{
		Use:   "list [SUBJECT...]",
		Short: "List the schemas for the requested subjects, or list all schemas.",

		Run: func(_ *cobra.Command, subjects []string) {
			cl, err := sr.NewClient(sr.URLs(urls...))
			out.MaybeDie(err, "unable to initialize schema registry client: %v", err)

			if len(subjects) == 0 {
				subjects, err = cl.Subjects(context.Background(), sr.HideShowDeleted(deleted))
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
					ss, err := cl.Schemas(context.Background(), subject, sr.HideShowDeleted(deleted))
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

			type row struct {
				Subject string
				Version int
				ID      int
				Type    string
				Err     string
			}

			tw := out.NewTable("subject", "version", "id", "type", "error")
			defer tw.Flush()
			for _, res := range results {
				if res.err != nil {
					tw.PrintStructFields(row{
						Subject: res.subject,
						Err:     res.err.Error(),
					})
					continue
				}
				for _, s := range res.ss {
					tw.PrintStructFields(row{
						Subject: s.Subject,
						Version: s.Version,
						ID:      s.ID,
						Type:    s.Type.String(),
					})
				}
			}
		},
	}

	flagURLs(cmd, &urls)
	cmd.Flags().BoolVar(&deleted, "deleted", false, "if true, list deleted schemas as well")
	return cmd
}
