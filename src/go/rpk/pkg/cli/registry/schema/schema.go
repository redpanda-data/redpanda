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
	"strconv"
	"strings"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/sr"
)

func NewCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "schema",
		Args:  cobra.ExactArgs(0),
		Short: "Manage your schema registry schemas",
	}
	cmd.AddCommand(
		newCheckCompatibilityCommand(fs, p),
		newCreateCommand(fs, p),
		newDeleteCommand(fs, p),
		newGetCommand(fs, p),
		newListCommand(fs, p),
		newReferencesCommand(fs, p),
	)
	p.InstallFormatFlag(cmd)
	return cmd
}

type subjectSchema struct {
	Subject string `json:"subject" yaml:"subject"`
	Version int    `json:"version" yaml:"version"`
	ID      int    `json:"id" yaml:"id"`
	Type    string `json:"type" yaml:"type"`
}

func printSubjectSchemaTable(f config.OutFormatter, single bool, ss ...sr.SubjectSchema) error {
	var rows []subjectSchema
	for _, s := range ss {
		rows = append(rows, subjectSchema{
			Subject: s.Subject,
			Version: s.Version,
			ID:      s.ID,
			Type:    s.Type.String(),
		})
	}
	// There are commands where a table for text format is fine, but a json
	// list does not make sense (e.g: schema create)
	toPrint := any(rows)
	if single && len(ss) == 1 {
		toPrint = rows[0]
	}
	if isText, _, s, err := f.Format(toPrint); !isText {
		if err != nil {
			return fmt.Errorf("unable to print in the required format %q: %v", f.Kind, err)
		}
		fmt.Println(s)
		return nil
	}
	tw := out.NewTable("subject", "version", "id", "type")
	defer tw.Flush()
	for _, r := range rows {
		tw.PrintStructFields(r)
	}
	return nil
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

// resolveSchemaType retrieves the parsed schema type from the provided
// typeFlag, or infers the type from the schema file extension if the flag is
// not provided.
func resolveSchemaType(typeFlag, schemaFile string) (t sr.SchemaType, err error) {
	if typeFlag != "" {
		t, err = typeFromFlag(typeFlag)
	} else {
		t, err = typeFromFile(schemaFile)
	}
	return
}

func typeFromFlag(typeFlag string) (sr.SchemaType, error) {
	switch strings.ToLower(typeFlag) {
	case "avro", "avsc":
		return sr.TypeAvro, nil
	case "proto", "protobuf":
		return sr.TypeProtobuf, nil
	default:
		return 0, fmt.Errorf("unrecognized type %q", typeFlag)
	}
}

func typeFromFile(schemaFile string) (sr.SchemaType, error) {
	switch {
	case strings.HasSuffix(schemaFile, ".avro") || strings.HasSuffix(schemaFile, ".avsc"):
		return sr.TypeAvro, nil
	case strings.HasSuffix(schemaFile, ".proto"):
		return sr.TypeProtobuf, nil
	default:
		return 0, fmt.Errorf("unable to determine the schema type from %q", schemaFile)
	}
}
