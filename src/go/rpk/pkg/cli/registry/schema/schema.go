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
	"regexp"
	"strconv"
	"strings"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/sr"
)

var supportedTypes = []string{"avro", "protobuf", "json"}

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

func validTypes() func(*cobra.Command, []string, string) ([]string, cobra.ShellCompDirective) {
	return func(_ *cobra.Command, _ []string, _ string) ([]string, cobra.ShellCompDirective) {
		return supportedTypes, cobra.ShellCompDirectiveDefault
	}
}

func typeFromFlag(typeFlag string) (sr.SchemaType, error) {
	switch strings.ToLower(typeFlag) {
	case "avro", "avsc":
		return sr.TypeAvro, nil
	case "proto", "protobuf":
		return sr.TypeProtobuf, nil
	case "json":
		return sr.TypeJSON, nil
	default:
		return 0, fmt.Errorf("unrecognized type %q", typeFlag)
	}
}

func typeFromFile(schemaFile string) (sr.SchemaType, error) {
	switch {
	case strings.HasSuffix(schemaFile, ".avro") || strings.HasSuffix(schemaFile, ".avsc"):
		return sr.TypeAvro, nil
	case strings.HasSuffix(schemaFile, ".proto") || strings.HasSuffix(schemaFile, ".protobuf"):
		return sr.TypeProtobuf, nil
	case strings.HasSuffix(schemaFile, ".json"):
		return sr.TypeJSON, nil
	default:
		return 0, fmt.Errorf("unable to determine the schema type from %q", schemaFile)
	}
}

// This regexp captures the reference flag in its simplest form which is:
//
//	name:subject:version
//
// However, name can also be a URI. Therefore, this regexp has 3 capturing
// groups:
//  1. Either alphabetic characters (scheme) followed by '://', then followed by
//     any other character except ':', or ':' followed by numbers and more
//     characters to capture optional ports in the authority and the path.
//  2. Captures any character except ':' (subject).
//  3. Captures any character except ':' (version).
var referenceRegex = regexp.MustCompile(`^([a-zA-Z]+://[^:]+(?::\d+)?(?:/[^:]+)*|[^:]+):([^:]+):([^:]+)$`)

// parseReferenceFlag parses the --reference flag which can be either a comma
// separated list of name:subject:version or a path to a file containing the
// schema references.
func parseReferenceFlag(fs afero.Fs, referenceFlag string) ([]sr.SchemaReference, error) {
	if referenceFlag == "" {
		// An empty flag is ok. We just send an empty list.
		return []sr.SchemaReference{}, nil
	}
	var references []sr.SchemaReference
	if strings.Contains(referenceFlag, ":") {
		split := strings.Split(referenceFlag, ",")
		for _, v := range split {
			match := referenceRegex.FindStringSubmatch(v)
			if len(match) == 0 {
				return nil, fmt.Errorf("unexpected format %q, please use name:subject:version format", referenceFlag)
			}
			name, subject, versionString := match[1], match[2], match[3] // match[0] is the full match
			if name == "" || subject == "" || versionString == "" {
				return nil, fmt.Errorf("invalid empty values in %q", referenceFlag)
			}
			version, err := strconv.Atoi(versionString)
			if err != nil {
				return nil, fmt.Errorf("unable to parse version %q: %v", versionString, err)
			}
			references = append(references, sr.SchemaReference{
				Name:    name,
				Subject: subject,
				Version: version,
			})
		}
	} else {
		parsed, err := out.ParseFileArray[struct {
			Name    string `json:"name" yaml:"name"`
			Subject string `json:"subject" yaml:"subject"`
			Version int    `json:"version" yaml:"version"`
		}](fs, referenceFlag)
		if err != nil {
			return nil, fmt.Errorf("unable to parse reference file: %v", err)
		}
		for _, r := range parsed {
			references = append(references, sr.SchemaReference{
				Name:    r.Name,
				Subject: r.Subject,
				Version: r.Version,
			})
		}
	}
	return references, nil
}
