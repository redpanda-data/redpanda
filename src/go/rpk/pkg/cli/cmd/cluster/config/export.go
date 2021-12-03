// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package config

import (
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/api/admin"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/config"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/out"
	yaml "gopkg.in/yaml.v3"
)

func exportConfig(
	file *os.File, schema admin.ConfigSchema, config admin.Config, all bool,
) (err error) {
	template := make(map[string]interface{})

	// Present properties in alphabetical order, providing some pseudo-grouping based on common prefixes
	keys := make([]string, 0, len(schema))
	for k := range schema {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	yamlNode := yaml.Node{
		Kind: yaml.MappingNode,
	}

	for _, name := range keys {
		meta := schema[name]
		curValue := config[name]
		template[name] = curValue

		visibility := meta.Visibility

		if visibility == "deprecated" {
			continue
		}

		if meta.Visibility == "tunable" && !all {
			continue
		}

		// Preface each property with a descriptive comment
		var commentTokens []string

		if meta.Example != "" {
			commentTokens = append(commentTokens, fmt.Sprintf("e.g. '%s'", meta.Example))
		}

		if meta.NeedsRestart {
			commentTokens = append(commentTokens, "restart required")
		}

		if meta.Nullable {
			commentTokens = append(commentTokens, "may be nil")
		}

		commentDetails := ""
		if len(commentTokens) > 0 {
			commentDetails = fmt.Sprintf(" (%s)", strings.Join(commentTokens, ", "))
		}

		propertyNode := yaml.Node{
			Kind:  yaml.ScalarNode,
			Value: name,
		}
		propertyNode.HeadComment = fmt.Sprintf("%s%s", meta.Description, commentDetails)

		var valueNode yaml.Node
		switch x := curValue.(type) {
		case int:
			valueNode = yaml.Node{
				Kind:  yaml.ScalarNode,
				Value: strconv.Itoa(x),
			}
		case float64:
			if x == float64(int64(x)) {
				valueNode = yaml.Node{
					Kind:  yaml.ScalarNode,
					Value: strconv.FormatInt(int64(x), 10),
				}
			} else {
				valueNode = yaml.Node{
					Kind:  yaml.ScalarNode,
					Value: strconv.FormatFloat(x, 'f', -1, 64),
				}
			}
		case string:
			valueNode = yaml.Node{
				Kind:  yaml.ScalarNode,
				Value: x,
			}
		case bool:
			valueNode = yaml.Node{
				Kind:  yaml.ScalarNode,
				Value: strconv.FormatBool(x),
			}
		case []interface{}:
			valueNode = yaml.Node{
				Kind:  yaml.SequenceNode,
				Value: "",
			}
			// TODO generalize for other types of array
			for _, v := range x {
				seqNode := yaml.Node{
					Kind:  yaml.ScalarNode,
					Value: v.(string),
				}
				valueNode.Content = append(valueNode.Content, &seqNode)
			}
		case nil:
			valueNode = yaml.Node{
				Kind:  yaml.ScalarNode,
				Value: "",
			}
		default:
			out.Die("Unexpected property value type: %s: %T", name, curValue)
			continue
		}

		// Blank line between each property for readability
		propertyNode.FootComment = "\n"

		yamlNode.Content = append(yamlNode.Content, &propertyNode)
		yamlNode.Content = append(yamlNode.Content, &valueNode)
	}

	serialized, err := yaml.Marshal(&yamlNode)
	if err != nil {
		return err
	}

	_, err = file.Write(serialized)
	if err != nil {
		return err
	}

	return nil
}

func newExportCommand(fs afero.Fs, all *bool) *cobra.Command {
	var filename string

	cmd := &cobra.Command{
		Use:   "export",
		Short: "Export cluster configuration.",
		Long: `Export cluster configuration.

Writes out a YAML representation of the cluster configuration to a file,
suitable for editing and later applying with the corresponding 'import'
command.

By default, low level tunables are excluded: use the '--all' flag
to include all properties including these low level tunables.
`,
		Run: func(cmd *cobra.Command, _ []string) {
			p := config.ParamsFromCommand(cmd)
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			client, err := admin.NewClient(fs, cfg)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			// GET the schema
			schema, err := client.ClusterConfigSchema()
			out.MaybeDie(err, "unable to query config schema: %v", err)

			// GET current config
			var currentConfig admin.Config
			currentConfig, err = client.Config()
			out.MaybeDie(err, "unable to query current config: %v", err)

			// Generate a yaml template for editing
			var file *os.File
			if filename == "" {
				file, err = ioutil.TempFile("/tmp", "config_*.yaml")
			} else {
				file, err = os.Create(filename)
			}

			out.MaybeDie(err, "unable to create file %q: %v", file.Name(), err)
			err = exportConfig(file, schema, currentConfig, *all)
			out.MaybeDie(err, "failed to write out config %q: %v", file.Name(), err)
			err = file.Close()
			fmt.Printf("Wrote configuration to file %q.\n", file.Name())
			out.MaybeDie(err, "error closing file %q: %v", file.Name(), err)
		},
	}

	cmd.Flags().StringVarP(
		&filename,
		"filename",
		"f",
		"",
		"full path to file to export to, e.g. '/tmp/config.yml'",
	)

	return cmd
}
