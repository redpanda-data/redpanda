// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package export

import (
	"fmt"
	"io/ioutil"
	"os"
	"sort"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/api/admin"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/config"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/out"
	yaml "gopkg.in/yaml.v3"
)

func ExportConfig(
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

		property_node := yaml.Node{
			Kind:  yaml.ScalarNode,
			Value: name,
		}

		var valueNode yaml.Node
		switch x := curValue.(type) {
		case int:
			valueNode = yaml.Node{
				Kind:  yaml.ScalarNode,
				Value: fmt.Sprintf("%d", x),
			}
		case float64:
			if x == float64(int64(x)) {
				valueNode = yaml.Node{
					Kind:  yaml.ScalarNode,
					Value: fmt.Sprintf("%d", int64(x)),
				}
			} else {
				valueNode = yaml.Node{
					Kind:  yaml.ScalarNode,
					Value: fmt.Sprintf("%f", x),
				}
			}
		case string:
			valueNode = yaml.Node{
				Kind:  yaml.ScalarNode,
				Value: x,
			}
		case bool:
			var x_str string
			if x {
				x_str = "true"
			} else {
				x_str = "false"
			}
			valueNode = yaml.Node{
				Kind:  yaml.ScalarNode,
				Value: x_str,
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
			log.Warningf("Unexpected property value type: %s: %T", name, curValue)
			continue
		}

		// TODO: nullability in description
		// TODO: needs-restart in description

		// Preface each property with a descriptive comment
		if meta.Example != "" {
			property_node.HeadComment = fmt.Sprintf("%s (e.g. '%s')", meta.Description, meta.Example)
		} else {
			property_node.HeadComment = meta.Description
		}

		// Blank line between each property for readability
		property_node.FootComment = "\n"

		yamlNode.Content = append(yamlNode.Content, &property_node)
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

func NewCommand(fs afero.Fs, all *bool) *cobra.Command {
	var filename string

	cmd := &cobra.Command{
		Use:   "export",
		Short: "Export cluster configuration.",
		Long: `Export cluster-wide configuration properties

Cluster-wide properties apply to all nodes in a redpanda cluster.  For
node properties, use the 'rpk config' command.
`,
		Run: func(cmd *cobra.Command, args []string) {

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
			out.MaybeDie(err, "unable to create temporary file: %v", err)
			err = ExportConfig(file, schema, currentConfig, *all)
			out.MaybeDie(err, "Failed to write out config: %v", err)
			err = file.Close()
			log.Infof("Wrote configuration to file %s", file.Name())
			file = nil
			out.MaybeDie(err, "error closing temporary file: %v", err)
		},
	}

	cmd.Flags().StringVar(
		&filename,
		"filename",
		"",
		"Full path to file to import, e.g. '/tmp/config.yml'",
	)

	return cmd
}
