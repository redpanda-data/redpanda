// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package shadow

import (
	"fmt"
	"time"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	rpkos "github.com/redpanda-data/redpanda/src/go/rpk/pkg/os"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v2"
)

func newShadowConfigCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config",
		Args:  cobra.NoArgs,
		Short: "Generate a Redpanda Shadow Link configuration file",
	}
	cmd.AddCommand(
		newGenerateCommand(fs, p),
	)
	return cmd
}

func newGenerateCommand(fs afero.Fs, _ *config.Params) *cobra.Command {
	var output string
	cmd := &cobra.Command{
		Use:   "generate",
		Args:  cobra.NoArgs,
		Short: "Generate a Redpanda Shadow Link configuration file",
		Long: `Generate a Redpanda Shadow Link configuration file

By default, the command prints a sample configuration file to standard output.
Use the --output flag to specify a file path to save the configuration file.

It generates a configuration file with placeholder values that you need to fill
in with actual connection details and settings for your Shadow Link. You may
also create a configuration file from an existing rpk profile or from a Redpanda
configuration file.
`,
		Run: func(_ *cobra.Command, _ []string) {
			// TODO: support generating from an rpk profile or Redpanda config file.
			sampleConfig := generateSampleConfig()

			yamlData, err := yaml.Marshal(sampleConfig)
			out.MaybeDie(err, "unable to marshal configuration to YAML: %v", err)

			if output != "" {
				// TODO: check if file exists and prompt for confirmation to overwrite.
				err = rpkos.ReplaceFile(fs, output, yamlData, 0o644)
				out.MaybeDie(err, "unable to write configuration file to %q: %v", output, err)

				fmt.Printf("Configuration file generated successfully: %s\n", output)
			} else {
				fmt.Println(string(yamlData))
			}
		},
	}
	cmd.Flags().StringVarP(&output, "output", "o", "", "File path to save the generated configuration file. If not specified, prints to standard output")
	return cmd
}

func generateSampleConfig() *ShadowLinkConfig {
	return &ShadowLinkConfig{
		Name: "sample-shadow-link",
		ClientOptions: &ShadowLinkClientOptions{
			BootstrapServers: []string{"localhost:9092", "localhost:19092"},
			SourceClusterID:  "optional-source-cluster-id",
			TLSSettings: &TLSFileSettings{
				Enabled:  true,
				CAPath:   "/path/to/ca.crt",
				KeyPath:  "/path/to/optional/client.key",
				CertPath: "/path/to/optional/client.crt",
			},
			AuthenticationConfiguration: &ScramConfig{
				Username:       "username",
				Password:       "password",
				ScramMechanism: ScramMechanismScramSha256,
			},
			MetadataMaxAgeMs:    10000,
			ConnectionTimeoutMs: 1000,
			RetryBackoffMs:      100,
			FetchWaitMaxMs:      100,
			FetchMinBytes:       1,
			FetchMaxBytes:       1048576,
		},
		TopicMetadataSyncOptions: &TopicMetadataSyncOptions{
			ExcludeDefault: true,
			Interval:       30 * time.Second,
			AutoCreateShadowTopicFilters: []*NameFilter{
				{
					PatternType: PatternTypeLiteral,
					FilterType:  FilterTypeInclude,
					Name:        "*",
				},
				{
					PatternType: PatternTypePrefix,
					FilterType:  FilterTypeExclude,
					Name:        "foo-",
				},
			},
			SyncedShadowTopicProperties: []string{"retention.ms", "segment.ms"},
		},
		ConsumerOffsetSyncOptions: &ConsumerOffsetSyncOptions{
			Interval: 30 * time.Second,
			Enabled:  true,
			GroupFilters: []*NameFilter{
				{
					PatternType: PatternTypeLiteral,
					FilterType:  FilterTypeInclude,
					Name:        "*",
				},
			},
		},
		SecuritySyncOptions: &SecuritySettingsSyncOptions{
			Interval: 30 * time.Second,
			Enabled:  false,
			ACLFilters: []*ACLFilter{
				{
					ResourceFilter: &ACLResourceFilter{
						ResourceType: ACLResourceTopic,
						PatternType:  ACLPatternPrefixed,
						Name:         "test-",
					},
					AccessFilter: &ACLAccessFilter{
						Principal:      "User:admin",
						Operation:      ACLOperationAny,
						PermissionType: ACLPermissionTypeAllow,
						Host:           "*",
					},
				},
			},
		},
	}
}
