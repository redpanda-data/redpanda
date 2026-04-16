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
	"slices"
	"strings"
	"time"

	v2 "buf.build/gen/go/redpandadata/core/protocolbuffers/go/redpanda/core/admin/v2"
	adminv2comments "github.com/redpanda-data/redpanda/src/go/rpk/gen/protocomments/admin/v2"
	commonv1comments "github.com/redpanda-data/redpanda/src/go/rpk/gen/protocomments/common/v1"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	rpkos "github.com/redpanda-data/redpanda/src/go/rpk/pkg/osutil"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"google.golang.org/genproto/googleapis/api/annotations"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
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
	var (
		outputPath    string
		printTemplate bool
		cloud         bool
	)
	cmd := &cobra.Command{
		Use:   "generate",
		Args:  cobra.NoArgs,
		Short: "Generate a Redpanda Shadow Link configuration file",
		Long: `Generate a configuration file for creating a Shadow Link.

By default, this command creates a sample configuration file with placeholder
values that you can customize for your environment. If you are generating a 
Shadow Link for Redpanda Cloud, use the --for-cloud flag.

Use the --print-template flag to generate a configuration template with detailed
field documentations.

By default, this command prints the configuration to standard output. Use the
--output flag to save the configuration to a file.

After you generate the configuration file, update the placeholder values with
your actual connection details and settings. Then use 'rpk shadow create' to
create the Shadow Link.
`,
		Example: `
Generate a sample configuration and print it to standard output:
  rpk shadow config generate

Generate a configuration template with all the field documentation:
  rpk shadow config generate --print-template

Save the sample configuration to a file:
  rpk shadow config generate -o shadow-link.yaml

Save the template with documentation to a file:
  rpk shadow config generate --print-template -o shadow-link.yaml
`,
		Run: func(_ *cobra.Command, _ []string) {
			var outputData, successMsg string
			if printTemplate {
				template := generateConfigTemplate(cloud)
				outputData = template
				successMsg = "Template file generated successfully: %s\n"
			} else {
				sampleConfig := generateSampleConfig(cloud)
				yamlData, err := yaml.Marshal(sampleConfig)
				out.MaybeDie(err, "unable to marshal configuration to YAML: %v", err)
				outputData = string(yamlData)
				successMsg = "Configuration file generated successfully: %s\n"
			}

			if outputPath != "" {
				// We ignore the error on purpose, as rpkos.ReplaceFile will
				// handle it accordingly.
				if exists, _ := afero.Exists(fs, outputPath); exists {
					confirm, err := out.Confirm("File %q already exists. Overwrite?", outputPath)
					out.MaybeDie(err, "confirmation error: %v", err)
					if !confirm {
						out.Exit("Operation cancelled")
					}
				}
				err := rpkos.ReplaceFile(fs, outputPath, []byte(outputData), 0o644)
				out.MaybeDie(err, "unable to write configuration file to %q: %v", outputPath, err)
				fmt.Printf(successMsg, outputPath)
			} else {
				fmt.Println(outputData)
			}
		},
	}
	cmd.Flags().StringVarP(&outputPath, "output", "o", "", "File path to save the generated configuration file. If not specified, prints to standard output")
	cmd.Flags().BoolVar(&cloud, "for-cloud", false, "Generate configuration suitable for Redpanda Cloud")
	cmd.Flags().BoolVar(&printTemplate, "print-template", false, "Generate a configuration template with field documentation instead of a sample configuration")
	return cmd
}

func generateSampleConfig(cloud bool) *ShadowLinkConfig {
	slCfg := &ShadowLinkConfig{
		Name: "sample-shadow-link",
		ClientOptions: &ShadowLinkClientOptions{
			BootstrapServers: []string{"localhost:9092", "localhost:19092"},
			SourceClusterID:  "optional-source-cluster-id",
			TLSSettings: &TLSSettings{
				Enabled: true,
				TLSFileSettings: &TLSFileSettings{
					CAPath:   "/path/to/ca.crt",
					KeyPath:  "/path/to/optional/client.key",
					CertPath: "/path/to/optional/client.crt",
				},
			},
			AuthenticationConfiguration: &AuthenticationConfiguration{
				ScramConfiguration: &ScramConfiguration{
					Username:       "username",
					Password:       "password",
					ScramMechanism: ScramMechanismScramSha256,
				},
			},
			MetadataMaxAgeMs:       10000,
			ConnectionTimeoutMs:    1000,
			RetryBackoffMs:         100,
			FetchWaitMaxMs:         100,
			FetchMinBytes:          100,
			FetchMaxBytes:          1048576,
			FetchPartitionMaxBytes: 1048576,
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
			StartAtEarliest:             &StartAtEarliest{},
		},
		ConsumerOffsetSyncOptions: &ConsumerOffsetSyncOptions{
			Interval: 30 * time.Second,
			Paused:   false,
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
			Paused:   false,
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
		SchemaRegistrySyncOptions: &SchemaRegistrySyncOptions{
			ShadowSchemaRegistryTopic: &ShadowSchemaRegistryTopic{},
		},
	}
	if cloud {
		slCfg.CloudOptions = &CloudShadowLinkOptions{
			SourceRedpandaID: "m7xtv2qq5njbhwruk88f",
			ShadowRedpandaID: "p9skc1dd3fmzgvquj66h",
		}

		slCfg.ClientOptions.BootstrapServers = nil
		// This is confusing on Cloud, user is already providing the Redpanda ID.
		slCfg.ClientOptions.SourceClusterID = ""
		// Cloud only accepts passwords from the secret store.
		slCfg.ClientOptions.AuthenticationConfiguration.ScramConfiguration.Password = "${secrets.PASSWORD_FROM_SHADOW_CLUSTER_SECRET_STORE}"

		// Replace TLS settings as file settings are not valid in Cloud.
		slCfg.ClientOptions.TLSSettings = &TLSSettings{
			Enabled: true,
			TLSPEMSettings: &TLSPEMSettings{
				CA:   "-----BEGIN CERTIFICATE-----\n...\n-----END CERTIFICATE-----",
				Key:  "${secrets.KEY_FROM_SHADOW_CLUSTER_SECRET_STORE}",
				Cert: "-----BEGIN CERTIFICATE-----\n...\n-----END CERTIFICATE-----",
			},
		}
	}
	return slCfg
}

func generateConfigTemplate(cloud bool) string {
	var sb strings.Builder

	if cloud {
		sb.WriteString("# Shadow Link Configuration Template for Redpanda Cloud\n")
	} else {
		sb.WriteString("# Shadow Link Configuration Template\n")
	}

	// Manually add name field (not in ShadowLinkConfigurations proto, but in ShadowLink)
	sb.WriteString("# The name of the shadow link\n")
	sb.WriteString("name: \"\"\n")

	// Inject cloud_options manually (not in admin/v2 proto)
	if cloud {
		sb.WriteString("# Configurations for Shadow Link in Redpanda Cloud\n")
		sb.WriteString("cloud_options:\n")
		sb.WriteString("  # The ID of the source Redpanda Cloud cluster (optional)\n")
		sb.WriteString("  source_redpanda_id: \"\"\n")
		sb.WriteString("  # The ID of the shadow Redpanda Cloud cluster\n")
		sb.WriteString("  shadow_redpanda_id: \"\"\n\n")
	}

	// Get the message descriptor from the global registry
	cfg := &v2.ShadowLinkConfigurations{}
	msg := cfg.ProtoReflect()
	if msg == nil {
		return "# Error: Could not find message descriptor"
	}
	msgDesc := msg.Descriptor()

	if msgDesc == nil {
		return "# Error: Message descriptor is nil\n"
	}

	// Walk through all fields and generate YAML with comments
	fields := msgDesc.Fields()
	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)

		// Skip OUTPUT_ONLY fields
		if isFieldOutputOnly(field) {
			continue
		}

		writeFieldTemplate(&sb, field, 0, cloud)
	}

	return sb.String()
}

func isFieldOutputOnly(field protoreflect.FieldDescriptor) bool {
	f, ok := field.Options().(*descriptorpb.FieldOptions)
	if !ok {
		return false
	}
	e := proto.GetExtension(f, annotations.E_FieldBehavior)
	fb, ok := e.([]annotations.FieldBehavior)
	if !ok {
		return false
	}
	return slices.Contains(fb, annotations.FieldBehavior_OUTPUT_ONLY)
}

// getCommentForField dispatches to the appropriate package's comment registry.
func getCommentForField(field protoreflect.FieldDescriptor) string {
	// Determine which package based on the proto file path
	protoFile := field.ParentFile().Path()

	if strings.Contains(protoFile, "common/v1") {
		return commonv1comments.GetCommentForField(field)
	}
	// Default to admin/v2
	return adminv2comments.GetCommentForField(field)
}

// stripEnumPrefix removes common proto enum prefixes to match our config
// enum expectations.
//
// The prefix is derived by converting the enum type name (last component of the
// fully qualified name) from PascalCase to SCREAMING_SNAKE_CASE with a trailing
// underscore.
// For example:
//   - "redpanda.core.common.v1.ACLResource" → "ACL_RESOURCE_"
//   - "redpanda.core.admin.v2.PatternType" → "PATTERN_TYPE_"
func stripEnumPrefix(enumValue protoreflect.EnumValueDescriptor) string {
	enumName := string(enumValue.Name())
	enumType := string(enumValue.Parent().FullName())

	// Extract the type name (last component after the last dot)
	lastDot := strings.LastIndex(enumType, ".")
	if lastDot == -1 {
		return enumName // No package prefix, return as-is
	}
	typeName := enumType[lastDot+1:]

	prefix := toScreamingSnakeCase(typeName) + "_"

	return strings.TrimPrefix(enumName, prefix)
}

// toScreamingSnakeCase converts a PascalCase string to SCREAMING_SNAKE_CASE.
func toScreamingSnakeCase(s string) string {
	var result strings.Builder
	for i, r := range s {
		if i > 0 && r >= 'A' && r <= 'Z' {
			// Add underscore before uppercase letter if:
			// - Previous character was lowercase, OR
			// - Previous character was uppercase AND next character is lowercase (to handle "ACLResource" correctly)
			prev := rune(s[i-1])
			if prev >= 'a' && prev <= 'z' {
				result.WriteRune('_')
			} else if i+1 < len(s) {
				next := rune(s[i+1])
				if next >= 'a' && next <= 'z' {
					result.WriteRune('_')
				}
			}
		}
		result.WriteRune(r)
	}
	return strings.ToUpper(result.String())
}

func writeFieldTemplate(sb *strings.Builder, field protoreflect.FieldDescriptor, indent int, cloud bool) {
	// Skip tls_file_settings for Cloud (only tls_pem_settings is valid)
	if cloud && string(field.Name()) == "tls_file_settings" {
		return
	}

	indentStr := strings.Repeat("  ", indent)

	// Get field comment using the appropriate package registry
	comment := getCommentForField(field)
	if comment != "" {
		// Write comment lines with proper indentation
		lines := strings.SplitSeq(comment, "\n")
		for line := range lines {
			line = strings.TrimSpace(line)
			if line != "" {
				fmt.Fprintf(sb, "%s# %s\n", indentStr, line)
			}
		}
	}

	// Get the field name (use proto name directly)
	fieldName := string(field.Name())

	// Write the field name
	fmt.Fprintf(sb, "%s%s:", indentStr, fieldName)

	// Handle different field types
	if field.IsMap() {
		sb.WriteString(" {}\n")
	} else if field.IsList() {
		// It's a repeated field
		if field.Message() != nil {
			// List of messages
			sb.WriteString("\n")

			// Write nested message fields with increased indent
			nestedMsg := field.Message()
			nestedFields := nestedMsg.Fields()
			for i := 0; i < nestedFields.Len(); i++ {
				nestedField := nestedFields.Get(i)

				// Skip OUTPUT_ONLY fields in nested messages too
				if isFieldOutputOnly(nestedField) {
					continue
				}

				writeFieldTemplate(sb, nestedField, indent+2, cloud)
			}
		} else {
			// List of scalars
			sb.WriteString(" []\n")
		}
	} else if field.Message() != nil {
		// It's a nested message
		nestedMsg := field.Message()

		// Check if it's a well-known type
		fullName := string(nestedMsg.FullName())
		if strings.HasPrefix(fullName, "google.protobuf.") {
			// Handle well-known types
			switch nestedMsg.Name() {
			case "Duration":
				sb.WriteString(" 30s  # duration (e.g., 30s, 1m, 1h)\n")
			case "Timestamp":
				sb.WriteString(" \"2024-01-01T00:00:00Z\"  # RFC3339 timestamp\n")
			default:
				sb.WriteString(" {}\n")
			}
		} else {
			// Regular nested message
			sb.WriteString("\n")

			// Recursively write nested message fields
			nestedFields := nestedMsg.Fields()
			for i := 0; i < nestedFields.Len(); i++ {
				nestedField := nestedFields.Get(i)

				// Skip OUTPUT_ONLY fields
				if isFieldOutputOnly(nestedField) {
					continue
				}

				writeFieldTemplate(sb, nestedField, indent+1, cloud)
			}
		}
	} else {
		// Scalar field - provide a placeholder
		switch field.Kind() {
		case protoreflect.BoolKind:
			sb.WriteString(" false\n")
		case protoreflect.StringKind:
			sb.WriteString(" \"\"\n")
		case protoreflect.Int32Kind, protoreflect.Int64Kind, protoreflect.Uint32Kind, protoreflect.Uint64Kind,
			protoreflect.Sint32Kind, protoreflect.Sint64Kind, protoreflect.Fixed32Kind, protoreflect.Fixed64Kind,
			protoreflect.Sfixed32Kind, protoreflect.Sfixed64Kind:
			sb.WriteString(" 0\n")
		case protoreflect.FloatKind, protoreflect.DoubleKind:
			sb.WriteString(" 0.0\n")
		case protoreflect.EnumKind:
			// Get the enum descriptor and use the first non-zero value if possible
			enumDesc := field.Enum()
			if enumDesc.Values().Len() > 0 {
				// Try to find a non-unspecified value
				var enumValue protoreflect.EnumValueDescriptor
				for i := 0; i < enumDesc.Values().Len(); i++ {
					val := enumDesc.Values().Get(i)
					if val.Number() != 0 {
						enumValue = val
						break
					}
				}
				// If all values are zero, use the first one
				if enumValue == nil {
					enumValue = enumDesc.Values().Get(0)
				}

				// Get comment for the enum value if available
				protoFile := field.ParentFile().Path()
				var enumComment string
				if strings.Contains(protoFile, "common/v1") {
					enumComment = commonv1comments.GetCommentForEnumValue(enumValue)
				} else {
					enumComment = adminv2comments.GetCommentForEnumValue(enumValue)
				}
				// Strip enum prefix to match rpk command expectations
				strippedName := stripEnumPrefix(enumValue)
				if enumComment != "" {
					fmt.Fprintf(sb, " %s  # %s\n", strippedName, enumComment)
				} else {
					fmt.Fprintf(sb, " %s\n", strippedName)
				}
			} else {
				sb.WriteString(" 0\n")
			}
		case protoreflect.BytesKind:
			sb.WriteString(" \"\"\n")
		default:
			sb.WriteString(" null\n")
		}
	}

	// Add a blank line after top-level fields for readability
	if indent == 0 {
		sb.WriteString("\n")
	}
}
