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
	"os"
	"reflect"
	"strings"

	controlplanev1 "buf.build/gen/go/redpandadata/cloud/protocolbuffers/go/redpanda/api/controlplane/v1"
	adminv2 "buf.build/gen/go/redpandadata/core/protocolbuffers/go/redpanda/core/admin/v2"
	"connectrpc.com/connect"
	"github.com/redpanda-data/common-go/rpadmin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/oauth/providers/auth0"
	rpkos "github.com/redpanda-data/redpanda/src/go/rpk/pkg/osutil"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/publicapi"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// redacted is the placeholder shown in place of a set password; the server
// only reports whether a password is set, never its value.
const redacted = "<redacted>"

// cloudUpdateReplacePaths is the update mask covering every updatable field of
// the cloud ShadowLinkUpdate message. The cloud API requires a mask, so
// file-based updates use this to replace the entire configuration.
var cloudUpdateReplacePaths = []string{
	"client_options",
	"topic_metadata_sync_options",
	"consumer_offset_sync_options",
	"security_sync_options",
	"schema_registry_sync_options",
}

func newUpdateCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var cfgLocation string
	cmd := &cobra.Command{
		Use:   "update [LINK_NAME]",
		Short: "Update a Shadow Link",
		Long: `Updates a Shadow Link.

By default, this command opens your default editor with the current Shadow
Link configuration. Update the fields you want to change, save the file, and
close the editor. The command applies only the changed fields to the Shadow
Link.

Alternatively, use the '--config-file' flag to apply a configuration file
directly without opening an editor, which is useful for scripted workflows. In
this mode the file replaces the entire Shadow Link configuration: any field
omitted from the file is reset to its default value. The name in the
configuration file must match LINK_NAME.

You cannot change the Shadow Link name. If you need to rename a Shadow Link,
delete it and create a new one with the desired name.

The editor respects your EDITOR environment variable. If EDITOR is not set, the
command uses 'vi' on Unix-like systems.
`,
		Example: `
Update a Shadow Link in your editor:
  rpk shadow update my-shadow-link

Replace the entire Shadow Link configuration from a file:
  rpk shadow update my-shadow-link --config-file shadow-link.yaml
`,
		Args: cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load rpk config: %v", err)
			prof := cfg.VirtualProfile()
			config.CheckExitServerlessAdmin(prof)

			// The editor flow (default) diffs the current config and applies
			// only the changed fields via a mask; --config-file replaces the
			// entire configuration.
			fromCloud := prof.CheckFromCloud()
			fileMode := cfgLocation != ""
			linkName := args[0]
			var (
				originalCfg *ShadowLinkConfig
				updatedCfg  *ShadowLinkConfig
				diff        []string
				adminClient *rpadmin.AdminAPI
				cloudClient *publicapi.CloudClientSet
				cloudLinkID string
			)

			if fileMode {
				updatedCfg, err = updatedConfigFromFile(fs, cfgLocation, linkName, fromCloud, prof.CloudCluster.ClusterID)
				out.MaybeDieErr(err)
			}

			// Cloud always looks up the link to resolve its ID; editor mode
			// also seeds the config from the current one.
			if fromCloud {
				cloudClient, err = publicapi.NewValidatedCloudClientSet(
					cfg.DevOverrides().PublicAPIURL,
					prof.CurrentAuth().AuthToken,
					auth0.NewClient(cfg.DevOverrides()).Audience(),
					[]string{prof.CurrentAuth().ClientID},
				)
				out.MaybeDieErr(err)

				link, err := cloudClient.ShadowLinkByNameAndRPID(cmd.Context(), linkName, prof.CloudCluster.ClusterID)
				out.MaybeDie(err, "unable to find Shadow Link %q", linkName)

				cloudLinkID = link.GetId()
				if !fileMode {
					// Cloud uses secrets for passwords, no redaction needed.
					originalCfg = cloudShadowLinkToConfig(link)
				}
			} else {
				adminClient, err = adminapi.NewClient(cmd.Context(), fs, prof)
				out.MaybeDie(err, "unable to initialize admin client: %v", err)

				if !fileMode {
					link, err := adminClient.ShadowLinkService().GetShadowLink(cmd.Context(), connect.NewRequest(&adminv2.GetShadowLinkRequest{
						Name: linkName,
					}))
					out.MaybeDie(err, "unable to get Redpanda Shadow Link information: %v", handleConnectError(err, "get", linkName))

					shadowLink := link.Msg.GetShadowLink()
					originalCfg = shadowLinkToConfig(shadowLink)

					addRedactedPasswordString(originalCfg, shadowLink)
				}
			}

			// Editor mode: open the editor and calculate the diff.
			if !fileMode {
				updatedCfg, err = rpkos.EditTmpYAMLFile(cmd.Context(), fs, originalCfg)
				out.MaybeDie(err, "unable to edit Shadow Link configuration: %v", err)

				err = validateParsedShadowLinkConfig(updatedCfg)
				out.MaybeDie(err, "invalid Shadow Link configuration: %v", err)

				if updatedCfg.Name != originalCfg.Name {
					out.Die("shadow link name cannot be changed; if you need to rename, please delete and recreate the shadow link")
				}

				diff = diffConfigs(originalCfg, updatedCfg)
				if diff == nil {
					out.Exit("No changes detected")
				}
			}

			// Submit the update request.
			if fromCloud {
				updatedSL := shadowLinkConfigToCloudUpdate(updatedCfg, cloudLinkID)

				var cloudPaths []string
				if fileMode {
					err = validateCloudSecrets(cmd.Context(), prof, updatedCfg)
					out.MaybeDie(err, "unable to validate cloud secrets: %v", err)

					cloudPaths = cloudUpdateReplacePaths
				} else {
					// Cloud proto has no "configurations" wrapper; strip it.
					cloudPaths = make([]string, len(diff))
					for i, path := range diff {
						cloudPaths[i] = strings.TrimPrefix(path, "configurations.")
					}
				}

				fm, err := fieldmaskpb.New(updatedSL, cloudPaths...)
				out.MaybeDie(err, "unrecognized changed fields: %v; please report this with Redpanda Support", err)

				zap.L().Sugar().Debugf("Requesting configuration update for: %v", strings.Join(cloudPaths, ", "))
				op, err := cloudClient.ShadowLink.UpdateShadowLink(cmd.Context(), connect.NewRequest(&controlplanev1.UpdateShadowLinkRequest{
					ShadowLink: updatedSL,
					UpdateMask: fm,
				}))
				out.MaybeDie(err, "unable to update Shadow Link: %v", handleConnectError(err, "update", linkName))
				spinner := out.NewSpinner(cmd.Context(), "Updating Shadow Link...")
				isComplete, err := waitForOperation(cmd.Context(), cloudClient, op.Msg.GetOperation().GetId())
				if err != nil {
					spinner.Fail(fmt.Sprintf("unable to confirm Shadow Link update: %v", err))
					os.Exit(1)
				}
				if !isComplete {
					spinner.Stop()
					out.Exit("Shadow link update is taking longer than expected. Please check the status of the shadow link using 'rpk shadow status %q'", linkName)
				}
				spinner.Success(fmt.Sprintf("Successfully updated shadow link %q", linkName))
				os.Exit(0)
			}
			// Self-hosted path. In file mode the mask stays unset, which the
			// server treats as a full replace.
			updatedSL := shadowLinkConfigToProto(updatedCfg)
			var fm *fieldmaskpb.FieldMask
			if fileMode {
				zap.L().Sugar().Debug("Requesting full configuration replacement")
			} else {
				fm, err = fieldmaskpb.New(updatedSL, diff...)
				out.MaybeDie(err, "unrecognized changed fields: %v; please report this with Redpanda Support", err)

				zap.L().Sugar().Debugf("Requesting configuration update for: %v", strings.Join(diff, ", "))
			}
			_, err = adminClient.ShadowLinkService().UpdateShadowLink(cmd.Context(), connect.NewRequest(&adminv2.UpdateShadowLinkRequest{
				ShadowLink: updatedSL,
				UpdateMask: fm,
			}))
			out.MaybeDie(err, "unable to update Shadow Link: %v", handleConnectError(err, "update", linkName))
			fmt.Printf("Successfully updated shadow link %q.\n", linkName)
		},
	}
	cmd.Flags().StringVarP(&cfgLocation, "config-file", "c", "", "Path to a configuration file that replaces the entire Shadow Link configuration; use --help for details")
	return cmd
}

// updatedConfigFromFile parses and validates a Shadow Link configuration file
// for use in update; the config's name must match linkName, and on a cloud
// profile a cloud_options.shadow_redpanda_id, if present, must match the
// selected cluster.
func updatedConfigFromFile(fs afero.Fs, path, linkName string, fromCloud bool, clusterID string) (*ShadowLinkConfig, error) {
	slCfg, err := parseShadowLinkConfig(fs, path)
	if err != nil {
		return nil, err
	}
	// A placeholder copied from the editor flow would be stored verbatim.
	if authPassword(slCfg.ClientOptions) == redacted || srAPIAuthPassword(slCfg.SchemaRegistrySyncOptions) == redacted {
		return nil, fmt.Errorf("the configuration file contains the placeholder password %q; set the real password, or leave it empty to keep the existing one", redacted)
	}
	if err := validateParsedShadowLinkConfig(slCfg); err != nil {
		return nil, fmt.Errorf("invalid Shadow Link configuration: %w", err)
	}
	if slCfg.Name != linkName {
		return nil, fmt.Errorf("shadow link name %q in the configuration file does not match %q; the link name cannot be changed", slCfg.Name, linkName)
	}
	if co := slCfg.CloudOptions; fromCloud && co != nil && co.ShadowRedpandaID != clusterID {
		return nil, fmt.Errorf("shadow_redpanda_id %q in the configuration file does not match the selected cluster %q", co.ShadowRedpandaID, clusterID)
	}
	return slCfg, nil
}

// if a password is set, replace it with a redacted value so user can provide
// a change easily instead of writing the full password field. The server only
// reports whether a password is set, never its value.
func addRedactedPasswordString(cfg *ShadowLinkConfig, link *adminv2.ShadowLink) {
	cfgs := link.GetConfigurations()
	if cfgs.GetClientOptions().GetAuthenticationConfiguration().GetScramConfiguration().GetPasswordSet() {
		if co := cfg.ClientOptions; co != nil {
			if auth := co.AuthenticationConfiguration; auth != nil && auth.ScramConfiguration != nil {
				auth.ScramConfiguration.Password = redacted
			}
		}
	}

	if cfgs.GetSchemaRegistrySyncOptions().GetShadowSchemaRegistryApi().GetAuthOptions().GetBasic().GetPasswordSet() {
		if sr := cfg.SchemaRegistrySyncOptions; sr != nil && sr.ShadowSchemaRegistryAPI != nil {
			if auth := sr.ShadowSchemaRegistryAPI.AuthOptions; auth != nil && auth.Basic != nil {
				auth.Basic.Password = redacted
			}
		}
	}
}

// diffConfigs compares two ShadowLinkConfig objects and returns a list of
// fields that changed. It uses Reflect to compare the fields and returns the
// full JSON path of the changed fields, starting with "configurations".
//
// For example, if the 'bootstrap_servers' field changed, it returns
// "configurations.client_options.bootstrap_servers".
func diffConfigs(original, updated *ShadowLinkConfig) []string {
	if reflect.DeepEqual(original, updated) {
		return nil // deeply equal, no changes.
	}
	if original == nil || updated == nil {
		// One is nil, other is not: everything changed.
		return []string{"configurations"}
	}

	var changedPaths []string
	compareValues(reflect.ValueOf(original), reflect.ValueOf(updated), "configurations", &changedPaths)
	return changedPaths
}

// getJSONTag extracts the JSON tag name from a struct field.
// Returns empty string if the field should be skipped.
func getJSONTag(field reflect.StructField) string {
	jsonTag := field.Tag.Get("json")
	if jsonTag == "" || jsonTag == "-" {
		return ""
	}
	// Extract the name before any options ("name,omitempty" -> "name")
	parts := strings.Split(jsonTag, ",")
	return parts[0]
}

// compareValues recursively compares two reflect.Value objects and records
// changed paths.
func compareValues(original, updated reflect.Value, path string, changedPaths *[]string) {
	if !original.IsValid() && !updated.IsValid() {
		return
	}
	// One invalid, other valid: record as changed
	if !original.IsValid() || !updated.IsValid() {
		*changedPaths = append(*changedPaths, path)
		return
	}

	// Dereference pointers
	if original.Kind() == reflect.Pointer {
		if original.IsNil() && updated.IsNil() {
			return
		}
		if original.IsNil() || updated.IsNil() {
			*changedPaths = append(*changedPaths, path)
			return
		}
		compareValues(original.Elem(), updated.Elem(), path, changedPaths)
		return
	}

	switch original.Kind() {
	case reflect.Struct:
		compareStructs(original, updated, path, changedPaths)
	default:
		// For other types, just use DeepEqual.
		if !reflect.DeepEqual(original.Interface(), updated.Interface()) {
			*changedPaths = append(*changedPaths, path)
		}
	}
}

// compareStructs compares two struct values field by field.
func compareStructs(original, updated reflect.Value, path string, changedPaths *[]string) {
	typ := original.Type()

	// Handle empty structs (markers like StartAtEarliest)
	if typ.NumField() == 0 {
		// Empty struct - just check existence (already handled by pointer nil check)
		return
	}

	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)

		if !field.IsExported() {
			continue
		}

		jsonTag := getJSONTag(field)
		if jsonTag == "" {
			continue
		}
		fieldPath := path + "." + jsonTag

		compareValues(original.Field(i), updated.Field(i), fieldPath, changedPaths)
	}
}
