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
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"

	controlplanev1 "buf.build/gen/go/redpandadata/cloud/protocolbuffers/go/redpanda/api/controlplane/v1"
	adminv2 "buf.build/gen/go/redpandadata/core/protocolbuffers/go/redpanda/core/admin/v2"
	"connectrpc.com/connect"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	rpkos "github.com/redpanda-data/redpanda/src/go/rpk/pkg/osutil"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// redacted is the placeholder for a set password; the server never reports
// the value, only whether one is set.
const redacted = "<redacted>"

// cloudUpdateReplacePaths lists every updatable ShadowLinkUpdate field; the
// cloud API requires a mask, so updates always replace the entire config.
var cloudUpdateReplacePaths = []string{
	"client_options",
	"topic_metadata_sync_options",
	"consumer_offset_sync_options",
	"security_sync_options",
	"schema_registry_sync_options",
	"role_sync_options",
}

func newUpdateCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var cfgLocation string
	cmd := &cobra.Command{
		Use:   "update [LINK_NAME]",
		Short: "Update a Shadow Link",
		Long: `Updates a Shadow Link.

By default, this command opens your default editor with the current Shadow
Link configuration. Update the fields you want to change, save the file, and
close the editor.

Alternatively, use the '--config-file' flag to apply a configuration file
directly without opening an editor, which is useful for scripted workflows.

In both modes the submitted configuration replaces the entire Shadow Link
configuration: any field omitted is reset to its default value. The modes
differ only in where the configuration comes from; the editor is seeded with
the current configuration, while a configuration file is applied as-is. The
name in the configuration file must match LINK_NAME.

Set passwords appear in the editor as the placeholder '<redacted>'; leave the
placeholder untouched to keep the existing password, or replace it to set a
new one.

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

			fromCloud := prof.CheckFromCloud()
			linkName := args[0]

			// Both modes replace the entire configuration; they differ only
			// in where it comes from (--config-file or a pre-seeded editor).
			updatedCfg, changed, err := resolveUpdatedConfig(cmd.Context(), fs, cfg, prof, fromCloud, cfgLocation, linkName)
			out.MaybeDieErr(err)
			if !changed {
				out.Exit("No changes detected")
			}

			if fromCloud {
				cloudClient, err := newCloudClientSet(cfg, prof)
				out.MaybeDieErr(err)

				// Cloud updates address the link by ID, resolved from its name.
				link, err := cloudClient.ShadowLinkByNameAndRPID(cmd.Context(), linkName, prof.CloudCluster.ClusterID)
				out.MaybeDie(err, "unable to find Shadow Link %q", linkName)

				err = validateCloudSecrets(cmd.Context(), prof, updatedCfg)
				out.MaybeDie(err, "unable to validate cloud secrets: %v", err)

				op, err := cloudClient.ShadowLink.UpdateShadowLink(cmd.Context(), connect.NewRequest(&controlplanev1.UpdateShadowLinkRequest{
					ShadowLink: shadowLinkConfigToCloudUpdate(updatedCfg, link.GetId()),
					UpdateMask: &fieldmaskpb.FieldMask{Paths: cloudUpdateReplacePaths}, // The cloud API rejects an empty mask.
				}))
				out.MaybeDie(err, "unable to update Shadow Link: %v", handleConnectError(err, "update", linkName))

				spinner := out.NewSpinner(cmd.Context(), "Updating Shadow Link...", out.WithElapsedTime())
				isComplete, err := waitForOperation(cmd.Context(), cloudClient, op.Msg.GetOperation().GetId())
				if err != nil {
					if oErr := new(OperationFailedError); errors.As(err, &oErr) {
						spinner.Fail(tryShadowLinkErrReason(cmd.Context(), cloudClient.ShadowLink, oErr))
						os.Exit(1)
					}
					spinner.Fail(fmt.Sprintf("unable to confirm Shadow Link update: %v", err))
					os.Exit(1)
				}
				if !isComplete {
					spinner.Stop()
					out.Exit("Shadow link update is taking longer than expected. Please check the status of the shadow link using 'rpk shadow status %v'", linkName)
				}
				spinner.Success(fmt.Sprintf("Successfully updated shadow link %q", linkName))
				return
			}

			adminClient, err := adminapi.NewClient(cmd.Context(), fs, prof)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			zap.L().Sugar().Debug("Requesting full configuration replacement")
			_, err = adminClient.ShadowLinkService().UpdateShadowLink(cmd.Context(), connect.NewRequest(&adminv2.UpdateShadowLinkRequest{
				ShadowLink: shadowLinkConfigToProto(updatedCfg),
			}))
			out.MaybeDie(err, "unable to update Shadow Link: %v", handleConnectError(err, "update", linkName))
			fmt.Printf("Successfully updated shadow link %q.\n", linkName)
		},
	}
	cmd.Flags().StringVarP(&cfgLocation, "config-file", "c", "", "Path to a configuration file that replaces the entire Shadow Link configuration; use --help for details")
	return cmd
}

// resolveUpdatedConfig returns the replacement config, parsed from cfgPath
// or from an editor; changed is false when the editor left it untouched.
func resolveUpdatedConfig(ctx context.Context, fs afero.Fs, cfg *config.Config, prof *config.RpkProfile, fromCloud bool, cfgPath, linkName string) (updated *ShadowLinkConfig, changed bool, err error) {
	if cfgPath != "" {
		updated, err := updatedConfigFromFile(fs, cfgPath, linkName, fromCloud, prof.CloudCluster.ClusterID)
		if err != nil {
			return nil, false, err
		}
		return updated, true, nil
	}
	original, err := currentShadowLinkConfig(ctx, fs, cfg, prof, fromCloud, linkName)
	if err != nil {
		return nil, false, err
	}
	updated, diff, err := editShadowLinkConfig(ctx, fs, original)
	if err != nil {
		return nil, false, err
	}
	return updated, diff != nil, nil
}

// currentShadowLinkConfig fetches the link's current configuration to seed
// the editor; self-hosted set passwords are replaced with the placeholder.
func currentShadowLinkConfig(ctx context.Context, fs afero.Fs, cfg *config.Config, prof *config.RpkProfile, fromCloud bool, linkName string) (*ShadowLinkConfig, error) {
	if fromCloud {
		cloudClient, err := newCloudClientSet(cfg, prof)
		if err != nil {
			return nil, err
		}
		link, err := cloudClient.ShadowLinkByNameAndRPID(ctx, linkName, prof.CloudCluster.ClusterID)
		if err != nil {
			return nil, fmt.Errorf("unable to find Shadow Link %q", linkName)
		}
		return cloudShadowLinkToConfig(link), nil
	}
	adminClient, err := adminapi.NewClient(ctx, fs, prof)
	if err != nil {
		return nil, fmt.Errorf("unable to initialize admin client: %v", err)
	}
	link, err := adminClient.ShadowLinkService().GetShadowLink(ctx, connect.NewRequest(&adminv2.GetShadowLinkRequest{
		Name: linkName,
	}))
	if err != nil {
		return nil, fmt.Errorf("unable to get Redpanda Shadow Link information: %v", handleConnectError(err, "get", linkName))
	}
	shadowLink := link.Msg.GetShadowLink()
	original := shadowLinkToConfig(shadowLink)
	addRedactedPasswordString(original, shadowLink)
	return original, nil
}

// updatedConfigFromFile parses and validates a config file for update; the
// name must match linkName, and any shadow_redpanda_id must match clusterID.
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

// editShadowLinkConfig opens an editor seeded with original; a nil diff means
// no changes. Untouched password placeholders are cleared before returning.
func editShadowLinkConfig(ctx context.Context, fs afero.Fs, original *ShadowLinkConfig) (*ShadowLinkConfig, []string, error) {
	updated, err := rpkos.EditTmpYAMLFile(ctx, fs, original)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to edit Shadow Link configuration: %w", err)
	}
	if err := validateParsedShadowLinkConfig(updated); err != nil {
		return nil, nil, fmt.Errorf("invalid Shadow Link configuration: %w", err)
	}
	if updated.Name != original.Name {
		return nil, nil, errors.New("shadow link name cannot be changed; if you need to rename, please delete and recreate the shadow link")
	}
	diff := diffConfigs(original, updated)
	if diff == nil {
		return nil, nil, nil
	}
	zap.L().Sugar().Debugf("Detected changes in: %v", strings.Join(diff, ", "))

	// An untouched placeholder means "keep the existing password"; the server
	// keeps the stored password when it's empty and the username is set.
	stripRedactedPasswords(updated)
	return updated, diff, nil
}

// addRedactedPasswordString replaces set passwords with the placeholder; the
// server only reports whether a password is set, never its value.
func addRedactedPasswordString(cfg *ShadowLinkConfig, link *adminv2.ShadowLink) {
	cfgs := link.GetConfigurations()
	if cfgs.GetClientOptions().GetAuthenticationConfiguration().GetScramConfiguration().GetPasswordSet() {
		if co := cfg.ClientOptions; co != nil {
			if auth := co.AuthenticationConfiguration; auth != nil && auth.ScramConfiguration != nil {
				auth.ScramConfiguration.Password = redacted
			}
		}
	}

	if cfgs.GetClientOptions().GetAuthenticationConfiguration().GetPlainConfiguration().GetPasswordSet() {
		if co := cfg.ClientOptions; co != nil {
			if auth := co.AuthenticationConfiguration; auth != nil && auth.PlainConfiguration != nil {
				auth.PlainConfiguration.Password = redacted
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

// stripRedactedPasswords clears editor-seeded placeholders; an empty password
// with a username set tells the server to keep the existing one.
func stripRedactedPasswords(cfg *ShadowLinkConfig) {
	if co := cfg.ClientOptions; co != nil {
		if auth := co.AuthenticationConfiguration; auth != nil {
			if scram := auth.ScramConfiguration; scram != nil && scram.Password == redacted {
				scram.Password = ""
			}
			if plain := auth.PlainConfiguration; plain != nil && plain.Password == redacted {
				plain.Password = ""
			}
		}
	}
	if sr := cfg.SchemaRegistrySyncOptions; sr != nil && sr.ShadowSchemaRegistryAPI != nil {
		if auth := sr.ShadowSchemaRegistryAPI.AuthOptions; auth != nil && auth.Basic != nil && auth.Basic.Password == redacted {
			auth.Basic.Password = ""
		}
	}
}

// diffConfigs returns the JSON paths of the fields that changed between the
// two configs, e.g. "configurations.client_options.bootstrap_servers".
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

// getJSONTag returns the field's json tag name; "" means skip the field.
func getJSONTag(field reflect.StructField) string {
	jsonTag := field.Tag.Get("json")
	if jsonTag == "" || jsonTag == "-" {
		return ""
	}
	// Take the name before any options ("name,omitempty" -> "name").
	name, _, _ := strings.Cut(jsonTag, ",")
	return name
}

// compareValues recursively compares two values and records changed paths.
func compareValues(original, updated reflect.Value, path string, changedPaths *[]string) {
	if !original.IsValid() && !updated.IsValid() {
		return
	}
	// One invalid, other valid: record as changed
	if !original.IsValid() || !updated.IsValid() {
		*changedPaths = append(*changedPaths, path)
		return
	}

	switch original.Kind() {
	case reflect.Pointer:
		if original.IsNil() && updated.IsNil() {
			return
		}
		if original.IsNil() || updated.IsNil() {
			*changedPaths = append(*changedPaths, path)
			return
		}
		compareValues(original.Elem(), updated.Elem(), path, changedPaths)
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
	for i := range typ.NumField() {
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
