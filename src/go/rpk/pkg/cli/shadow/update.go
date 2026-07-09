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
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"time"

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
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func newUpdateCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "update [LINK_NAME]",
		Short: "Update a Shadow Link",
		Long: `Update a Shadow Link.

This command opens your default editor with the current Shadow Link
configuration. Update the fields you want to change, save the file, and close
the editor. The command applies only the changed fields to the Shadow Link.
List fields, such as topic filters, are replaced as a whole: the Shadow Link
ends up with exactly the list you leave in the editor.

You cannot change the Shadow Link name. If you need to rename a Shadow Link,
delete it and create a new one with the desired name.

The editor respects your EDITOR environment variable. If EDITOR is not set, the
command uses 'vi' on Unix-like systems.
`,
		Example: `
Update a Shadow Link configuration:
  rpk shadow update my-shadow-link
`,
		Args: cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load rpk config: %v", err)
			prof := cfg.VirtualProfile()
			config.CheckExitServerlessAdmin(prof)

			// This commands retrieves the current ShadowLink configuration from
			// 2 different sources depending on whether it's for Cloud or SH.
			// The user will be prompted for changes in their editor of choice,
			// and we calculate the diff from it. At the end, we need to call
			// the appropriate API to submit the update.
			fromCloud := prof.CheckFromCloud()
			linkName := args[0]
			var (
				originalCfg      *ShadowLinkConfig
				adminClient      *rpadmin.AdminAPI
				cloudClient      *publicapi.CloudClientSet
				cloudLinkID      string
				plainPasswordSet bool
			)

			// First part: retrieve current configuration.
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
				originalCfg = cloudShadowLinkToConfig(link)

				// Cloud uses secrets for passwords so we don't need to add the
				// redacted string here.
			} else {
				adminClient, err = adminapi.NewClient(cmd.Context(), fs, prof)
				out.MaybeDie(err, "unable to initialize admin client: %v", err)

				link, err := adminClient.ShadowLinkService().GetShadowLink(cmd.Context(), connect.NewRequest(&adminv2.GetShadowLinkRequest{
					Name: linkName,
				}))
				out.MaybeDie(err, "unable to get Redpanda Shadow Link information: %v", handleConnectError(err, "get", linkName))

				shadowLink := link.Msg.GetShadowLink()
				originalCfg = shadowLinkToConfig(shadowLink)

				addRedactedPasswordString(originalCfg, shadowLink)
				plainPasswordSet = shadowLink.GetConfigurations().GetClientOptions().GetAuthenticationConfiguration().GetPlainConfiguration().GetPasswordSet()
			}

			// Second part: open editor and get updated configuration.
			updatedCfg, err := rpkos.EditTmpYAMLFile(cmd.Context(), fs, originalCfg)
			out.MaybeDie(err, "unable to edit Shadow Link configuration: %v", err)

			err = validateParsedShadowLinkConfig(updatedCfg)
			out.MaybeDie(err, "invalid Shadow Link configuration: %v", err)

			if updatedCfg.Name != originalCfg.Name {
				out.Die("shadow link name cannot be changed; if you need to rename, please delete and recreate the shadow link")
			}

			// Third part: calculate diff.
			diff := diffConfigs(originalCfg, updatedCfg)
			if diff == nil {
				out.Exit("No changes detected")
			}

			// Finally: submit the update request, for that we calculate the
			// field mask from the diff.
			if fromCloud {
				updatedSL := shadowLinkConfigToCloudUpdate(updatedCfg, cloudLinkID)

				// Cloud proto doesn't have the "configurations" wrapper, so we
				// need to strip it from the paths.
				cloudDiff := make([]string, len(diff))
				for i, path := range diff {
					cloudDiff[i] = strings.TrimPrefix(path, "configurations.")
				}

				fm, err := fieldmaskpb.New(updatedSL, cloudDiff...)
				out.MaybeDie(err, "unrecognized changed fields: %v; please report this with Redpanda Support", err)

				zap.L().Sugar().Debugf("Requesting configuration update for: %v", strings.Join(cloudDiff, ", "))
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
			// Self-hosted path.
			maskPaths := selfHostedMaskPaths(diff)
			err = checkPlainPasswordErased(updatedCfg, plainPasswordSet)
			out.MaybeDieErr(err)
			stripRedactedPasswords(originalCfg, updatedCfg)

			updatedSL := shadowLinkConfigToProto(updatedCfg)
			fm, err := fieldmaskpb.New(updatedSL, maskPaths...)
			out.MaybeDie(err, "unrecognized changed fields: %v; please report this with Redpanda Support", err)

			zap.L().Sugar().Debugf("Requesting configuration update for: %v", strings.Join(maskPaths, ", "))
			_, err = adminClient.ShadowLinkService().UpdateShadowLink(cmd.Context(), connect.NewRequest(&adminv2.UpdateShadowLinkRequest{
				ShadowLink: updatedSL,
				UpdateMask: fm,
			}))
			out.MaybeDie(err, "unable to update Shadow Link: %v", handleConnectError(err, "update", linkName))
			fmt.Printf("Successfully updated shadow link %q.\n", linkName)
		},
	}
	return cmd
}

// redactedPassword is shown in the editor in place of a password that is set
// on the cluster; the server only reports whether a password is set, never
// its value. stripRedactedPasswords clears it before an update is sent.
const redactedPassword = "<redacted>"

// if a password is set, replace it with a redacted value so user can provide
// a change easily instead of writing the full password field. The server only
// reports whether a password is set, never its value.
func addRedactedPasswordString(cfg *ShadowLinkConfig, link *adminv2.ShadowLink) {
	cfgs := link.GetConfigurations()
	if cfgs.GetClientOptions().GetAuthenticationConfiguration().GetScramConfiguration().GetPasswordSet() {
		if scram := scramConfig(cfg); scram != nil {
			scram.Password = redactedPassword
		}
	}

	if cfgs.GetSchemaRegistrySyncOptions().GetShadowSchemaRegistryApi().GetAuthOptions().GetBasic().GetPasswordSet() {
		if basic := srBasicAuth(cfg); basic != nil {
			basic.Password = redactedPassword
		}
	}
}

// stripRedactedPasswords clears passwords still holding the placeholder that
// addRedactedPasswordString injected. An unchanged password must be sent
// empty: when an update mask path covers an authentication message, the
// server replaces the whole message and preserves the stored password only
// if the incoming password is empty and the username is set. Only values
// that were redacted in the first place are cleared, so a literal
// placeholder typed on a link without a stored password is sent as-is.
func stripRedactedPasswords(original, updated *ShadowLinkConfig) {
	if scram := scramConfig(updated); scram != nil && scram.Password == redactedPassword {
		if orig := scramConfig(original); orig != nil && orig.Password == redactedPassword {
			scram.Password = ""
		}
	}
	if basic := srBasicAuth(updated); basic != nil && basic.Password == redactedPassword {
		if orig := srBasicAuth(original); orig != nil && orig.Password == redactedPassword {
			basic.Password = ""
		}
	}
}

// scramConfig returns the SCRAM configuration, or nil if any link in the
// chain is unset.
func scramConfig(cfg *ShadowLinkConfig) *ScramConfiguration {
	if cfg == nil || cfg.ClientOptions == nil || cfg.ClientOptions.AuthenticationConfiguration == nil {
		return nil
	}
	return cfg.ClientOptions.AuthenticationConfiguration.ScramConfiguration
}

// srBasicAuth returns the Schema Registry HTTP basic auth options, or nil if
// any link in the chain is unset.
func srBasicAuth(cfg *ShadowLinkConfig) *HTTPBasicAuthOptions {
	if cfg == nil || cfg.SchemaRegistrySyncOptions == nil || cfg.SchemaRegistrySyncOptions.ShadowSchemaRegistryAPI == nil || cfg.SchemaRegistrySyncOptions.ShadowSchemaRegistryAPI.AuthOptions == nil {
		return nil
	}
	return cfg.SchemaRegistrySyncOptions.ShadowSchemaRegistryAPI.AuthOptions.Basic
}

// selfHostedMaskPaths converts diff paths into update mask paths for the
// self-hosted Admin API, widening paths the API cannot apply in isolation
// (see widenSelfHostedMaskPath) and collapsing overlapping paths, which the
// server rejects.
func selfHostedMaskPaths(diff []string) []string {
	paths := make([]string, 0, len(diff))
	for _, path := range diff {
		paths = append(paths, widenSelfHostedMaskPath(path))
	}
	fm := &fieldmaskpb.FieldMask{Paths: paths}
	fm.Normalize()
	return fm.GetPaths()
}

// widenSelfHostedMaskPath widens a diff path to its parent message path when
// it ends at a field the self-hosted Admin API cannot replace in isolation:
// repeated fields are appended to and map fields merged rather than
// replaced, and naming a oneof member clears the whole oneof when the update
// does not carry that member — mask paths are applied in order, so when the
// user switches members, the old member's path can clear the new member
// right after it was set. Replacing the parent message instead is
// order-independent and results in exactly the edited configuration.
func widenSelfHostedMaskPath(path string) string {
	segments := strings.Split(path, ".")
	msg := (&adminv2.ShadowLink{}).ProtoReflect().Descriptor()
	var fd protoreflect.FieldDescriptor
	for i, segment := range segments {
		if msg == nil {
			return path
		}
		fd = msg.Fields().ByName(protoreflect.Name(segment))
		if fd == nil {
			// Unknown segment: leave the path for fieldmaskpb.New to
			// report.
			return path
		}
		if i < len(segments)-1 {
			msg = fd.Message()
		}
	}
	widen := fd.IsList() || fd.IsMap()
	if oneof := fd.ContainingOneof(); oneof != nil && !oneof.IsSynthetic() {
		widen = true
	}
	if !widen || len(segments) < 2 {
		return path
	}
	return strings.Join(segments[:len(segments)-1], ".")
}

// checkPlainPasswordErased returns an error when the cluster stores a
// SASL/PLAIN password and the update does not re-supply it. The cluster
// rebuilds the full shadow link configuration on every update and, unlike
// SCRAM, does not preserve a stored PLAIN password that is absent from the
// update, so the request would fail server-side with an unhelpful error.
func checkPlainPasswordErased(updated *ShadowLinkConfig, plainPasswordSet bool) error {
	if !plainPasswordSet || updated == nil || updated.ClientOptions == nil {
		return nil
	}
	auth := updated.ClientOptions.AuthenticationConfiguration
	if auth == nil || auth.PlainConfiguration == nil || auth.PlainConfiguration.Password != "" {
		return nil
	}
	return errors.New("the cluster cannot preserve the stored SASL/PLAIN password during updates; re-enter client_options.authentication_configuration.plain_configuration.password in the editor and retry")
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
		// time.Time has only unexported fields, which compareStructs
		// skips; compare the instants directly.
		if original.Type() == reflect.TypeOf(time.Time{}) {
			if !original.Interface().(time.Time).Equal(updated.Interface().(time.Time)) {
				*changedPaths = append(*changedPaths, path)
			}
			return
		}
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
