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
	"strings"

	"buf.build/gen/go/redpandadata/cloud/connectrpc/go/redpanda/api/controlplane/v1/controlplanev1connect"
	controlplanev1 "buf.build/gen/go/redpandadata/cloud/protocolbuffers/go/redpanda/api/controlplane/v1"
	adminv2 "buf.build/gen/go/redpandadata/core/protocolbuffers/go/redpanda/core/admin/v2"
	dataplanev1 "buf.build/gen/go/redpandadata/dataplane/protocolbuffers/go/redpanda/api/dataplane/v1"
	"connectrpc.com/connect"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/oauth/providers/auth0"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/publicapi"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"gopkg.in/yaml.v2"
)

// secretsPrefix is the prefix that denotes that a field is referencing a secret
// in the secrets store. (used in password and TLS key).
const secretsPrefix = "${secrets."

func newCreateCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var (
		noConfirm   bool
		cfgLocation string
	)
	cmd := &cobra.Command{
		Use:   "create",
		Args:  cobra.NoArgs,
		Short: "Create a Redpanda Shadow Link",
		Long: `Create a Redpanda Shadow Link.

This command creates a Shadow Link using a configuration file that defines the
connection details and synchronization settings.

Before you create a Shadow Link, generate a configuration file with 'rpk shadow
config generate' and update it with your source cluster details. The command
prompts you to confirm the creation. Use the --no-confirm flag to skip the
confirmation prompt.

When creating a Shadow Link for Redpanda Cloud, make sure to login and select
the cluster where you want to create the Shadow Link before running this
command. See 'rpk cloud login' and 'rpk cloud select'. For SCRAM authentication,
store your password in the secrets store. See 'rpk security secret --help' for
more details.

After you create the Shadow Link, use 'rpk shadow status' to monitor the
replication progress.
`,
		Example: `
Create a Shadow Link using a configuration file:
  rpk shadow create --config-file shadow-link.yaml

Create a Shadow Link without confirmation prompt:
  rpk shadow create -c shadow-link.yaml --no-confirm
`,
		Run: func(cmd *cobra.Command, _ []string) {
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load rpk config: %v", err)
			prof := cfg.VirtualProfile()
			config.CheckExitServerlessAdmin(prof)

			slCfg, err := parseShadowLinkConfig(fs, cfgLocation)
			out.MaybeDie(err, "unable to parse Shadow Link configuration file: %v", err)

			// This is a naive client side validation, the server will do a full
			// validation and return proper errors if something is wrong.
			err = validateParsedShadowLinkConfig(slCfg)
			out.MaybeDie(err, "invalid Shadow Link configuration: %v", err)

			printShadowLinkCfgOverview(slCfg)
			if !noConfirm {
				ok, err := out.Confirm("Do you want to create this shadow link?")
				out.MaybeDie(err, "unable to confirm Shadow Link creation: %v", err)
				if !ok {
					out.Exit("Shadow Link creation cancelled")
				}
			}

			successMsgTmpl := "Successfully created shadow link %q with ID %q. To query the status, run:\n  'rpk shadow status %[1]v'"
			if prof.CheckFromCloud() {
				cloudClient, err := publicapi.NewValidatedCloudClientSet(
					cfg.DevOverrides().PublicAPIURL,
					prof.CurrentAuth().AuthToken,
					auth0.NewClient(cfg.DevOverrides()).Audience(),
					[]string{prof.CurrentAuth().ClientID},
				)
				out.MaybeDieErr(err)

				err = validateCloudSecrets(cmd.Context(), prof, slCfg)
				out.MaybeDie(err, "unable to validate cloud secrets: %v", err)

				op, err := cloudClient.ShadowLink.CreateShadowLink(cmd.Context(), connect.NewRequest(&controlplanev1.CreateShadowLinkRequest{
					ShadowLink: shadowLinkConfigToCloudCreate(slCfg),
				}))
				out.MaybeDie(err, "unable to create Shadow Link: %v", err)

				spinner := out.NewSpinner(cmd.Context(), "Creating Shadow Link...", out.WithElapsedTime())
				isComplete, err := waitForOperation(cmd.Context(), cloudClient, op.Msg.GetOperation().GetId())
				if err != nil {
					if oErr := new(OperationFailedError); errors.As(err, &oErr) {
						spinner.Fail(tryShadowLinkErrReason(cmd.Context(), cloudClient.ShadowLink, oErr))
						os.Exit(1)
					}
					spinner.Fail(fmt.Sprintf("unable to confirm Shadow Link creation: %v", err))
					os.Exit(1)
				}
				if isComplete {
					spinner.Success(fmt.Sprintf(successMsgTmpl, slCfg.Name, op.Msg.GetOperation().GetResourceId()))
					os.Exit(0)
				}
				spinner.Stop()
				out.Exit("Shadow link creation is taking longer than expected. Please check the state of the shadow link using 'rpk shadow describe %v' and 'rpk shadow status %v'", slCfg.Name, slCfg.Name)
			}
			cl, err := adminapi.NewClient(cmd.Context(), fs, prof)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			spinner := out.NewSpinner(cmd.Context(), "Creating Shadow Link...")
			link, err := cl.ShadowLinkService().CreateShadowLink(cmd.Context(), connect.NewRequest(&adminv2.CreateShadowLinkRequest{
				ShadowLink: shadowLinkConfigToProto(slCfg),
			}))
			spinner.Stop()
			out.MaybeDie(err, "unable to create shadow link: %v", handleConnectError(err, "create", slCfg.Name))

			out.Exit(successMsgTmpl, link.Msg.GetShadowLink().GetName(), link.Msg.GetShadowLink().GetUid())
		},
	}

	cmd.Flags().BoolVar(&noConfirm, "no-confirm", false, "Disable confirmation prompt")
	cmd.Flags().StringVarP(&cfgLocation, "config-file", "c", "", "Path to configuration file to use for the shadow link; use --help for details")
	cmd.MarkFlagRequired("config-file")
	return cmd
}

func parseShadowLinkConfig(fs afero.Fs, path string) (*ShadowLinkConfig, error) {
	file, err := afero.ReadFile(fs, path)
	if err != nil {
		return nil, fmt.Errorf("unable to read Shadow Link config file %q: %w", path, err)
	}

	var slCfg ShadowLinkConfig
	err = yaml.Unmarshal(file, &slCfg)
	if err != nil {
		return nil, fmt.Errorf("unable to parse Shadow Link config file %q: %w", path, err)
	}
	return &slCfg, nil
}

func printShadowLinkCfgOverview(slCfg *ShadowLinkConfig) {
	tw := out.NewTable()
	defer tw.Flush()
	tw.Print("Link Name:", slCfg.Name)
	if slCfg.CloudOptions != nil {
		tw.Print("Shadow Redpanda ID:", slCfg.CloudOptions.ShadowRedpandaID)
		if slCfg.CloudOptions.SourceRedpandaID != "" {
			tw.Print("Source Redpanda ID:", slCfg.CloudOptions.SourceRedpandaID)
		}
	}
	if len(slCfg.ClientOptions.BootstrapServers) > 0 {
		tw.Print("Bootstrap Servers:", "")
		for _, srv := range slCfg.ClientOptions.BootstrapServers {
			tw.Print("", fmt.Sprintf("- %s", srv))
		}
	}
}

func validateParsedShadowLinkConfig(slCfg *ShadowLinkConfig) error {
	if slCfg == nil {
		return errors.New("provided configuration file generated an empty configuration")
	}
	if slCfg.Name == "" {
		return errors.New("the Shadow Link name is required")
	}
	// Cloud configuration does not require bootstrap servers.
	if slCfg.CloudOptions == nil && len(slCfg.ClientOptions.BootstrapServers) == 0 {
		return errors.New("at least one bootstrap server is required")
	}
	if tls := slCfg.ClientOptions.TLSSettings; tls != nil && tls.TLSFileSettings != nil && tls.TLSPEMSettings != nil {
		return errors.New("only one of TLS file settings or PEM settings can be provided")
	}
	if auth := slCfg.ClientOptions.AuthenticationConfiguration; auth != nil && auth.ScramConfiguration != nil && auth.PlainConfiguration != nil {
		return errors.New("only one of scram_configuration or plain_configuration can be provided")
	}
	if ts := slCfg.TopicMetadataSyncOptions; ts != nil {
		var count int
		if ts.StartAtLatest != nil {
			count++
		}
		if ts.StartAtEarliest != nil {
			count++
		}
		if ts.StartAtTimestamp != nil {
			count++
		}
		if count > 1 {
			return errors.New("only one of start_at_latest, start_at_earliest, or start_at_timestamp can be provided")
		}
	}

	slc := slCfg.CloudOptions
	if slc == nil {
		return nil
	}
	// Cloud only validations.
	if slc.ShadowRedpandaID == "" {
		return errors.New("shadow_redpanda_id is required in cloud options")
	}
	if slc.ShadowRedpandaID == slc.SourceRedpandaID {
		return errors.New("shadow_redpanda_id and source_redpanda_id cannot be the same")
	}
	co := slCfg.ClientOptions
	if co == nil {
		return nil
	}
	if co.TLSSettings != nil && co.TLSSettings.TLSFileSettings != nil {
		return errors.New("TLS file settings are not supported when using cloud options; use tls_pem_settings instead")
	}
	if pw := authPassword(co); pw != "" && !strings.HasPrefix(pw, secretsPrefix) {
		return errors.New("cloud shadow links don't support plain passwords, you must use secrets from the secrets store. See 'rpk security secret --help' for more details")
	}
	if key := tlsKey(slCfg.ClientOptions); key != "" && !strings.HasPrefix(key, secretsPrefix) {
		return errors.New("cloud shadow links don't support plain TLS keys, you must use secrets from the secrets store. See 'rpk security secret --help' for more details")
	}
	return nil
}

// authPassword extracts the authentication password from the client options, if set.
func authPassword(co *ShadowLinkClientOptions) string {
	if co == nil || co.AuthenticationConfiguration == nil {
		return ""
	}
	auth := co.AuthenticationConfiguration
	if auth.ScramConfiguration != nil {
		return auth.ScramConfiguration.Password
	}
	if auth.PlainConfiguration != nil {
		return auth.PlainConfiguration.Password
	}
	return ""
}

func tlsKey(co *ShadowLinkClientOptions) string {
	if co == nil || co.TLSSettings == nil {
		return ""
	}
	tls := co.TLSSettings
	if pem := tls.TLSPEMSettings; pem != nil {
		return pem.Key
	}
	return ""
}

// tryShadowLinkErrReason attempts to extract a more specific error reason from
// the Shadow Link response, defaulting to a generic error message if the call
// fails or there is no additional information.
func tryShadowLinkErrReason(ctx context.Context, cl controlplanev1connect.ShadowLinkServiceClient, oErr *OperationFailedError) string {
	errMsg := oErr.Error()
	link, err := cl.GetShadowLink(ctx, connect.NewRequest(&controlplanev1.GetShadowLinkRequest{
		Id: oErr.Operation.GetResourceId(),
	}))
	if err != nil || link.Msg.GetShadowLink().GetReason() == "" {
		return errMsg
	}
	return fmt.Sprintf("%v. Reason: %v", errMsg, link.Msg.GetShadowLink().GetReason())
}

func validateCloudSecrets(ctx context.Context, prof *config.RpkProfile, slCfg *ShadowLinkConfig) error {
	// We should only try to validate if pass or key are present in the config.
	pass, key := authPassword(slCfg.ClientOptions), tlsKey(slCfg.ClientOptions)
	if pass == "" && key == "" {
		return nil
	}
	// We can't validate the presence of secrets if the Shadow Link is not for
	// this cluster. In this case we default to the server validation.
	if slCfg.CloudOptions != nil && slCfg.CloudOptions.ShadowRedpandaID != prof.CloudCluster.ClusterID {
		zap.L().Sugar().Warn("Shadow Link cluster is different from the current selected cluster in your profile; skipping secrets validation")
		return nil
	}
	dpClient, err := publicapi.DataplaneClientFromRpkProfile(prof)
	if err != nil {
		return err
	}
	secrets, err := dpClient.Secret.ListSecrets(ctx, connect.NewRequest(&dataplanev1.ListSecretsRequest{
		PageSize: 500, // 500 is a reasonable upper limit for now.
		Filter: &dataplanev1.ListSecretsFilter{
			Scopes: []dataplanev1.Scope{dataplanev1.Scope_SCOPE_REDPANDA_CLUSTER},
		},
	}))
	if err != nil {
		return fmt.Errorf("unable to list secrets at REDPANDA_CLUSTER scope: %v", err)
	}

	secretRefs := make(map[string]struct{})
	for _, secret := range secrets.Msg.GetSecrets() {
		secretRefs[fmt.Sprintf("%s%s}", secretsPrefix, secret.Id)] = struct{}{}
	}

	if pass != "" {
		if _, ok := secretRefs[pass]; !ok {
			return fmt.Errorf("unable to find authentication password secret %q in the shadow cluster secrets store (REDPANDA_CLUSTER scope)", pass)
		}
	}
	if key != "" {
		if _, ok := secretRefs[key]; !ok {
			return fmt.Errorf("unable to find TLS key secret %q in the shadow cluster secrets store (REDPANDA_CLUSTER scope)", key)
		}
	}
	return nil
}
