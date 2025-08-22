// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package config

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"google.golang.org/genproto/googleapis/rpc/errdetails"

	controlplanev1 "buf.build/gen/go/redpandadata/cloud/protocolbuffers/go/redpanda/api/controlplane/v1"
	"connectrpc.com/connect"
	"github.com/redpanda-data/common-go/rpadmin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/publicapi"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/structpb"
	"gopkg.in/yaml.v3"
)

// anySlice represents a slice of any value type.
type anySlice []any

// A custom unmarshal is needed because go-yaml parse "YYYY-MM-DD" as a full
// timestamp, writing YYYY-MM-DD HH:MM:SS +0000 UTC when encoding, so we are
// going to treat timestamps as strings.
// See: https://github.com/go-yaml/yaml/issues/770

func (s *anySlice) UnmarshalYAML(n *yaml.Node) error {
	replaceTimestamp(n)

	var a []any
	err := n.Decode(&a)
	if err != nil {
		return err
	}
	*s = a
	return nil
}

func parseArgs(args []string) ([]string, error) {
	if len(args) == 2 && !strings.Contains(args[0], "=") {
		args = []string{args[0] + "=" + args[1]}
	}
	for _, arg := range args {
		if !strings.Contains(arg, "=") {
			return nil, fmt.Errorf("invalid arguments: %v, please use one of 'rpk cluster config set <key> <value>' or 'rpk cluster config set <key>=<value>', for empty values use 'rpk cluster config set <key>=\"\"' ", args)
		}
	}
	return args, nil
}

func newSetCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var noConfirm bool
	cmd := &cobra.Command{
		Use:   "set [KEY] [VALUE]",
		Short: "Set a single cluster configuration property",
		Long: `Set a single cluster configuration property.

This command is provided for use in scripts.  For interactive editing, or bulk
changes, use the 'edit' and 'import' commands respectively.

You may also use <key>=<value> notation for setting configuration properties:

  rpk cluster config set log_retention_ms=-1

If an empty string is given as the value, the property is reset to its default.
Use the flag '--no-confirm' to avoid the confirmation prompt.`,

		Args: cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			configs, err := parseArgs(args)
			out.MaybeDieErr(err)
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			vp := cfg.VirtualProfile()

			if vp.FromCloud {
				if vp.CloudCluster.IsServerless() {
					out.Die("rpk cluster config set is not supported on Redpanda serverless clusters")
				}
				out.MaybeDie(err, "rpk unable to load config: %v", err)
				operation, err := setCloudConfig(cmd.Context(), cfg, vp, configs)
				out.MaybeDieErr(err)
				fmt.Print("Processing configuration, this operation may take up to 10 minutes. To check the status, run 'rpk cluster config status'\n\n")
				fmt.Printf("Operation ID: %s \n", operation.GetOperation().GetId())
			} else {
				client, err := adminapi.NewClient(cmd.Context(), fs, vp)
				out.MaybeDie(err, "unable to initialize admin client: %v", err)

				schema, err := client.ClusterConfigSchema(cmd.Context())
				out.MaybeDie(err, "unable to query config schema: %v", err)

				upsert, remove, err := validateConfigSelfHosted(schema, configs, noConfirm)
				out.MaybeDieErr(err)

				result, err := client.PatchClusterConfig(cmd.Context(), upsert, remove)

				if he := (*rpadmin.HTTPResponseError)(nil); errors.As(err, &he) {
					// Special case 400 (validation) errors with friendly output
					// about which configuration properties were invalid.
					if he.Response.StatusCode == 400 {
						ve, err := formatValidationError(err, he)
						out.MaybeDie(err, "error setting config: %v", err)
						out.Die("No changes were made: %v", ve)
					}
				}

				out.MaybeDie(err, "error setting property: %v", err)
				fmt.Printf("Successfully updated configuration. New configuration version is %d.\n", result.ConfigVersion)

				status, err := client.ClusterConfigStatus(cmd.Context(), true)
				out.MaybeDie(err, "unable to check if the cluster needs to be restarted: %v\nCheck the status with 'rpk cluster config status'.", err)
				for _, value := range status {
					if value.Restart {
						fmt.Print("\nCluster needs to be restarted. See more details with 'rpk cluster config status'.\n")
						break
					}
				}
			}
		},
	}

	cmd.Flags().BoolVar(&noConfirm, "no-confirm", false, "Disable confirmation prompt")
	return cmd
}

func validateConfigSelfHosted(schema rpadmin.ConfigSchema, args []string, noConfirm bool) (map[string]any, []string, error) {
	upsert := make(map[string]any)
	remove := make([]string, 0)

	for _, arg := range args {
		split := strings.SplitN(arg, "=", 2)
		key, value := split[0], split[1]

		// Disabling Tiered Storage requires a confirmation from the user because it may lead to data loss.
		if key == "cloud_storage_enable_remote_write" && value == "false" {
			if !noConfirm {
				confirmed, err := out.Confirm("Warning: disabling Tiered Storage may lead to data loss. If you only want to pause Tiered Storage temporarily, use the 'cloud_storage_enable_segment_uploads' option. Abort?")
				out.MaybeDie(err, "unable to read user input: %v", err)
				if confirmed {
					out.Die("Aborted by user.")
				}
			}
		}

		meta, ok := schema[key]

		if !ok {
			// loop over schema, try to find key in the Aliases,
			for _, v := range schema {
				if slices.Contains(v.Aliases, key) {
					meta, ok = v, true
					break
				}
			}
			if !ok {
				return nil, nil, fmt.Errorf("unknown property %q", key)
			}
		}

		// - For scalars, pass string values through to the REST
		// API -- it will give more informative errors than we can
		// about validation.  Special case strings for nullable
		// properties ('null') and for resetting to default ('')
		// - For arrays, make an effort: otherwise the REST API
		// may interpret a scalar string as a list of length 1
		// (via one_or_many_property).

		if meta.Nullable && value == "null" {
			// Nullable types may be explicitly set to null
			upsert[key] = nil
		} else if meta.Type != "string" && (value == "") {
			// Non-string types that receive an empty string
			// are reset to default
			remove = append(remove, key)
		} else if meta.Type == "array" {
			var a anySlice
			err := yaml.Unmarshal([]byte(value), &a)
			if err != nil {
				return nil, nil, fmt.Errorf("invalid list syntax: %v", err)
			}
			upsert[key] = a
		} else {
			upsert[key] = value
		}
	}

	return upsert, remove, nil
}

func setCloudConfig(ctx context.Context, cfg *config.Config, p *config.RpkProfile, configs []string) (*controlplanev1.UpdateClusterOperation, error) {
	cloudClient := publicapi.NewCloudClientSet(cfg.DevOverrides().PublicAPIURL, p.CurrentAuth().AuthToken)

	redpandaConfigs := make(map[string]any)
	paths := make([]string, 0)

	for _, c := range configs {
		split := strings.SplitN(c, "=", 2)
		key, value := split[0], split[1]
		redpandaConfigs[key] = value
		paths = append(paths, fmt.Sprintf("cluster_configuration.custom_properties.%s", key))
	}
	customerProperties, err := structpb.NewStruct(redpandaConfigs)
	if err != nil {
		return nil, fmt.Errorf("internal error while converting config to redpanda cloud configs: %v", err)
	}
	req := &controlplanev1.UpdateClusterRequest{
		Cluster: &controlplanev1.ClusterUpdate{
			Id: p.CloudCluster.ClusterID,
			ClusterConfiguration: &controlplanev1.ClusterUpdate_ClusterConfiguration{
				CustomProperties: customerProperties,
			},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: paths},
	}

	operation, err := cloudClient.Cluster.UpdateCluster(ctx, connect.NewRequest(req))
	if err != nil {
		var ce *connect.Error
		if errors.As(err, &ce) {
			if ce.Code() == connect.CodePermissionDenied {
				return nil, fmt.Errorf("this user does not have permission to update the cluster configuration, please check your role or contact admin")
			}
			if ce.Code() == connect.CodeNotFound {
				return nil, fmt.Errorf("cluster not found. Please ensure the cluster exists in the cloud")
			}
			if ce.Code() == connect.CodeInvalidArgument {
				var errs []string
				for _, detail := range ce.Details() {
					c, _ := detail.Value()
					switch d := c.(type) {
					case *errdetails.BadRequest:
						for _, violation := range d.FieldViolations {
							errs = append(
								errs,
								fmt.Sprintf("Field violation, description: %s\n", violation.Description),
							)
						}
					default:
						// do nothing
					}
				}
				if len(errs) > 0 {
					return nil, fmt.Errorf("invalid arguments: %s", strings.Join(errs, "\n"))
				}
			}
		}
		return nil, fmt.Errorf("internal error while updating redpanda cloud configs: %v", err)
	}
	return operation.Msg, nil
}
