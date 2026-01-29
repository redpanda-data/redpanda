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
	"math"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"

	"google.golang.org/protobuf/types/known/structpb"

	controlplanev1 "buf.build/gen/go/redpandadata/cloud/protocolbuffers/go/redpanda/api/controlplane/v1"
	"connectrpc.com/connect"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/publicapi"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

func newGetCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "get [KEY]",
		Short: "Get a cluster configuration property",
		Long: `Get a cluster configuration property.

This command is provided for use in scripts.  For interactive editing, or bulk
output, use the 'edit' and 'export' commands respectively.`,
		Args: cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			key := args[0]

			cfg, err := p.Load(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)

			p := cfg.VirtualProfile()
			if p.CheckFromCloud() {
				if p.CloudCluster.IsServerless() {
					out.Die("rpk cluster config get is not supported for serverless clusters")
				}
				configValue, e := getCloudConfig(cmd.Context(), cfg, p, key)
				out.MaybeDie(e, "rpk unable to get cloud config: %v", e)
				out.Exit(configValue)
			}

			client, err := adminapi.NewClient(cmd.Context(), fs, p)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			currentConfig, err := client.SingleKeyConfig(cmd.Context(), key)
			out.MaybeDie(err, "unable to query current config: %v", err)

			val, exists := currentConfig[key]
			if !exists {
				out.Die("Property '%s' not found", key)
			} else {
				// currentConfig is the result of json.Unmarshal into a
				// map[string]interface{}. Due to json rules, all numbers
				// are float64. We do not want to print floats for large
				// numbers.
				if f64, ok := val.(float64); ok {
					if math.Mod(f64, 1.0) == 0 {
						val = int64(f64)
					} else {
						val = f64
					}
				}
				// Intentionally bare output, so that the output can be readily
				// consumed in a script.
				bytes, err := yaml.Marshal(val)
				out.MaybeDie(err, "Unexpected non-YAML-encodable value %v", val)
				fmt.Print(string(bytes))
			}
		},
	}

	return cmd
}

func getCloudConfig(ctx context.Context, cfg *config.Config, p *config.RpkProfile, configName string) (string, error) {
	cloudClient := publicapi.NewCloudClientSet(cfg.DevOverrides().PublicAPIURL, p.CurrentAuth().AuthToken)
	req := connect.NewRequest(&controlplanev1.GetClusterRequest{
		Id: p.CloudCluster.ClusterID,
	})

	cluster, err := cloudClient.Cluster.GetCluster(ctx, req)
	if err != nil {
		var ce *connect.Error
		if errors.As(err, &ce) {
			if ce.Code() == connect.CodePermissionDenied {
				return "", fmt.Errorf("this user does not have permission to get cluster information, please check your role or contact admin")
			}
			if ce.Code() == connect.CodeNotFound {
				return "", fmt.Errorf("cluster not found. Please ensure the cluster exists in Redpanda Cloud organization")
			}
		}
		return "", fmt.Errorf("internal error while getting Redpanda cloud configs: %v", err)
	}
	configs := cluster.Msg.GetCluster().GetClusterConfiguration().GetComputedProperties()
	value, ok := configs.GetFields()[configName]
	if !ok {
		return "", fmt.Errorf("unknown property: %s. \nsome configurations are not available for retrieval from cloud clusters", configName)
	}

	return fromStructPbToString(value), nil
}

func fromStructPbToString(s *structpb.Value) string {
	switch s.Kind.(type) {
	case *structpb.Value_ListValue:
		var v []string
		for _, value := range s.GetListValue().Values {
			v = append(v, fromStructPbToString(value))
		}
		return fmt.Sprintf("%v", strings.Join(v, ", "))
	case *structpb.Value_StructValue:
		return protojson.MarshalOptions{Multiline: false}.Format(s.GetStructValue())
	case *structpb.Value_NullValue:
		return "null"
	case *structpb.Value_NumberValue:
		return fmt.Sprintf("%v", s.GetNumberValue())
	case *structpb.Value_StringValue:
		return s.GetStringValue()
	case *structpb.Value_BoolValue:
		return fmt.Sprintf("%t", s.GetBoolValue())
	default:
		return s.String()
	}
}
