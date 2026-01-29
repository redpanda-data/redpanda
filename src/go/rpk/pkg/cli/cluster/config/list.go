// Copyright 2025 Redpanda Data, Inc.
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
	"regexp"
	"sort"

	"google.golang.org/protobuf/types/known/structpb"

	controlplanev1 "buf.build/gen/go/redpandadata/cloud/protocolbuffers/go/redpanda/api/controlplane/v1"
	"connectrpc.com/connect"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/publicapi"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newListCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var filter string

	cmd := &cobra.Command{
		Use:   "list",
		Short: "List cluster configuration properties",
		Long: `List cluster configuration properties.

This command lists all available cluster configuration properties. Use the 'get'
command to retrieve specific property values, or 'edit' for interactive editing.

Use the --filter flag with a regular expression to filter configuration keys.`,
		Example: `
List all cluster configuration properties:
  rpk cluster config list

List configuration properties matching a filter:
  rpk cluster config list --filter="kafka.*"

List configuration properties in JSON format:
  rpk cluster config list --format=json

List configuration properties in YAML format:
  rpk cluster config list --format=yaml`,
		Args: cobra.NoArgs,
		Run: func(cmd *cobra.Command, _ []string) {
			f := p.Formatter
			if h, ok := f.Help(map[string]any{}); ok {
				out.Exit(h)
			}
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)

			p := cfg.VirtualProfile()

			var configMap map[string]any

			if p.CheckFromCloud() {
				if p.CloudCluster.IsServerless() {
					out.Die("rpk cluster config list is not supported for serverless clusters")
				}
				configMap, err = listCloudConfig(cmd.Context(), cfg, p)
				out.MaybeDie(err, "rpk unable to list cloud config: %v", err)
			} else {
				client, err := adminapi.NewClient(cmd.Context(), fs, p)
				out.MaybeDie(err, "unable to initialize admin client: %v", err)

				currentConfig, err := client.Config(cmd.Context(), true)
				out.MaybeDie(err, "unable to query current config: %v", err)

				configMap = make(map[string]any)
				for key, value := range currentConfig {
					configMap[key] = formatValueForDisplay(value)
				}
			}

			// Apply regex filter if provided
			if filter != "" {
				filteredMap, err := filterConfigMap(configMap, filter)
				out.MaybeDie(err, "unable to apply filter: %v", err)
				configMap = filteredMap
			}

			if isText, _, s, err := f.Format(configMap); !isText {
				out.MaybeDie(err, "unable to print in the required format %q: %v", f.Kind, err)
				out.Exit(s)
				return
			}

			// Convert map to sorted slice only for table display
			keys := make([]string, 0, len(configMap))
			for key := range configMap {
				keys = append(keys, key)
			}
			sort.Strings(keys)

			t := out.NewTable("Property", "Value")
			defer t.Flush()
			for _, key := range keys {
				t.PrintStructFields(struct {
					Key   string
					Value any
				}{
					Key:   key,
					Value: configMap[key],
				})
			}
		},
	}

	cmd.Flags().StringVar(&filter, "filter", "", "Filter configuration keys using regular expression")
	p.InstallFormatFlag(cmd)
	return cmd
}

func listCloudConfig(ctx context.Context, cfg *config.Config, p *config.RpkProfile) (map[string]any, error) {
	cloudClient := publicapi.NewCloudClientSet(cfg.DevOverrides().PublicAPIURL, p.CurrentAuth().AuthToken)
	req := connect.NewRequest(&controlplanev1.GetClusterRequest{
		Id: p.CloudCluster.ClusterID,
	})

	cluster, err := cloudClient.Cluster.GetCluster(ctx, req)
	if err != nil {
		var ce *connect.Error
		if errors.As(err, &ce) {
			if ce.Code() == connect.CodePermissionDenied {
				return nil, fmt.Errorf("this user does not have permission to get cluster information, please check your role or contact admin")
			}
			if ce.Code() == connect.CodeNotFound {
				return nil, fmt.Errorf("cluster not found. Please ensure the cluster exists in Redpanda Cloud organization")
			}
		}
		return nil, fmt.Errorf("unable to get Redpanda Cloud configs: %v", err)
	}

	configs := cluster.Msg.GetCluster().GetClusterConfiguration().GetComputedProperties()
	configMap := make(map[string]any, len(configs.GetFields()))

	for key, value := range configs.GetFields() {
		configMap[key] = fromStructPbToInterface(value)
	}

	return configMap, nil
}

func fromStructPbToInterface(s *structpb.Value) any {
	switch s.Kind.(type) {
	case *structpb.Value_ListValue:
		var v []any
		for _, value := range s.GetListValue().Values {
			v = append(v, fromStructPbToInterface(value))
		}
		return v
	case *structpb.Value_StructValue:
		result := make(map[string]any)
		for key, value := range s.GetStructValue().Fields {
			result[key] = fromStructPbToInterface(value)
		}
		return result
	case *structpb.Value_NullValue:
		return nil
	case *structpb.Value_NumberValue:
		return formatValueForDisplay(s.GetNumberValue())
	case *structpb.Value_StringValue:
		return s.GetStringValue()
	case *structpb.Value_BoolValue:
		return s.GetBoolValue()
	default:
		return s.String()
	}
}

func filterConfigMap(configMap map[string]any, filterPattern string) (map[string]any, error) {
	regex, err := regexp.Compile(filterPattern)
	if err != nil {
		return nil, fmt.Errorf("invalid regex pattern '%s': %v", filterPattern, err)
	}

	filteredMap := make(map[string]any)
	for key, value := range configMap {
		if regex.MatchString(key) {
			filteredMap[key] = value
		}
	}

	return filteredMap, nil
}

func formatValueForDisplay(val any) any {
	// Handle float64 values to avoid scientific notation in table display
	// This is similar to the logic in get.go
	if f64, ok := val.(float64); ok {
		if math.Mod(f64, 1.0) == 0 {
			// If it's a whole number, convert to int64
			return int64(f64)
		} else {
			// Keep as float64 but will be formatted normally (not in scientific notation)
			return f64
		}
	}
	return val
}
