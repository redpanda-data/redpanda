// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package config

import (
	"errors"
	"fmt"
	"slices"

	"go.uber.org/zap"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	controlplanev1 "buf.build/gen/go/redpandadata/cloud/protocolbuffers/go/redpanda/api/controlplane/v1"
	"connectrpc.com/connect"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/publicapi"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newStatusCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Get configuration status of redpanda nodes.",
		Long: `Get configuration status of redpanda nodes.

For each node, indicate whether a restart is required for settings to
take effect, and any settings that the node has identified as invalid
or unknown properties.

Additionally show the version of cluster configuration that each node
has applied: under normal circumstances these should all be equal,
a lower number shows that a node is out of sync, perhaps because it
is offline.`,
		Args: cobra.NoArgs,
		Run: func(cmd *cobra.Command, _ []string) {
			vp, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)

			if vp.CheckFromCloud() {
				if vp.CloudCluster.IsServerless() {
					out.Die("rpk cluster config status is not supported on Redpanda serverless clusters")
				}
				cfg, err := p.Load(fs)
				out.MaybeDie(err, "rpk unable to load config: %v", err)
				ops, err := getCloudConfigStatus(cmd, cfg, vp)
				out.MaybeDieErr(err)
				tw := out.NewTable("OPERATION-ID", "STATUS", "STARTED", "COMPLETED")
				defer tw.Flush()
				for _, op := range ops {
					tw.PrintStructFields(struct {
						OperationID string
						Status      string
						Started     string
						Completed   string
					}{op.OperationID, op.Status, op.Started, op.Completed})
				}
			} else {
				client, err := adminapi.NewClient(cmd.Context(), fs, vp)
				out.MaybeDie(err, "unable to initialize admin client: %v", err)

				// GET the status endpoint
				resp, err := client.ClusterConfigStatus(cmd.Context(), false)
				out.MaybeDie(err, "error fetching status: %v", err)

				tw := out.NewTable("NODE", "CONFIG-VERSION", "NEEDS-RESTART", "INVALID", "UNKNOWN")
				defer tw.Flush()

				for _, node := range resp {
					tw.PrintStructFields(struct {
						ID      int64
						Version int64
						Restart bool
						Invalid []string
						Unknown []string
					}{node.NodeID, node.ConfigVersion, node.Restart, node.Invalid, node.Unknown})
				}
			}
		},
	}

	return cmd
}

type CloudStatusItem struct {
	OperationID string
	Status      string
	Details     string
	Started     string
	Completed   string
}

func getCloudConfigStatus(cmd *cobra.Command, cfg *config.Config, p *config.RpkProfile) ([]CloudStatusItem, error) {
	cloudClient := publicapi.NewCloudClientSet(cfg.DevOverrides().PublicAPIURL, p.CurrentAuth().AuthToken)

	operation, err := cloudClient.Operations.ListOperations(cmd.Context(), connect.NewRequest(&controlplanev1.ListOperationsRequest{
		Filter: &controlplanev1.ListOperationsRequest_Filter{
			ResourceId: p.CloudCluster.ClusterID,
			TypeIn: []controlplanev1.Operation_Type{
				controlplanev1.Operation_TYPE_UPDATE_CLUSTER,
			},
		},
		ReadMask: &fieldmaskpb.FieldMask{
			Paths: []string{"metadata", "id", "state", "started_at", "finished_at", "resource_id"},
		},
	}))
	if err != nil {
		var ce *connect.Error
		if errors.As(err, &ce) {
			if ce.Code() == connect.CodePermissionDenied {
				return nil, fmt.Errorf("permission denied")
			}
		}
		return nil, fmt.Errorf("internal error while updating redpanda cloud configs: %v", err)
	}

	cst := make([]CloudStatusItem, 0)

	for _, op := range operation.Msg.Operations {
		opm := controlplanev1.UpdateClusterMetadata{}
		metadata := op.GetMetadata()
		err = anypb.UnmarshalTo(metadata, &opm, proto.UnmarshalOptions{})
		if err != nil {
			zap.L().Debug("failed to unmarshal update operation metadata", zap.Error(err))
		}
		complete := ""
		if op.FinishedAt != nil {
			complete = op.FinishedAt.AsTime().Format("2006-01-02 15:04:05")
		}
		if opm.GetUpdateType() != nil {
			if slices.Contains(opm.GetUpdateType(), controlplanev1.UpdateClusterMetadata_UPDATE_CLUSTER_TYPE_CUSTOMER_CONFIG) {
				cst = append(cst, CloudStatusItem{
					OperationID: op.Id,
					Status:      mapOpsStateToState(op),
					Details:     fmt.Sprintf("https://cloud.redpanda.com/operations/%s", op.Id),
					Started:     op.GetStartedAt().AsTime().Format("2006-01-02 15:04:05"),
					Completed:   complete,
				})
			}
		}
	}

	return cst, nil
}

func mapOpsStateToState(op *controlplanev1.Operation) string {
	switch op.GetState() {
	case controlplanev1.Operation_STATE_COMPLETED:
		return "COMPLETED"
	case controlplanev1.Operation_STATE_FAILED:
		return "FAILED"
	case controlplanev1.Operation_STATE_IN_PROGRESS:
		return "RUNNING"
	default:
		return "UNKNOWN"
	}
}
