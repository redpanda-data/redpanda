// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package adminapi

import (
	"context"

	adminv2 "buf.build/gen/go/redpandadata/core/protocolbuffers/go/redpanda/core/admin/v2"
	"connectrpc.com/connect"
	"github.com/redpanda-data/common-go/rpadmin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/redpanda"
)

// HasMinimumVersion returns true if all brokers in the cluster are at least
// the given version. Returns false on any error or if broker info is missing.
func HasMinimumVersion(ctx context.Context, cl *rpadmin.AdminAPI, version redpanda.Version) bool {
	brokers, err := cl.BrokerService().ListBrokers(ctx, &connect.Request[adminv2.ListBrokersRequest]{})
	if err != nil || len(brokers.Msg.Brokers) == 0 {
		return false
	}
	for _, broker := range brokers.Msg.Brokers {
		brokerResp, err := cl.BrokerService().GetBroker(ctx, &connect.Request[adminv2.GetBrokerRequest]{Msg: &adminv2.GetBrokerRequest{NodeId: &broker.NodeId}})
		if err != nil || brokerResp.Msg.Broker.BuildInfo == nil {
			return false
		}
		v, err := redpanda.VersionFromString(brokerResp.Msg.Broker.BuildInfo.Version)
		if err != nil || !v.IsAtLeast(version) {
			return false
		}
	}
	return true
}
