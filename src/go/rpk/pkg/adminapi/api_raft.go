// Copyright 2023 Redpanda Data, Inc.
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
	"net/http"
)

type RaftRecoveryStatus struct {
	PartitionsToRecover int `json:"partitions_to_recover"`
	PartitionsActive    int `json:"partitions_active"`
	OffsetsPending      int `json:"offsets_pending"`
}

// RaftRecoveryStatus returns the node's recovery status.
func (a *AdminAPI) RaftRecoveryStatus(ctx context.Context) (RaftRecoveryStatus, error) {
	var status RaftRecoveryStatus
	return status, a.sendOne(ctx, http.MethodGet, "/v1/raft/recovery/status", nil, &status, false)
}
