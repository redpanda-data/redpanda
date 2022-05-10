// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package admin

import "net/http"

type NodeConfig struct {
	NodeID int `json:"node_id"`
	// TODO: add the rest of the fields
}

// NodeConfig returns a single node configuration.
// It's expected to be called from an AdminAPI with a single broker URL,
// otherwise the method will return an error.
func (a *AdminAPI) GetNodeConfig() (NodeConfig, error) {
	var nodeconfig NodeConfig

	return nodeconfig, a.sendOne(http.MethodGet, "/v1/node_config", nil, &nodeconfig, false)
}
