// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package admin

import (
	"fmt"
	"net/http"
	"sort"
)

const brokersEndpoint = "/v1/brokers"

// Broker is the information returned from the Redpanda admin broker endpoints.
type Broker struct {
	NodeID           int    `json:"node_id"`
	NumCores         int    `json:"num_cores"`
	MembershipStatus string `json:"membership_status"`
	IsAlive          *bool  `json:"is_alive"`
	Version          string `json:"version`
}

// Brokers queries one of the client's hosts and returns the list of brokers.
func (a *AdminAPI) Brokers() ([]Broker, error) {
	var bs []Broker
	defer func() {
		sort.Slice(bs, func(i, j int) bool { return bs[i].NodeID < bs[j].NodeID })
	}()
	return bs, a.sendAny(http.MethodGet, brokersEndpoint, nil, &bs)
}

// DecommissionBroker issues a decommission request for the given broker.
func (a *AdminAPI) DecommissionBroker(node int) error {
	return a.sendToLeader(
		http.MethodPut,
		fmt.Sprintf("%s/%d/decommission", brokersEndpoint, node),
		nil,
		nil,
	)
}

// RecommissionBroker issues a recommission request for the given broker.
func (a *AdminAPI) RecommissionBroker(node int) error {
	return a.sendToLeader(
		http.MethodPut,
		fmt.Sprintf("%s/%d/recommission", brokersEndpoint, node),
		nil,
		nil,
	)
}

// EnableMaintenanceMode enables maintenance mode for a node.
func (a *AdminAPI) EnableMaintenanceMode(nodeId int) error {
	return a.sendAny(
		http.MethodPut,
		fmt.Sprintf("%s/%d/maintenance", brokersEndpoint, nodeId),
		nil,
		nil,
	)
}

// DisableMaintenanceMode disables maintenance mode for a node.
func (a *AdminAPI) DisableMaintenanceMode(nodeId int) error {
	return a.sendAny(
		http.MethodDelete,
		fmt.Sprintf("%s/%d/maintenance", brokersEndpoint, nodeId),
		nil,
		nil,
	)
}
