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
const clusterViewEndpoint = "v1/cluster_view"

type MaintenanceStatus struct {
	Draining     bool `json:"draining"`
	Finished     bool `json:"finished"`
	Errors       bool `json:"errors"`
	Partitions   int  `json:"partitions"`
	Eligible     int  `json:"eligible"`
	Transferring int  `json:"transferring"`
	Failed       int  `json:"failed"`
}

type ClusterView struct {
	Version int      `json:"version"`
	Brokers []Broker `json:"brokers"`
}

// Broker is the information returned from the Redpanda admin broker endpoints.
type Broker struct {
	NodeID           int                `json:"node_id"`
	NumCores         int                `json:"num_cores"`
	MembershipStatus string             `json:"membership_status"`
	IsAlive          *bool              `json:"is_alive"`
	Version          string             `json:"version"`
	Maintenance      *MaintenanceStatus `json:"maintenance_status"`
}

// Brokers queries one of the client's hosts and returns the list of brokers.
func (a *AdminAPI) Brokers() ([]Broker, error) {
	var cv ClusterView
	defer func() {
		sort.Slice(cv.Brokers, func(i, j int) bool { return cv.Brokers[i].NodeID < cv.Brokers[j].NodeID }) //nolint:revive // return inside this deferred function is for the sort's less function
	}()
	err := a.sendAny(http.MethodGet, clusterViewEndpoint, nil, &cv)
	return cv.Brokers, err
}

// Broker queries one of the client's hosts and returns broker information.
func (a *AdminAPI) Broker(node int) (Broker, error) {
	var b Broker
	err := a.sendAny(
		http.MethodGet,
		fmt.Sprintf("%s/%d", brokersEndpoint, node), nil, &b)
	return b, err
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
func (a *AdminAPI) EnableMaintenanceMode(nodeID int) error {
	return a.sendAny(
		http.MethodPut,
		fmt.Sprintf("%s/%d/maintenance", brokersEndpoint, nodeID),
		nil,
		nil,
	)
}

// DisableMaintenanceMode disables maintenance mode for a node.
func (a *AdminAPI) DisableMaintenanceMode(nodeID int) error {
	return a.sendAny(
		http.MethodDelete,
		fmt.Sprintf("%s/%d/maintenance", brokersEndpoint, nodeID),
		nil,
		nil,
	)
}
