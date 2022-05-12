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

const (
	brokersEndpoint = "/v1/brokers"
	brokerEndpoint  = "/v1/brokers/%d"
)

type MaintenanceStatus struct {
	Draining     bool `json:"draining"`
	Finished     bool `json:"finished"`
	Errors       bool `json:"errors"`
	Partitions   int  `json:"partitions"`
	Eligible     int  `json:"eligible"`
	Transferring int  `json:"transferring"`
	Failed       int  `json:"failed"`
}

type dSpace struct {
	Path	string	`json:"path"`
	Free	int64	`json:"free"`
	Total	int64	`json:"total"`
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
	var bs []Broker
	defer func() {
		sort.Slice(bs, func(i, j int) bool { return bs[i].NodeID < bs[j].NodeID }) //nolint:revive // return inside this deferred function is for the sort's less function
	}()
	return bs, a.sendAny(http.MethodGet, brokersEndpoint, nil, &bs)
}

// Broker queries one of the client's hosts and returns broker information.
func (a *AdminAPI) Broker(node int) (Broker, error) {
	var b Broker
	err := a.sendAny(
		http.MethodGet,
		fmt.Sprintf(brokerEndpoint, node), nil, &b)
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
