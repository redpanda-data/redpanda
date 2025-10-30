// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// Package connections deals with listing current connections in the cluster
package connections

import (
	"time"

	adminv2 "buf.build/gen/go/redpandadata/core/protocolbuffers/go/redpanda/core/admin/v2"
	dataplanev1 "buf.build/gen/go/redpandadata/dataplane/protocolbuffers/go/redpanda/api/dataplane/v1"
)

type GroupInfo struct {
	ID         string `json:"id" yaml:"id"`
	InstanceID string `json:"instance_id" yaml:"instance_id"`
	MemberID   string `json:"member_id" yaml:"member_id"`
}

// Connection represents a Kafka connection with custom formatting for output.
type Connection struct {
	NodeID               int32              `json:"node_id" yaml:"node_id"`
	ShardID              uint32             `json:"shard_id" yaml:"shard_id"`
	UID                  string             `json:"uid" yaml:"uid"`
	State                string             `json:"state" yaml:"state"`
	OpenTime             string             `json:"open_time" yaml:"open_time"`
	CloseTime            *string            `json:"close_time" yaml:"close_time"`
	ConnectionDuration   string             `json:"connection_duration" yaml:"connection_duration"`
	Authentication       *Authentication    `json:"authentication,omitempty" yaml:"authentication,omitempty"`
	TLSEnabled           bool               `json:"tls_enabled" yaml:"tls_enabled"`
	Client               *Client            `json:"client,omitempty" yaml:"client,omitempty"`
	Group                *GroupInfo         `json:"group" yaml:"group"`
	ListenerName         string             `json:"listener_name,omitempty" yaml:"listener_name,omitempty"`
	TransactionalID      string             `json:"transactional_id,omitempty" yaml:"transactional_id,omitempty"`
	APIVersions          []APIVersion       `json:"api_versions" yaml:"api_versions"`
	IdleDuration         string             `json:"idle_duration,omitempty" yaml:"idle_duration,omitempty"`
	ActiveRequests       *ActiveRequests    `json:"active_requests,omitempty" yaml:"active_requests,omitempty"`
	RequestStatisticsAll *RequestStatistics `json:"request_statistics_all,omitempty" yaml:"request_statistics_all,omitempty"`
	RequestStatistics1m  *RequestStatistics `json:"request_statistics_1m,omitempty" yaml:"request_statistics_1m,omitempty"`
}

// Authentication holds authentication information.
type Authentication struct {
	State         string `json:"state" yaml:"state"`
	Mechanism     string `json:"mechanism" yaml:"mechanism"`
	UserPrincipal string `json:"user_principal,omitempty" yaml:"user_principal,omitempty"`
}

// Client holds client connection information.
type Client struct {
	IP              string `json:"ip,omitempty" yaml:"ip,omitempty"`
	Port            uint32 `json:"port,omitempty" yaml:"port,omitempty"`
	ID              string `json:"id,omitempty" yaml:"id,omitempty"`
	SoftwareName    string `json:"software_name,omitempty" yaml:"software_name,omitempty"`
	SoftwareVersion string `json:"software_version,omitempty" yaml:"software_version,omitempty"`
}

// APIVersion represents a Kafka API and its version.
type APIVersion struct {
	API     string `json:"api" yaml:"api"`
	Version int32  `json:"version" yaml:"version"`
}

// ActiveRequests holds information about in-flight requests.
type ActiveRequests struct {
	SampledRequests []SampledRequest `json:"sampled_requests" yaml:"sampled_requests"`
	HasMoreRequests bool             `json:"has_more_requests" yaml:"has_more_requests"`
}

// SampledRequest represents a single in-flight request.
type SampledRequest struct {
	API      string `json:"api" yaml:"api"`
	Duration string `json:"duration" yaml:"duration"`
}

// RequestStatistics holds aggregated request statistics.
type RequestStatistics struct {
	ProduceBytes      uint64 `json:"produce_bytes" yaml:"produce_bytes"`
	FetchBytes        uint64 `json:"fetch_bytes" yaml:"fetch_bytes"`
	RequestCount      uint64 `json:"request_count" yaml:"request_count"`
	ProduceBatchCount uint64 `json:"produce_batch_count" yaml:"produce_batch_count"`
}

func parseDataplaneConnection(conn *dataplanev1.Connection) *Connection {
	opened := conn.OpenTime.AsTime().Format(time.RFC3339)
	closed := ""
	if conn.CloseTime != nil {
		closed = conn.CloseTime.AsTime().Format(time.RFC3339)
	}

	versions := make([]APIVersion, len(conn.ApiVersions))
	for i, ver := range conn.ApiVersions {
		versions[i] = APIVersion{API: ver.Api.String(), Version: ver.Version}
	}

	requests := make([]SampledRequest, len(conn.ActiveRequests.Requests))
	for i, req := range conn.ActiveRequests.Requests {
		requests[i] = SampledRequest{API: req.Api.String(), Duration: req.Duration.AsDuration().String()}
	}

	return &Connection{
		NodeID:             conn.NodeId,
		ShardID:            conn.ShardId,
		UID:                conn.Uid,
		OpenTime:           opened,
		CloseTime:          &closed,
		ConnectionDuration: getConnectionDuration(conn.OpenTime.AsTime(), conn.CloseTime.AsTime()),
		IdleDuration:       conn.IdleDuration.AsDuration().String(),
		State:              conn.State.String(),
		TLSEnabled:         conn.TlsEnabled,
		ListenerName:       conn.ListenerName,
		TransactionalID:    conn.TransactionalId,
		Authentication: &Authentication{
			State:         conn.Authentication.State.String(),
			Mechanism:     conn.Authentication.Mechanism.String(),
			UserPrincipal: conn.Authentication.UserPrincipal,
		},
		APIVersions: versions,
		Group: &GroupInfo{
			ID:         conn.Group.Id,
			MemberID:   conn.Group.MemberId,
			InstanceID: conn.Group.InstanceId,
		},
		Client: &Client{
			IP:              conn.Client.Ip,
			Port:            conn.Client.Port,
			ID:              conn.Client.Id,
			SoftwareName:    conn.Client.SoftwareName,
			SoftwareVersion: conn.Client.SoftwareVersion,
		},
		ActiveRequests: &ActiveRequests{
			SampledRequests: requests,
			HasMoreRequests: conn.ActiveRequests.HasMoreRequests,
		},
		RequestStatisticsAll: &RequestStatistics{
			ProduceBytes:      conn.RequestStatisticsAll.ProduceBytes,
			FetchBytes:        conn.RequestStatisticsAll.FetchBytes,
			RequestCount:      conn.RequestStatisticsAll.RequestCount,
			ProduceBatchCount: conn.RequestStatisticsAll.ProduceBatchCount,
		},
		RequestStatistics1m: &RequestStatistics{
			ProduceBytes:      conn.RequestStatistics_1M.ProduceBytes,
			FetchBytes:        conn.RequestStatistics_1M.FetchBytes,
			RequestCount:      conn.RequestStatistics_1M.RequestCount,
			ProduceBatchCount: conn.RequestStatistics_1M.ProduceBatchCount,
		},
	}
}

func parseConnection(conn *adminv2.KafkaConnection) *Connection {
	c := &Connection{
		NodeID:  conn.NodeId,
		ShardID: conn.ShardId,
		UID:     conn.Uid,
		State:   conn.State.String(),
	}

	// Add timestamps and duration
	if conn.OpenTime != nil {
		c.OpenTime = conn.OpenTime.AsTime().Format(time.RFC3339)
		if conn.CloseTime != nil {
			closeTime := conn.CloseTime.AsTime().Format(time.RFC3339)
			c.CloseTime = &closeTime
		}
		c.ConnectionDuration = getConnectionDuration(conn.OpenTime.AsTime(), conn.CloseTime.AsTime())
	}

	// Parse authentication info
	if conn.AuthenticationInfo != nil {
		c.Authentication = &Authentication{
			State:         conn.AuthenticationInfo.State.String(),
			Mechanism:     conn.AuthenticationInfo.Mechanism.String(),
			UserPrincipal: conn.AuthenticationInfo.UserPrincipal,
		}
	}

	c.TLSEnabled = conn.TlsInfo.Enabled

	// Parse client info
	if conn.Source != nil || conn.ClientId != "" || conn.ClientSoftwareName != "" || conn.ClientSoftwareVersion != "" {
		c.Client = &Client{
			ID:              conn.ClientId,
			SoftwareName:    conn.ClientSoftwareName,
			SoftwareVersion: conn.ClientSoftwareVersion,
		}
		if conn.Source != nil {
			c.Client.IP = conn.Source.IpAddress
			c.Client.Port = conn.Source.Port
		}
	}

	// Parse group ID
	c.Group = &GroupInfo{
		ID:         conn.GroupId,
		InstanceID: conn.GroupInstanceId,
		MemberID:   conn.GroupMemberId,
	}

	// Parse API versions
	c.APIVersions = []APIVersion{}
	for apiKey, version := range conn.ApiVersions {
		c.APIVersions = append(c.APIVersions, APIVersion{
			API:     mapAPIKey(apiKey).String(),
			Version: version,
		})
	}

	c.IdleDuration = conn.IdleDuration.AsDuration().String()
	c.ListenerName = conn.ListenerName
	c.TransactionalID = conn.TransactionalId

	// Parse in-flight requests
	c.ActiveRequests = &ActiveRequests{
		SampledRequests: []SampledRequest{},
		HasMoreRequests: conn.InFlightRequests.HasMoreRequests,
	}

	if conn.InFlightRequests != nil {
		if len(conn.InFlightRequests.SampledInFlightRequests) > 0 {
			c.ActiveRequests.SampledRequests = make([]SampledRequest, len(conn.InFlightRequests.SampledInFlightRequests))
			for i, req := range conn.InFlightRequests.SampledInFlightRequests {
				c.ActiveRequests.SampledRequests[i] = SampledRequest{
					API:      mapAPIKey(req.ApiKey).String(),
					Duration: req.InFlightDuration.AsDuration().String(),
				}
			}
		}
	}

	// Parse total statistics
	if conn.TotalRequestStatistics != nil {
		c.RequestStatisticsAll = &RequestStatistics{
			ProduceBytes:      conn.TotalRequestStatistics.ProduceBytes,
			FetchBytes:        conn.TotalRequestStatistics.FetchBytes,
			RequestCount:      conn.TotalRequestStatistics.RequestCount,
			ProduceBatchCount: conn.TotalRequestStatistics.ProduceBatchCount,
		}
	}

	// Parse recent statistics
	if conn.RecentRequestStatistics != nil {
		c.RequestStatistics1m = &RequestStatistics{
			ProduceBytes:      conn.RecentRequestStatistics.ProduceBytes,
			FetchBytes:        conn.RecentRequestStatistics.FetchBytes,
			RequestCount:      conn.RecentRequestStatistics.RequestCount,
			ProduceBatchCount: conn.RecentRequestStatistics.ProduceBatchCount,
		}
	}

	return c
}

// mapAPIKey converts a Kafka API key to its enum value.
func mapAPIKey(apiKey int32) dataplanev1.KafkaAPI {
	// Kafka API keys: https://kafka.apache.org/protocol.html#protocol_api_keys
	vals := map[int32]dataplanev1.KafkaAPI{
		0:  dataplanev1.KafkaAPI_KAFKA_API_PRODUCE,
		1:  dataplanev1.KafkaAPI_KAFKA_API_FETCH,
		2:  dataplanev1.KafkaAPI_KAFKA_API_OFFSETS,
		3:  dataplanev1.KafkaAPI_KAFKA_API_METADATA,
		4:  dataplanev1.KafkaAPI_KAFKA_API_LEADER_AND_ISR,
		5:  dataplanev1.KafkaAPI_KAFKA_API_STOP_REPLICA,
		6:  dataplanev1.KafkaAPI_KAFKA_API_UPDATE_METADATA,
		7:  dataplanev1.KafkaAPI_KAFKA_API_CONTROLLED_SHUTDOWN,
		8:  dataplanev1.KafkaAPI_KAFKA_API_OFFSET_COMMIT,
		9:  dataplanev1.KafkaAPI_KAFKA_API_OFFSET_FETCH,
		10: dataplanev1.KafkaAPI_KAFKA_API_GROUP_COORDINATOR,
		11: dataplanev1.KafkaAPI_KAFKA_API_JOIN_GROUP,
		12: dataplanev1.KafkaAPI_KAFKA_API_HEARTBEAT,
		13: dataplanev1.KafkaAPI_KAFKA_API_LEAVE_GROUP,
		14: dataplanev1.KafkaAPI_KAFKA_API_SYNC_GROUP,
		15: dataplanev1.KafkaAPI_KAFKA_API_DESCRIBE_GROUPS,
		16: dataplanev1.KafkaAPI_KAFKA_API_LIST_GROUPS,
		17: dataplanev1.KafkaAPI_KAFKA_API_SASL_HANDSHAKE,
		18: dataplanev1.KafkaAPI_KAFKA_API_API_VERSIONS,
		19: dataplanev1.KafkaAPI_KAFKA_API_CREATE_TOPICS,
		20: dataplanev1.KafkaAPI_KAFKA_API_DELETE_TOPICS,
	}

	if val, ok := vals[apiKey]; ok {
		return val
	}
	return dataplanev1.KafkaAPI_KAFKA_API_UNSPECIFIED
}
