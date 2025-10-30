// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package connections

import (
	"testing"
	"time"

	adminv2 "buf.build/gen/go/redpandadata/core/protocolbuffers/go/redpanda/core/admin/v2"
	dataplanev1 "buf.build/gen/go/redpandadata/dataplane/protocolbuffers/go/redpanda/api/dataplane/v1"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	closeTime = "2025-10-23T01:02:05Z"
	expected  = &Connection{
		NodeID:             2,
		ShardID:            4,
		UID:                "36338ca5-86b7-4478-ad23-32d49cfaef61",
		State:              "KAFKA_CONNECTION_STATE_OPEN",
		OpenTime:           "2025-10-23T01:02:03Z",
		CloseTime:          &closeTime,
		ConnectionDuration: "2s",
		TLSEnabled:         true,
		IdleDuration:       "100ms",
		ListenerName:       "external",
		TransactionalID:    "trans-id",
		Group: &GroupInfo{
			ID:         "group-a",
			MemberID:   "group-member",
			InstanceID: "group-instance",
		},
		Authentication: &Authentication{
			State:         "AUTHENTICATION_STATE_SUCCESS",
			Mechanism:     "AUTHENTICATION_MECHANISM_MTLS",
			UserPrincipal: "someone",
		},
		Client: &Client{
			IP:              "4.2.2.1",
			Port:            49722,
			ID:              "a-unique-client-id",
			SoftwareName:    "some-library",
			SoftwareVersion: "v0.0.1",
		},
		APIVersions: []APIVersion{{API: dataplanev1.KafkaAPI_KAFKA_API_PRODUCE.String(), Version: 4}},
		ActiveRequests: &ActiveRequests{
			SampledRequests: []SampledRequest{
				{API: dataplanev1.KafkaAPI_KAFKA_API_PRODUCE.String(), Duration: "40ms"},
			},
			HasMoreRequests: true,
		},
		RequestStatisticsAll: &RequestStatistics{
			ProduceBytes:      10000,
			FetchBytes:        2000,
			RequestCount:      200,
			ProduceBatchCount: 10,
		},
		RequestStatistics1m: &RequestStatistics{
			ProduceBytes:      1000,
			FetchBytes:        200,
			RequestCount:      20,
			ProduceBatchCount: 1,
		},
	}
)

func TestParseConnection(t *testing.T) {
	protoConn := &adminv2.KafkaConnection{
		NodeId:    2,
		ShardId:   4,
		Uid:       "36338ca5-86b7-4478-ad23-32d49cfaef61",
		State:     adminv2.KafkaConnectionState_KAFKA_CONNECTION_STATE_OPEN,
		OpenTime:  timestamppb.New(time.Date(2025, 10, 23, 1, 2, 3, 0, time.UTC)),
		CloseTime: timestamppb.New(time.Date(2025, 10, 23, 1, 2, 5, 0, time.UTC)),
		AuthenticationInfo: &adminv2.AuthenticationInfo{
			State:         adminv2.AuthenticationState_AUTHENTICATION_STATE_SUCCESS,
			Mechanism:     adminv2.AuthenticationMechanism_AUTHENTICATION_MECHANISM_MTLS,
			UserPrincipal: "someone",
		},
		TlsInfo: &adminv2.TLSInfo{
			Enabled: true,
		},
		ListenerName: "external",
		Source: &adminv2.Source{
			IpAddress: "4.2.2.1",
			Port:      49722,
		},
		ClientId:              "a-unique-client-id",
		ClientSoftwareName:    "some-library",
		ClientSoftwareVersion: "v0.0.1",
		GroupId:               "group-a",
		GroupInstanceId:       "group-instance",
		GroupMemberId:         "group-member",
		ApiVersions:           map[int32]int32{0: 4},
		IdleDuration:          durationpb.New(100 * time.Millisecond),
		TransactionalId:       "trans-id",
		InFlightRequests: &adminv2.InFlightRequests{
			SampledInFlightRequests: []*adminv2.InFlightRequests_Request{
				{
					ApiKey:           0, // PRODUCE
					InFlightDuration: durationpb.New(40 * time.Millisecond),
				},
			},
			HasMoreRequests: true,
		},
		TotalRequestStatistics: &adminv2.RequestStatistics{
			ProduceBytes:      10000,
			FetchBytes:        2000,
			RequestCount:      200,
			ProduceBatchCount: 10,
		},
		RecentRequestStatistics: &adminv2.RequestStatistics{
			ProduceBytes:      1000,
			FetchBytes:        200,
			RequestCount:      20,
			ProduceBatchCount: 1,
		},
	}

	require.Equal(t, expected, parseConnection(protoConn))
}

func TestParseDataplaneConnection(t *testing.T) {
	require.Equal(t, expected, parseDataplaneConnection(&dataplanev1.Connection{
		NodeId:    2,
		ShardId:   4,
		Uid:       "36338ca5-86b7-4478-ad23-32d49cfaef61",
		State:     adminv2.KafkaConnectionState_KAFKA_CONNECTION_STATE_OPEN,
		OpenTime:  timestamppb.New(time.Date(2025, 10, 23, 1, 2, 3, 0, time.UTC)),
		CloseTime: timestamppb.New(time.Date(2025, 10, 23, 1, 2, 5, 0, time.UTC)),
		Authentication: &adminv2.AuthenticationInfo{
			State:         adminv2.AuthenticationState_AUTHENTICATION_STATE_SUCCESS,
			Mechanism:     adminv2.AuthenticationMechanism_AUTHENTICATION_MECHANISM_MTLS,
			UserPrincipal: "someone",
		},
		TlsEnabled:   true,
		ListenerName: "external",
		Client: &dataplanev1.ConnectionClient{
			Ip:              "4.2.2.1",
			Port:            49722,
			Id:              "a-unique-client-id",
			SoftwareName:    "some-library",
			SoftwareVersion: "v0.0.1",
		},
		Group: &dataplanev1.GroupInfo{
			Id:         "group-a",
			InstanceId: "group-instance",
			MemberId:   "group-member",
		},
		ApiVersions: []*dataplanev1.APIVersion{
			{Api: dataplanev1.KafkaAPI_KAFKA_API_PRODUCE, Version: 4},
		},
		IdleDuration:    durationpb.New(100 * time.Millisecond),
		TransactionalId: "trans-id",
		ActiveRequests: &dataplanev1.ActiveRequests{
			Requests: []*dataplanev1.ActiveRequests_Request{
				{Api: dataplanev1.KafkaAPI_KAFKA_API_PRODUCE, Duration: durationpb.New(40 * time.Millisecond)},
			},
			HasMoreRequests: true,
		},
		RequestStatisticsAll: &adminv2.RequestStatistics{
			ProduceBytes:      10000,
			FetchBytes:        2000,
			RequestCount:      200,
			ProduceBatchCount: 10,
		},
		RequestStatistics_1M: &adminv2.RequestStatistics{
			ProduceBytes:      1000,
			FetchBytes:        200,
			RequestCount:      20,
			ProduceBatchCount: 1,
		},
	}))
}
