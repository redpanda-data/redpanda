// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package certmanager

const (
	kafkaAPI = "kafka"
	// OperatorClientCert cert name - used by kubernetes operator to call KafkaAPI
	OperatorClientCert = "operator-client"
	// UserClientCert cert name - used by redpanda clients using KafkaAPI
	UserClientCert = "user-client"
	// AdminClientCert cert name - used by redpanda clients using KafkaAPI
	AdminClientCert = "admin-client"
	// RedpandaNodeCert cert name - node certificate
	RedpandaNodeCert = "redpanda"
)
