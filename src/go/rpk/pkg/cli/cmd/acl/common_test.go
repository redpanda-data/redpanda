// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package acl

import (
	"testing"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/require"
)

func TestParseOperations(t *testing.T) {
	tests := []struct {
		name           string
		operations     []string
		expected       []sarama.AclOperation
		expectedErrMsg string
	}{{
		name:       "it should parse valid operations",
		operations: supportedOperations(),
		expected: []sarama.AclOperation{
			sarama.AclOperationAny,
			sarama.AclOperationAll,
			sarama.AclOperationRead,
			sarama.AclOperationWrite,
			sarama.AclOperationCreate,
			sarama.AclOperationDelete,
			sarama.AclOperationAlter,
			sarama.AclOperationDescribe,
			sarama.AclOperationClusterAction,
			sarama.AclOperationDescribeConfigs,
			sarama.AclOperationAlterConfigs,
			sarama.AclOperationIdempotentWrite,
		},
	}, {
		name:           "it should fail if there's an invalid operation",
		operations:     []string{"write", "describe", "mycatiswalkingonmykeyboard"},
		expectedErrMsg: "1 error occurred:\n\t* no acl operation with name mycatiswalkingonmykeyboard\n\n",
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(st *testing.T) {
			res, err := parseOperations(tt.operations)
			if tt.expectedErrMsg != "" {
				require.EqualError(st, err, tt.expectedErrMsg)
				return
			}
			require.Exactly(st, tt.expected, res)
		})
	}
}

func TestParsePermissions(t *testing.T) {
	tests := []struct {
		name           string
		permissions    []string
		expected       []sarama.AclPermissionType
		expectedErrMsg string
	}{{
		name:        "it should parse valid permissions",
		permissions: supportedPermissions(),
		expected: []sarama.AclPermissionType{
			sarama.AclPermissionAny,
			sarama.AclPermissionDeny,
			sarama.AclPermissionAllow,
		},
	}, {
		name:           "it should fail if there's an invalid permission",
		permissions:    []string{"allow", "any", "squirrel"},
		expectedErrMsg: "1 error occurred:\n\t* no acl permission with name squirrel\n\n",
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(st *testing.T) {
			res, err := parsePermissions(tt.permissions)
			if tt.expectedErrMsg != "" {
				require.EqualError(st, err, tt.expectedErrMsg)
				return
			}
			require.Exactly(st, tt.expected, res)
		})
	}
}
