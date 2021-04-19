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
	"bytes"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/kafka/mocks"
)

func TestNewCreateACLsCommand(t *testing.T) {
	tests := []struct {
		name           string
		args           []string
		admin          func() (sarama.ClusterAdmin, error)
		expectedOut    string
		expectedErrMsg string
	}{{
		name: "it should fail if getting the admin fails",
		args: []string{
			"--resource", "topic",
			"--allow-principal", "User:*",
		},
		admin: func() (sarama.ClusterAdmin, error) {
			return nil, errors.New("No admin for you")
		},
		expectedErrMsg: "No admin for you",
	}, {
		name: "it should fail if allowPrincipals and denyPrincipals are empty",
		args: []string{
			"--resource", "topic",
			"--resource-name", "test-topic",
			"name-pattern", "literal",
		},
		expectedErrMsg: "at least one of --allow-principal or --deny-principal must be set",
	}, {
		name: "it should fail if there is an invalid resource",
		args: []string{
			"--resource", "weird-made-up-resource",
			"--name-pattern", "literal",
			"--allow-principal", "Group:group-1",
		},
		expectedErrMsg: "invalid resource 'weird-made-up-resource'. Supported values: any, cluster, group, topic, transactionalid.",
	}, {
		name: "it should fail if there is an invalid operation",
		args: []string{
			"--resource", "topic",
			"--resource-name", "test-topic",
			"--name-pattern", "any",
			"--deny-principal", "User:user-1",
			"--operation", "read",
			"--operation", "describe",
			"--operation", "do-cool-stuff",
		},
		expectedErrMsg: "1 error occurred:\n\t* no acl operation with name do-cool-stuff\n\n",
	}, {
		name: "it should fail CreateAcl fails",
		args: []string{
			"--resource", "topic",
			"--resource-name", "test-topic",
			"--name-pattern", "any",
			"--deny-principal", "User:user-1",
			"--operation", "describe",
		},
		admin: func() (sarama.ClusterAdmin, error) {
			return &mocks.MockAdmin{
				MockCreateACL: func(_ sarama.Resource, _ sarama.Acl) error {
					return errors.New("Couldn't create yo' ACLs, yo")
				},
			}, nil
		},
		expectedErrMsg: "Couldn't create ACL for principal 'User:user-1' with host '*', operation 'Describe' and permission 'Deny': Couldn't create yo' ACLs, yo",
	}, {
		name: "it should log the created ACLs",
		args: []string{
			"--resource", "topic",
			"--resource-name", "test-topic",
			"--name-pattern", "any",
			"--deny-principal", "User:user-1",
			"--operation", "describe",
		},
		admin: func() (sarama.ClusterAdmin, error) {
			return &mocks.MockAdmin{
				MockCreateACL: func(_ sarama.Resource, _ sarama.Acl) error {
					return nil
				},
			}, nil
		},
		expectedOut: "Created ACL for principal 'User:user-1' with host '*', operation 'Describe' and permission 'Deny'",
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(st *testing.T) {
			admin := func() (sarama.ClusterAdmin, error) {
				return mocks.MockAdmin{}, nil
			}
			if tt.admin != nil {
				admin = tt.admin
			}
			var out bytes.Buffer
			logrus.SetOutput(&out)
			cmd := NewCreateACLsCommand(admin)
			cmd.SetArgs(tt.args)
			err := cmd.Execute()
			if tt.expectedErrMsg != "" {
				require.EqualError(st, err, tt.expectedErrMsg)
				return
			}
			require.NoError(st, err)
			if tt.expectedOut != "" {
				require.Contains(t, out.String(), tt.expectedOut)
			}
		})
	}
}

func TestCalculateACLFilters(t *testing.T) {
	tests := []struct {
		name            string
		resource        string
		resourceName    string
		namePattern     string
		operations      []string
		allowPrincipals []string
		allowHosts      []string
		denyPrincipals  []string
		denyHosts       []string
		expected        []*sarama.Acl
		expectedErrMsg  string
	}{{
		name:           "it should fail if allowPrincipals and denyPrincipals are empty",
		expectedErrMsg: "empty allow & deny principals list",
	}, {
		name:           "it should fail if there is an invalid operation",
		resource:       "topic",
		resourceName:   "test-topic",
		namePattern:    "any",
		denyPrincipals: []string{"User:user-1"},
		operations:     []string{"read", "describe", "do-cool-stuff"},
		expectedErrMsg: "1 error occurred:\n\t* no acl operation with name do-cool-stuff\n\n",
	}, {
		name:           "it should fail if there is an invalid operation",
		resource:       "topic",
		resourceName:   "test-topic",
		namePattern:    "any",
		denyPrincipals: []string{"User:user-1"},
		operations:     []string{"read", "describe", "do-cool-stuff"},
		expectedErrMsg: "1 error occurred:\n\t* no acl operation with name do-cool-stuff\n\n",
	}, {
		name:            "it should calculate the ACLs (1)",
		resource:        "topic",
		resourceName:    "test-topic",
		namePattern:     "any",
		allowPrincipals: []string{"User:user-1"},
		denyPrincipals:  []string{"User:user-2"},
		operations:      []string{"read", "describe"},
		expected: []*sarama.Acl{{
			Principal:      "User:user-1",
			Host:           "*",
			Operation:      sarama.AclOperationRead,
			PermissionType: sarama.AclPermissionAllow,
		}, {
			Principal:      "User:user-2",
			Host:           "*",
			Operation:      sarama.AclOperationRead,
			PermissionType: sarama.AclPermissionDeny,
		}, {
			Principal:      "User:user-1",
			Host:           "*",
			Operation:      sarama.AclOperationDescribe,
			PermissionType: sarama.AclPermissionAllow,
		}, {
			Principal:      "User:user-2",
			Host:           "*",
			Operation:      sarama.AclOperationDescribe,
			PermissionType: sarama.AclPermissionDeny,
		}},
	}, {
		name:            "it should calculate the ACLs (2)",
		resource:        "topic",
		resourceName:    "test-topic",
		namePattern:     "any",
		allowPrincipals: []string{"User:user-1", "User:user-2"},
		allowHosts:      []string{"172.35.65.152", "192.168.786.90"},
		operations:      []string{"read"},
		expected: []*sarama.Acl{{
			Principal:      "User:user-1",
			Host:           "172.35.65.152",
			Operation:      sarama.AclOperationRead,
			PermissionType: sarama.AclPermissionAllow,
		}, {
			Principal:      "User:user-1",
			Host:           "192.168.786.90",
			Operation:      sarama.AclOperationRead,
			PermissionType: sarama.AclPermissionAllow,
		}, {
			Principal:      "User:user-2",
			Host:           "172.35.65.152",
			Operation:      sarama.AclOperationRead,
			PermissionType: sarama.AclPermissionAllow,
		}, {
			Principal:      "User:user-2",
			Host:           "192.168.786.90",
			Operation:      sarama.AclOperationRead,
			PermissionType: sarama.AclPermissionAllow,
		}},
	}, {
		name:            "it should calculate the ACLs (2)",
		resource:        "topic",
		resourceName:    "test-topic",
		namePattern:     "any",
		allowPrincipals: []string{"User:user-1"},
		allowHosts:      []string{"172.35.65.152", "192.168.786.90"},
		// Shouldn't generate any "deny" ACLs since denyPrincipals is empty
		denyHosts:  []string{"172.35.65.152", "192.168.786.90"},
		operations: []string{"read"},
		expected: []*sarama.Acl{{
			Principal:      "User:user-1",
			Host:           "172.35.65.152",
			Operation:      sarama.AclOperationRead,
			PermissionType: sarama.AclPermissionAllow,
		}, {
			Principal:      "User:user-1",
			Host:           "192.168.786.90",
			Operation:      sarama.AclOperationRead,
			PermissionType: sarama.AclPermissionAllow,
		}},
	}, {
		name:            "it should calculate the ACLs (2)",
		resource:        "topic",
		resourceName:    "test-topic",
		namePattern:     "any",
		allowPrincipals: []string{"User:user-1"},
		denyPrincipals:  []string{"User:user-3"},
		allowHosts:      []string{"172.35.65.152", "192.168.786.90"},
		// Shouldn't generate any "deny" ACLs since denyPrincipals is empty
		denyHosts:  []string{"172.35.65.153", "192.168.786.91"},
		operations: []string{"read"},
		expected: []*sarama.Acl{{
			Principal:      "User:user-1",
			Host:           "172.35.65.152",
			Operation:      sarama.AclOperationRead,
			PermissionType: sarama.AclPermissionAllow,
		}, {
			Principal:      "User:user-1",
			Host:           "192.168.786.90",
			Operation:      sarama.AclOperationRead,
			PermissionType: sarama.AclPermissionAllow,
		}, {
			Principal:      "User:user-3",
			Host:           "172.35.65.153",
			Operation:      sarama.AclOperationRead,
			PermissionType: sarama.AclPermissionDeny,
		}, {
			Principal:      "User:user-3",
			Host:           "192.168.786.91",
			Operation:      sarama.AclOperationRead,
			PermissionType: sarama.AclPermissionDeny,
		}},
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(st *testing.T) {
			operations := []string{}
			if tt.operations != nil {
				operations = tt.operations
			}
			allowPrincipals := []string{}
			if tt.allowPrincipals != nil {
				allowPrincipals = tt.allowPrincipals
			}

			denyPrincipals := []string{}
			if tt.denyPrincipals != nil {
				denyPrincipals = tt.denyPrincipals
			}
			allowHosts := []string{}
			if tt.allowHosts != nil {
				allowHosts = tt.allowHosts
			}
			denyHosts := []string{}
			if tt.denyHosts != nil {
				denyHosts = tt.denyHosts
			}
			var out bytes.Buffer
			logrus.SetOutput(&out)
			acls, err := calculateACLs(
				operations,
				allowPrincipals,
				denyPrincipals,
				allowHosts,
				denyHosts,
			)
			if tt.expectedErrMsg != "" {
				require.EqualError(st, err, tt.expectedErrMsg)
				return
			}
			require.NoError(st, err)
			require.Exactly(st, tt.expected, acls)
		})
	}
}
