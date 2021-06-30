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
	"errors"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/kafka/mocks"
)

func TestNewListACLsCommand(t *testing.T) {
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
			"--resource-name", "test-topic",
			"--operation", "describe",
		},
		admin: func() (sarama.ClusterAdmin, error) {
			return nil, errors.New("No admin for you")
		},
		expectedErrMsg: "No admin for you",
	}, {
		name:           "it should fail if there is an invalid resource",
		args:           []string{"--resource", "weird-made-up-resource"},
		expectedErrMsg: "no acl resource with name weird-made-up-resource",
	}, {
		name: "it should fail if there is an invalid operation",
		args: []string{
			"--resource", "topic",
			"--resource-name", "test-topic",
			"--operation", "read",
			"--operation", "write",
			"--operation", "do-cool-stuff",
		},
		expectedErrMsg: "1 error occurred:\n\t* no acl operation with name do-cool-stuff\n\n",
	}, {
		name: "it should fail if there is an invalid permission",
		args: []string{
			"--resource", "topic",
			"--resource-name", "test-topic",
			"--permission", "allow",
			"--permission", "whatever",
		},
		expectedErrMsg: "1 error occurred:\n\t* no acl permission with name whatever\n\n",
	}, {
		name: "it should fail if ListACLs fails",
		args: []string{
			"--resource", "cluster",
			"--operation", "write",
			"--permission", "deny",
			"--principal", "Group:group-2",
			"--host", "168.79.78.24",
		},
		admin: func() (sarama.ClusterAdmin, error) {
			return mocks.MockAdmin{
				MockListAcls: func(_ sarama.AclFilter) ([]sarama.ResourceAcls, error) {
					return nil, errors.New("Boom it failed ha")
				},
			}, nil
		},
		expectedErrMsg: "couldn't list ACLs with filter resource type: 'Cluster', name pattern type 'Any', operation 'Write', permission 'Deny', principal Group:group-2, host 168.79.78.24: Boom it failed ha",
	}, {
		name: "it should print the ACLs",
		// The inputs don't really matter since we're mocking the
		// result anyway. This case tests that the output is as expected.
		admin: func() (sarama.ClusterAdmin, error) {
			return &mocks.MockAdmin{
				MockListAcls: func(_ sarama.AclFilter) ([]sarama.ResourceAcls, error) {
					return []sarama.ResourceAcls{{
						Resource: sarama.Resource{
							ResourceType:        sarama.AclResourceTopic,
							ResourceName:        "some-cool-topic",
							ResourcePatternType: sarama.AclPatternAny,
						},
						Acls: []*sarama.Acl{{
							Principal:      "User:Lola",
							Host:           "127.0.0.1",
							Operation:      sarama.AclOperationAll,
							PermissionType: sarama.AclPermissionAllow,
						}, {
							Principal:      "User:Momo",
							Host:           "162.168.90.124",
							Operation:      sarama.AclOperationAny,
							PermissionType: sarama.AclPermissionDeny,
						}},
					}}, nil
				},
			}, nil
		},
		expectedOut: `  PRINCIPAL  HOST            OPERATION  PERMISSION TYPE  RESOURCE TYPE  RESOURCE NAME    RESOURCE PATTERN TYPE  
             
  User:Lola  127.0.0.1       All        Allow            Topic          some-cool-topic  Any                    
  User:Momo  162.168.90.124  Any        Deny             Topic          some-cool-topic  Any                    
             
`,
	}, {
		name: "it should print the ACLs",
		// The inputs don't really matter since we're mocking the
		// result anyway. This case tests that the resulting set of ACLs is
		// logged as expected.
		args: []string{"--resource", "topic"},
		admin: func() (sarama.ClusterAdmin, error) {
			return &mocks.MockAdmin{
				MockListAcls: func(_ sarama.AclFilter) ([]sarama.ResourceAcls, error) {
					return []sarama.ResourceAcls{}, nil
				},
			}, nil
		},
		expectedOut: "No ACLs found for the given filters.",
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(st *testing.T) {
			admin := func() (sarama.ClusterAdmin, error) {
				return mocks.MockAdmin{}, nil
			}
			if tt.admin != nil {
				admin = tt.admin
			}
			cmd := NewListACLsCommand(admin)
			cmd.SetArgs(tt.args)
			var out bytes.Buffer
			logrus.SetOutput(&out)
			err := cmd.Execute()
			if tt.expectedErrMsg != "" {
				require.EqualError(st, err, tt.expectedErrMsg)
				return
			}
			require.NoError(st, err)
			if tt.expectedOut != "" {
				require.Regexp(t, tt.expectedOut, out.String())
			}
		})
	}
}

func TestCalculateListACLFilters(t *testing.T) {
	tests := []struct {
		name           string
		resource       string
		resourceName   string
		namePattern    string
		operations     []string
		permissions    []string
		principals     []string
		hosts          []string
		expected       func() []*sarama.AclFilter
		expectedErrMsg string
	}{{
		name:           "it should fail if there is an invalid resource",
		resource:       "weird-made-up-resource",
		expectedErrMsg: "no acl resource with name weird-made-up-resource",
	}, {
		name:           "it should fail if there is an invalid operation",
		resource:       "topic",
		resourceName:   "test-topic",
		operations:     []string{"read", "write", "do-cool-stuff"},
		expectedErrMsg: "1 error occurred:\n\t* no acl operation with name do-cool-stuff\n\n",
	}, {
		name:           "it should fail if there is an invalid permission",
		resource:       "topic",
		resourceName:   "test-topic",
		permissions:    []string{"allow", "whatever"},
		expectedErrMsg: "1 error occurred:\n\t* no acl permission with name whatever\n\n",
	}, {
		name: "it should calculate the ACL filters (empty imputs)",
		expected: func() []*sarama.AclFilter {
			return []*sarama.AclFilter{{
				ResourceType:              sarama.AclResourceAny,
				ResourcePatternTypeFilter: sarama.AclPatternAny,
				Operation:                 sarama.AclOperationAny,
				PermissionType:            sarama.AclPermissionAny,
			}}
		},
	}, {
		name:         "it should calculate the ACL filters (mixed imputs 1)",
		resource:     "transactionalid",
		resourceName: "tx-id-2",
		operations:   []string{"read", "write"},
		permissions:  []string{"allow", "deny"},
		expected: func() []*sarama.AclFilter {
			name := "tx-id-2"
			return []*sarama.AclFilter{{
				ResourceType:              sarama.AclResourceTransactionalID,
				ResourceName:              &name,
				ResourcePatternTypeFilter: sarama.AclPatternAny,
				Operation:                 sarama.AclOperationRead,
				PermissionType:            sarama.AclPermissionAllow,
			}, {
				ResourceType:              sarama.AclResourceTransactionalID,
				ResourceName:              &name,
				ResourcePatternTypeFilter: sarama.AclPatternAny,
				Operation:                 sarama.AclOperationWrite,
				PermissionType:            sarama.AclPermissionAllow,
			}, {
				ResourceType:              sarama.AclResourceTransactionalID,
				ResourceName:              &name,
				ResourcePatternTypeFilter: sarama.AclPatternAny,
				Operation:                 sarama.AclOperationRead,
				PermissionType:            sarama.AclPermissionDeny,
			}, {
				ResourceType:              sarama.AclResourceTransactionalID,
				ResourceName:              &name,
				ResourcePatternTypeFilter: sarama.AclPatternAny,
				Operation:                 sarama.AclOperationWrite,
				PermissionType:            sarama.AclPermissionDeny,
			}}
		},
	}, {
		name:         "it should calculate the ACL filters (mixed imputs 2)",
		resource:     "topic",
		resourceName: "topic-3",
		namePattern:  "match",
		principals:   []string{"User:D", "User:B", "User:A"},
		operations:   []string{"read", "write", "alterconfigs"},
		permissions:  []string{"allow"},
		expected: func() []*sarama.AclFilter {
			name := "topic-3"
			ppal1 := "User:D"
			ppal2 := "User:B"
			ppal3 := "User:A"

			return []*sarama.AclFilter{{
				ResourceType:              sarama.AclResourceTopic,
				ResourceName:              &name,
				ResourcePatternTypeFilter: sarama.AclPatternMatch,
				Principal:                 &ppal1,
				Operation:                 sarama.AclOperationRead,
				PermissionType:            sarama.AclPermissionAllow,
			}, {
				ResourceType:              sarama.AclResourceTopic,
				ResourceName:              &name,
				ResourcePatternTypeFilter: sarama.AclPatternMatch,
				Principal:                 &ppal2,
				Operation:                 sarama.AclOperationRead,
				PermissionType:            sarama.AclPermissionAllow,
			}, {
				ResourceType:              sarama.AclResourceTopic,
				ResourceName:              &name,
				ResourcePatternTypeFilter: sarama.AclPatternMatch,
				Principal:                 &ppal3,
				Operation:                 sarama.AclOperationRead,
				PermissionType:            sarama.AclPermissionAllow,
			}, {
				ResourceType:              sarama.AclResourceTopic,
				ResourceName:              &name,
				ResourcePatternTypeFilter: sarama.AclPatternMatch,
				Principal:                 &ppal1,
				Operation:                 sarama.AclOperationWrite,
				PermissionType:            sarama.AclPermissionAllow,
			}, {
				ResourceType:              sarama.AclResourceTopic,
				ResourceName:              &name,
				ResourcePatternTypeFilter: sarama.AclPatternMatch,
				Principal:                 &ppal2,
				Operation:                 sarama.AclOperationWrite,
				PermissionType:            sarama.AclPermissionAllow,
			}, {
				ResourceType:              sarama.AclResourceTopic,
				ResourceName:              &name,
				ResourcePatternTypeFilter: sarama.AclPatternMatch,
				Principal:                 &ppal3,
				Operation:                 sarama.AclOperationWrite,
				PermissionType:            sarama.AclPermissionAllow,
			}, {
				ResourceType:              sarama.AclResourceTopic,
				ResourceName:              &name,
				ResourcePatternTypeFilter: sarama.AclPatternMatch,
				Principal:                 &ppal1,
				Operation:                 sarama.AclOperationAlterConfigs,
				PermissionType:            sarama.AclPermissionAllow,
			}, {
				ResourceType:              sarama.AclResourceTopic,
				ResourceName:              &name,
				ResourcePatternTypeFilter: sarama.AclPatternMatch,
				Principal:                 &ppal2,
				Operation:                 sarama.AclOperationAlterConfigs,
				PermissionType:            sarama.AclPermissionAllow,
			}, {
				ResourceType:              sarama.AclResourceTopic,
				ResourceName:              &name,
				ResourcePatternTypeFilter: sarama.AclPatternMatch,
				Principal:                 &ppal3,
				Operation:                 sarama.AclOperationAlterConfigs,
				PermissionType:            sarama.AclPermissionAllow,
			}}
		},
	}, {
		name:         "it should calculate the ACL filters (mixed imputs 3)",
		resource:     "group",
		resourceName: "asdfasdfasldfkjasdf2342134",
		namePattern:  "literal",
		operations:   []string{"describe", "delete"},
		permissions:  []string{"any"},
		hosts:        []string{"127.0.0.1", "localhost"},
		expected: func() []*sarama.AclFilter {
			name := "asdfasdfasldfkjasdf2342134"
			host1 := "127.0.0.1"
			host2 := "localhost"
			return []*sarama.AclFilter{{
				ResourceType:              sarama.AclResourceGroup,
				ResourceName:              &name,
				ResourcePatternTypeFilter: sarama.AclPatternLiteral,
				Host:                      &host1,
				Operation:                 sarama.AclOperationDescribe,
				PermissionType:            sarama.AclPermissionAny,
			}, {
				ResourceType:              sarama.AclResourceGroup,
				ResourceName:              &name,
				ResourcePatternTypeFilter: sarama.AclPatternLiteral,
				Host:                      &host2,
				Operation:                 sarama.AclOperationDescribe,
				PermissionType:            sarama.AclPermissionAny,
			}, {
				ResourceType:              sarama.AclResourceGroup,
				ResourceName:              &name,
				ResourcePatternTypeFilter: sarama.AclPatternLiteral,
				Host:                      &host1,
				Operation:                 sarama.AclOperationDelete,
				PermissionType:            sarama.AclPermissionAny,
			}, {
				ResourceType:              sarama.AclResourceGroup,
				ResourceName:              &name,
				ResourcePatternTypeFilter: sarama.AclPatternLiteral,
				Host:                      &host2,
				Operation:                 sarama.AclOperationDelete,
				PermissionType:            sarama.AclPermissionAny,
			}}
		},
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(st *testing.T) {
			operations := []string{}
			if tt.operations != nil {
				operations = tt.operations
			}
			permissions := []string{}
			if tt.permissions != nil {
				permissions = tt.permissions
			}
			principals := []string{}
			if tt.principals != nil {
				principals = tt.principals
			}
			hosts := []string{}
			if tt.hosts != nil {
				hosts = tt.hosts
			}
			filters, err := calculateListACLFilters(
				tt.resource,
				tt.resourceName,
				tt.namePattern,
				operations,
				permissions,
				principals,
				hosts,
			)
			if tt.expectedErrMsg != "" {
				require.EqualError(st, err, tt.expectedErrMsg)
				return
			}
			require.NoError(st, err)
			require.Exactly(st, tt.expected(), filters)
		})
	}
}
