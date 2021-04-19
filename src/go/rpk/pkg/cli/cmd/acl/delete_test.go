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

func TestNewDeleteACLsCommand(t *testing.T) {
	tests := []struct {
		name           string
		args           []string
		admin          func() (sarama.ClusterAdmin, error)
		expectedOut    string
		expectedErrMsg string
	}{{
		name: "it should fail if there is an invalid resource",
		args: []string{
			"--resource", "weird-made-up-resource",
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
			"--operation", "read,describe,do-cool-stuff",
		},
		expectedErrMsg: "1 error occurred:\n\t* no acl operation with name do-cool-stuff\n\n",
	}, {
		name: "it should fail if getting the admin fails",
		args: []string{
			"--resource", "topic",
			"--resource-name", "test-topic",
			"--deny-principal", "User:user-1",
			"--operation", "describe",
		},
		admin: func() (sarama.ClusterAdmin, error) {
			return nil, errors.New("No admin for you")
		},
		expectedErrMsg: "No admin for you",
	}, {
		name: "it should fail if DeleteAcl fails",
		args: []string{
			"--resource", "topic",
			"--resource-name", "test-topic",
			"--deny-principal", "User:user-1",
			"--operation", "describe",
		},
		admin: func() (sarama.ClusterAdmin, error) {
			return &mocks.MockAdmin{
				MockDeleteACL: func(_ sarama.AclFilter, _ bool) ([]sarama.MatchingAcl, error) {
					return nil, errors.New("Couldn't create yo' ACLs, yo")
				},
			}, nil
		},
		expectedErrMsg: "couldn't list ACLs with filter resource type: 'Topic', name pattern type 'Literal', operation 'Describe', permission 'Deny', resource name test-topic, principal User:user-1: Couldn't create yo' ACLs, yo",
	}, {
		name: "it should show the deleted ACLs",
		// The inputs don't really matter in this test case since the response is mocked.
		args: []string{
			"--resource", "topic",
			"--resource-name", "test-topic",
			"--deny-principal", "User:user-1",
			"--deny-principal", "User:user-2",
			"--operation", "describe",
		},
		admin: func() (sarama.ClusterAdmin, error) {
			return &mocks.MockAdmin{
				MockDeleteACL: func(filter sarama.AclFilter, _ bool) ([]sarama.MatchingAcl, error) {
					ppal1 := "User:user-1"
					ppal2 := "User:user-2"
					acl1 := sarama.MatchingAcl{
						Resource: sarama.Resource{
							ResourceType:        sarama.AclResourceTopic,
							ResourceName:        "some-cool-topic",
							ResourcePatternType: sarama.AclPatternAny,
						},
						Acl: sarama.Acl{
							Principal:      ppal1,
							Host:           "*",
							Operation:      sarama.AclOperationRead,
							PermissionType: sarama.AclPermissionAllow,
						},
					}
					acl2 := sarama.MatchingAcl{
						Resource: sarama.Resource{
							ResourceType:        sarama.AclResourceTopic,
							ResourceName:        "some-cool-topic-2",
							ResourcePatternType: sarama.AclPatternAny,
						},
						Acl: sarama.Acl{
							Principal:      ppal2,
							Host:           "127.0.0.1",
							Operation:      sarama.AclOperationAlter,
							PermissionType: sarama.AclPermissionDeny,
						},
					}
					if filter.Principal != nil && *filter.Principal == ppal1 {
						return []sarama.MatchingAcl{acl1}, nil
					}
					return []sarama.MatchingAcl{acl2}, nil
				},
			}, nil
		},
		expectedOut: `  DELETED  PRINCIPAL    HOST       OPERATION  PERMISSION TYPE  RESOURCE TYPE  RESOURCE NAME      ERROR MESSAGE  
           
  yes      User:user-2  127.0.0.1  Alter      Deny             Topic          some-cool-topic-2  None           
  yes      User:user-1  *          Read       Allow            Topic          some-cool-topic    None           
           
`,
	}, {
		name: "it should show the deleted & failed ACLs",
		// The inputs don't really matter in this test case since the response is mocked.
		args: []string{
			"--resource", "topic",
			"--resource-name", "test-topic",
			"--deny-principal", "User:user-1",
			"--deny-principal", "User:user-2",
			"--operation", "describe",
		},
		admin: func() (sarama.ClusterAdmin, error) {
			return &mocks.MockAdmin{
				MockDeleteACL: func(filter sarama.AclFilter, _ bool) ([]sarama.MatchingAcl, error) {
					ppal1 := "User:user-1"
					acl1 := sarama.MatchingAcl{
						Resource: sarama.Resource{
							ResourceType:        sarama.AclResourceTopic,
							ResourceName:        "some-cool-topic",
							ResourcePatternType: sarama.AclPatternAny,
						},
						Acl: sarama.Acl{
							Principal:      ppal1,
							Host:           "*",
							Operation:      sarama.AclOperationRead,
							PermissionType: sarama.AclPermissionAllow,
						},
					}
					errMsg := "something went wrong!"
					acl2 := sarama.MatchingAcl{ErrMsg: &errMsg}
					if filter.Principal != nil && *filter.Principal == ppal1 {
						return []sarama.MatchingAcl{acl1}, nil
					}
					return []sarama.MatchingAcl{acl2}, nil
				},
			}, nil
		},
		expectedOut: `  DELETED  PRINCIPAL    HOST  OPERATION  PERMISSION TYPE  RESOURCE TYPE  RESOURCE NAME    ERROR MESSAGE          
           
  no                          Unknown    Unknown          Unknown                         something went wrong!  
  yes      User:user-1  *     Read       Allow            Topic          some-cool-topic  None                   
           
`,
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
			cmd := NewDeleteACLsCommand(admin)
			cmd.SetOut(&out)
			cmd.SetArgs(tt.args)
			err := cmd.Execute()
			if tt.expectedErrMsg != "" {
				require.EqualError(st, err, tt.expectedErrMsg)
				return
			}
			require.NoError(st, err)
			if tt.expectedOut != "" {
				require.Equal(t, tt.expectedOut, out.String())
			}
		})
	}
}

func TestCalculateDeleteACLFilters(t *testing.T) {
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
		expected        func() []*sarama.AclFilter
		expectedErrMsg  string
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
		name:            "it should calculate the ACL filters (1)",
		resource:        "transactionalid",
		resourceName:    "id-2",
		operations:      []string{"read", "alter"},
		allowPrincipals: []string{"User:Bob"},
		allowHosts:      []string{"168.168.245.3"},
		expected: func() []*sarama.AclFilter {
			name := "id-2"
			host := "168.168.245.3"
			allowPpal := "User:Bob"
			return []*sarama.AclFilter{{
				ResourceType:              sarama.AclResourceTransactionalID,
				ResourceName:              &name,
				ResourcePatternTypeFilter: sarama.AclPatternAny,
				Principal:                 &allowPpal,
				Host:                      &host,
				Operation:                 sarama.AclOperationRead,
				PermissionType:            sarama.AclPermissionAllow,
			}, {
				ResourceType:              sarama.AclResourceTransactionalID,
				ResourceName:              &name,
				ResourcePatternTypeFilter: sarama.AclPatternAny,
				Principal:                 &allowPpal,
				Host:                      &host,
				Operation:                 sarama.AclOperationAlter,
				PermissionType:            sarama.AclPermissionAllow,
			}}
		},
	}, {
		name:           "it should calculate the ACL filters (2)",
		resource:       "topic",
		resourceName:   "topic-1",
		namePattern:    "match",
		operations:     []string{"read", "alter"},
		denyPrincipals: []string{"User:Bob", "Group:gr-0123"},
		denyHosts:      []string{"168.168.245.3"},
		expected: func() []*sarama.AclFilter {
			name := "topic-1"
			host := "168.168.245.3"
			denyPpal1 := "User:Bob"
			denyPpal2 := "Group:gr-0123"
			return []*sarama.AclFilter{{
				ResourceType:              sarama.AclResourceTopic,
				ResourceName:              &name,
				ResourcePatternTypeFilter: sarama.AclPatternMatch,
				Principal:                 &denyPpal1,
				Host:                      &host,
				Operation:                 sarama.AclOperationRead,
				PermissionType:            sarama.AclPermissionDeny,
			}, {
				ResourceType:              sarama.AclResourceTopic,
				ResourceName:              &name,
				ResourcePatternTypeFilter: sarama.AclPatternMatch,
				Principal:                 &denyPpal2,
				Host:                      &host,
				Operation:                 sarama.AclOperationRead,
				PermissionType:            sarama.AclPermissionDeny,
			}, {
				ResourceType:              sarama.AclResourceTopic,
				ResourceName:              &name,
				ResourcePatternTypeFilter: sarama.AclPatternMatch,
				Principal:                 &denyPpal1,
				Host:                      &host,
				Operation:                 sarama.AclOperationAlter,
				PermissionType:            sarama.AclPermissionDeny,
			}, {
				ResourceType:              sarama.AclResourceTopic,
				ResourceName:              &name,
				ResourcePatternTypeFilter: sarama.AclPatternMatch,
				Principal:                 &denyPpal2,
				Host:                      &host,
				Operation:                 sarama.AclOperationAlter,
				PermissionType:            sarama.AclPermissionDeny,
			}}
		},
	}, {
		name:            "it should calculate the ACL filters (3)",
		resource:        "topic",
		operations:      []string{"describe"},
		allowPrincipals: []string{"Group:0", "Group:1"},
		denyPrincipals:  []string{"User:Bob", "Group:gr-0123"},
		denyHosts:       []string{"168.168.245.3", "127.0.0.1"},
		expected: func() []*sarama.AclFilter {
			allowPpal1, allowPpal2 := "Group:0", "Group:1"
			denyPpal1, denyPpal2 := "User:Bob", "Group:gr-0123"
			denyHost1, denyHost2 := "168.168.245.3", "127.0.0.1"
			return []*sarama.AclFilter{{
				ResourceType:              sarama.AclResourceTopic,
				ResourcePatternTypeFilter: sarama.AclPatternAny,
				Principal:                 &allowPpal1,
				Operation:                 sarama.AclOperationDescribe,
				PermissionType:            sarama.AclPermissionAllow,
			}, {
				ResourceType:              sarama.AclResourceTopic,
				ResourcePatternTypeFilter: sarama.AclPatternAny,
				Principal:                 &allowPpal2,
				Operation:                 sarama.AclOperationDescribe,
				PermissionType:            sarama.AclPermissionAllow,
			}, {
				ResourceType:              sarama.AclResourceTopic,
				ResourcePatternTypeFilter: sarama.AclPatternAny,
				Principal:                 &denyPpal1,
				Host:                      &denyHost1,
				Operation:                 sarama.AclOperationDescribe,
				PermissionType:            sarama.AclPermissionDeny,
			}, {
				ResourceType:              sarama.AclResourceTopic,
				ResourcePatternTypeFilter: sarama.AclPatternAny,
				Principal:                 &denyPpal1,
				Host:                      &denyHost2,
				Operation:                 sarama.AclOperationDescribe,
				PermissionType:            sarama.AclPermissionDeny,
			}, {
				ResourceType:              sarama.AclResourceTopic,
				ResourcePatternTypeFilter: sarama.AclPatternAny,
				Principal:                 &denyPpal2,
				Host:                      &denyHost1,
				Operation:                 sarama.AclOperationDescribe,
				PermissionType:            sarama.AclPermissionDeny,
			}, {
				ResourceType:              sarama.AclResourceTopic,
				ResourcePatternTypeFilter: sarama.AclPatternAny,
				Principal:                 &denyPpal2,
				Host:                      &denyHost2,
				Operation:                 sarama.AclOperationDescribe,
				PermissionType:            sarama.AclPermissionDeny,
			}}
		},
	}, {
		name:       "it should calculate the ACL filters (4)",
		resource:   "topic",
		operations: []string{"describe"},
		denyHosts:  []string{"168.168.245.3", "127.0.0.1"},
		expected: func() []*sarama.AclFilter {
			return []*sarama.AclFilter{}
		},
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
			filters, err := calculateDeleteACLFilters(
				tt.resource,
				tt.resourceName,
				tt.namePattern,
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
			require.Exactly(st, tt.expected(), filters)
		})
	}
}
