// Copyright 2021 Redpanda Data, Inc.
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

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestBackCompatList(t *testing.T) {
	t.Run("incompatible combo", func(t *testing.T) {
		var a acls
		for _, newFlag := range []*[]string{
			&a.allowPrincipals,
			&a.allowHosts,
			&a.denyPrincipals,
			&a.denyHosts,
		} {
			for _, oldFlag := range []*[]string{
				&a.listPermissions,
				&a.listPrincipals,
				&a.listHosts,
			} {
				err := a.backcompatList()
				require.NoError(t, err, "unexpected error when no new / old flags are specified")
				*newFlag = append(*newFlag, "foo")
				err = a.backcompatList()
				require.NoError(t, err, "unexpected error when only new flag specified")

				*oldFlag = append(*oldFlag, "bar")

				err = a.backcompatList()
				require.Error(t, err, "expected error when both new and old flags are specified")
				a = acls{} // reset slices
			}
		}
	})

	t.Run("permission parsing", func(t *testing.T) {
		for _, test := range []struct {
			val string
			ok  bool
		}{
			{"ALLOW", true},
			{"DeNy", true},
			{"unknown", false},
			{"", false},
		} {
			a := acls{listPermissions: []string{test.val}}
			err := a.backcompatList()
			gotOk := err == nil
			require.Equal(t, test.ok, gotOk, "permission parsing mismatch!")
		}
	})

	// Now we test the actual backwards compatibility
	for _, test := range []struct {
		name string

		perms []string
		princ []string
		hosts []string

		exp acls
	}{
		{
			name: "unspecified old flags means match everything",
			exp:  acls{},
		},

		{
			name:  "allow perms, nothing else matches allow all",
			perms: []string{"allow"},
			exp:   acls{},
		},

		{
			name:  "allow & deny perms, some principals, no hosts",
			perms: []string{"allow", "deny"},
			princ: []string{"some", "principals"},
			exp: acls{
				allowPrincipals: []string{"some", "principals"},
				denyPrincipals:  []string{"some", "principals"},
			},
		},

		{
			name:  "deny perms, everything else",
			perms: []string{"deny"},
			princ: []string{"some", "principals"},
			hosts: []string{"hosts"},
			exp: acls{
				denyPrincipals: []string{"some", "principals"},
				denyHosts:      []string{"hosts"},
			},
		},

		//
	} {
		t.Run(test.name, func(t *testing.T) {
			a := acls{
				listPermissions: test.perms,
				listPrincipals:  test.princ,
				listHosts:       test.hosts,
			}

			err := a.backcompatList()
			require.NoError(t, err, "unexpected error")
			require.Equal(t, test.exp, a, "unexpected differences")
		})
	}
}

func TestBackcompat(t *testing.T) {
	for _, test := range []struct {
		name   string
		asList bool
		in     acls
		exp    acls
		expErr bool
	}{
		{
			name: "nothing old changes nothing",
		},

		{
			name:   "as list runs backcompat list logic, defaulting any",
			asList: true,
			exp:    acls{},
		},

		{
			name: "old resource pattern overrides new",
			in: acls{
				oldResourcePatternType: "foo",
				resourcePatternType:    "bar",
			},
			exp: acls{
				oldResourcePatternType: "foo",
				resourcePatternType:    "foo",
			},
		},

		{
			name: "resource type cluster sets kafka-cluster",
			in: acls{
				resourceType: "cluster",
			},
			exp: acls{
				resourceType: "cluster",
				resourceName: kafkaCluster,
				cluster:      true,
			},
		},

		{
			name: "resource type cluster sets kafka-cluster",
			in: acls{
				resourceType: "cluster",
			},
			exp: acls{
				resourceType: "cluster",
				resourceName: kafkaCluster,
				cluster:      true,
			},
		},

		{
			name: "resource type cluster name kafka-cluster is ok",
			in: acls{
				resourceType: "cluster",
				resourceName: kafkaCluster,
			},
			exp: acls{
				resourceType: "cluster",
				resourceName: kafkaCluster,
				cluster:      true,
			},
		},

		{
			name: "resource type topic added appropriately",
			in: acls{
				resourceType: "topic",
				resourceName: "name",
			},
			exp: acls{
				resourceType: "topic",
				resourceName: "name",
				topics:       []string{"name"},
			},
		},

		{
			name: "resource type transactionalID added appropriately",
			in: acls{
				resourceType: "trans.action_al-ID",
				resourceName: "name",
			},
			exp: acls{
				resourceType: "trans.action_al-ID",
				resourceName: "name",
				txnIDs:       []string{"name"},
			},
		},

		{
			name: "resource type group added appropriately",
			in: acls{
				resourceType: "group",
				resourceName: "g",
			},
			exp: acls{
				resourceType: "group",
				resourceName: "g",
				groups:       []string{"g"},
			},
		},

		{
			name: "empty name fails",
			in: acls{
				resourceType: "topic",
			},
			expErr: true,
		},

		{
			name: "invalid type fails",
			in: acls{
				resourceType: "invalid",
				resourceName: "asdf",
			},
			expErr: true,
		},

		{
			name: "resource type cluster invalid name fails",
			in: acls{
				resourceType: "cluster",
				resourceName: "foo",
			},
			expErr: true,
		},

		//
	} {
		t.Run(test.name, func(t *testing.T) {
			err := test.in.backcompat(test.asList)
			gotErr := err != nil
			require.Equal(t, test.expErr, gotErr, "error mismatch, got: %v, exp? %v", err, test.expErr)
			if test.expErr {
				return
			}
			require.Equal(t, test.exp, test.in, "backcompat result mismatch!")
		})
	}
}

func TestParseCommon(t *testing.T) {
	for _, test := range []struct {
		name   string
		in     acls
		exp    acls
		expErr bool
	}{
		{
			name: "nothing is fine",
			exp: acls{
				resourcePatternType: "literal",
				parsed: parsed{
					pattern: kadm.ACLPatternLiteral,
				},
			},
		},

		{
			name: "set values are parsed properly",
			in: acls{
				operations:          []string{"read", "write"},
				cluster:             true,
				resourcePatternType: "match",
			},
			exp: acls{
				operations:          []string{"read", "write"},
				cluster:             true,
				resourcePatternType: "match",
				parsed: parsed{
					operations: []kmsg.ACLOperation{
						kmsg.ACLOperationRead,
						kmsg.ACLOperationWrite,
					},
					pattern: kmsg.ACLResourcePatternTypeMatch,
				},
			},
		},

		{
			name: "bad operation fails",
			in: acls{
				operations: []string{"fail"},
			},
			expErr: true,
		},
		{
			name: "bad pattern fails",
			in: acls{
				resourcePatternType: "foo",
			},
			expErr: true,
		},

		//
	} {
		t.Run(test.name, func(t *testing.T) {
			err := test.in.parseCommon()
			gotErr := err != nil
			require.Equal(t, test.expErr, gotErr, "error mismatch, got: %v, exp? %v", err, test.expErr)
			if test.expErr {
				return
			}
			require.Equal(t, test.exp, test.in, "backcompat result mismatch!")
		})
	}
}
