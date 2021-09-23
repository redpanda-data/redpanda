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

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestBackCompatList(t *testing.T) {
	t.Run("incompatible combo", func(t *testing.T) {
		var a acls
		for _, newFlag := range []*[]string{
			&a.any,
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
			{"any", true},
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
			exp: acls{
				any: []string{
					allowPrincipalFlag,
					allowHostFlag,
					denyPrincipalFlag,
					denyHostFlag,
				},
			},
		},

		{
			name:  "allow perms, nothing else matches allow all",
			perms: []string{"allow"},
			exp: acls{
				any: []string{
					allowPrincipalFlag,
					allowHostFlag,
				},
			},
		},

		{
			name:  "allow & deny perms, some principals, no hosts",
			perms: []string{"allow", "deny"},
			princ: []string{"some", "principals"},
			exp: acls{
				allowPrincipals: []string{"some", "principals"},
				denyPrincipals:  []string{"some", "principals"},
				any: []string{
					allowHostFlag,
					denyHostFlag,
				},
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
			exp: acls{
				any: []string{
					allowPrincipalFlag,
					allowHostFlag,
					denyPrincipalFlag,
					denyHostFlag,
				},
			},
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
					pattern: patLit,
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
					clusters: []string{kafkaCluster},
					pattern:  kmsg.ACLResourcePatternTypeMatch,
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

func TestParseAny(t *testing.T) {
	var a acls
	p := &a.parsed
	for _, test := range []struct {
		check *[]string
		flag  string
		set   *bool
	}{
		{&a.topics, topicFlag, &p.anyTopic},
		{&a.groups, groupFlag, &p.anyGroup},
		{&a.parsed.clusters, clusterFlag, &p.anyCluster},
		{&a.txnIDs, txnIDFlag, &p.anyTxn},
		{&a.allowPrincipals, allowPrincipalFlag, &p.anyAllowPrincipal},
		{&a.allowHosts, allowHostFlag, &p.anyAllowHost},
		{&a.denyPrincipals, denyPrincipalFlag, &p.anyDenyPrincipal},
		{&a.denyHosts, denyHostFlag, &p.anyDenyHost},
		{&a.operations, operationFlag, &p.anyOperation},
	} {
		a = acls{} // reset for test loop

		err := a.parseAny()
		require.NoError(t, err, "no any set is fine")
		require.Equal(t, acls{}, a, "nothing set changes nothing")

		*test.check = append(*test.check, "foo")
		err = a.parseAny()
		require.NoError(t, err, "no any flag set with one val set is fine")
		require.Equal(t, false, *test.set, "expected any to not yet be set")

		// For clusters, we append to the parsed.clusters directly.
		// This test is for ensuring the anyCluster flag is false;
		// setting this slice is not what we are looking for but alows the test to pass.
		var expParsed parsed
		if test.flag == clusterFlag {
			expParsed.clusters = *test.check
		}
		require.Equal(t, expParsed, a.parsed, "no any flag set parses nothing")

		// Now we add an "any", we expect things to be fine.
		a.any = append(a.any, test.flag)
		err = a.parseAny()
		require.NoError(t, err, "unexpected error: %v", err)
		require.Equal(t, true, *test.set, "expected any to be set")
	}

	// Finally, ensure we cannot set weird values.
	a = acls{any: []string{"foo"}}
	err := a.parseAny()
	require.Error(t, err, "expected error on invalid any value")
}

// The tests below use these constants *a lot*, so we alias some long name
// constants here to make things a little bit smaller.
const (
	tany     = kmsg.ACLResourceTypeAny
	ttopic   = kmsg.ACLResourceTypeTopic
	tgroup   = kmsg.ACLResourceTypeGroup
	tcluster = kmsg.ACLResourceTypeCluster
	ttxn     = kmsg.ACLResourceTypeTransactionalId

	patAny = kmsg.ACLResourcePatternTypeAny
	patLit = kmsg.ACLResourcePatternTypeLiteral

	oany  = kmsg.ACLOperationAny
	oall  = kmsg.ACLOperationAll
	oread = kmsg.ACLOperationRead

	pany   = kmsg.ACLPermissionTypeAny
	pdeny  = kmsg.ACLPermissionTypeDeny
	pallow = kmsg.ACLPermissionTypeAllow
)

func TestCreateCreations(t *testing.T) {
	// For this test, for simplicity, we always use pattern type literal.
	// This "c" function helps us initialize creations succinctly.
	c := func(
		t kmsg.ACLResourceType,
		name string,
		principal string,
		host string,
		op kmsg.ACLOperation,
		perm kmsg.ACLPermissionType,
	) kmsg.CreateACLsRequestCreation {
		return kmsg.CreateACLsRequestCreation{
			ResourceType:        t,
			ResourceName:        name,
			ResourcePatternType: patLit,
			Principal:           principal,
			Host:                host,
			Operation:           op,
			PermissionType:      perm,
		}
	}

	for _, test := range []struct {
		name   string
		in     acls
		exp    []kmsg.CreateACLsRequestCreation
		expErr bool
	}{
		{

			// In this test, we basically demonstrate the
			// combinatorial explosion of processing inputs, as
			// well as show the creations are sorted.
			//
			// We specify no hosts to show that the default is *.
			//
			// For each type, for each name, there are 8 ACLs
			// created: 2 ops * (2 allow + 2 deny principals).
			//
			// We have 6 names total across 4 types, resulting
			// in 6*8 = 48 ACLs created.
			name: "combinatorial expansion",

			in: acls{
				allowPrincipals: []string{"p1", "p2"},
				denyPrincipals:  []string{"deny1", "deny2"},
				topics:          []string{"t1", "t2"},
				groups:          []string{"g"},
				cluster:         true,
				txnIDs:          []string{"txn1", "txn2"},
				operations:      []string{"read", "all"},
			},

			exp: []kmsg.CreateACLsRequestCreation{
				c(ttopic, "t1", "deny1", "*", oall, pdeny),
				c(ttopic, "t1", "deny1", "*", oread, pdeny),
				c(ttopic, "t1", "deny2", "*", oall, pdeny),
				c(ttopic, "t1", "deny2", "*", oread, pdeny),
				c(ttopic, "t1", "p1", "*", oall, pallow),
				c(ttopic, "t1", "p1", "*", oread, pallow),
				c(ttopic, "t1", "p2", "*", oall, pallow),
				c(ttopic, "t1", "p2", "*", oread, pallow),
				c(ttopic, "t2", "deny1", "*", oall, pdeny),
				c(ttopic, "t2", "deny1", "*", oread, pdeny),
				c(ttopic, "t2", "deny2", "*", oall, pdeny),
				c(ttopic, "t2", "deny2", "*", oread, pdeny),
				c(ttopic, "t2", "p1", "*", oall, pallow),
				c(ttopic, "t2", "p1", "*", oread, pallow),
				c(ttopic, "t2", "p2", "*", oall, pallow),
				c(ttopic, "t2", "p2", "*", oread, pallow),
				c(tgroup, "g", "deny1", "*", oall, pdeny),
				c(tgroup, "g", "deny1", "*", oread, pdeny),
				c(tgroup, "g", "deny2", "*", oall, pdeny),
				c(tgroup, "g", "deny2", "*", oread, pdeny),
				c(tgroup, "g", "p1", "*", oall, pallow),
				c(tgroup, "g", "p1", "*", oread, pallow),
				c(tgroup, "g", "p2", "*", oall, pallow),
				c(tgroup, "g", "p2", "*", oread, pallow),
				c(tcluster, kafkaCluster, "deny1", "*", oall, pdeny),
				c(tcluster, kafkaCluster, "deny1", "*", oread, pdeny),
				c(tcluster, kafkaCluster, "deny2", "*", oall, pdeny),
				c(tcluster, kafkaCluster, "deny2", "*", oread, pdeny),
				c(tcluster, kafkaCluster, "p1", "*", oall, pallow),
				c(tcluster, kafkaCluster, "p1", "*", oread, pallow),
				c(tcluster, kafkaCluster, "p2", "*", oall, pallow),
				c(tcluster, kafkaCluster, "p2", "*", oread, pallow),
				c(ttxn, "txn1", "deny1", "*", oall, pdeny),
				c(ttxn, "txn1", "deny1", "*", oread, pdeny),
				c(ttxn, "txn1", "deny2", "*", oall, pdeny),
				c(ttxn, "txn1", "deny2", "*", oread, pdeny),
				c(ttxn, "txn1", "p1", "*", oall, pallow),
				c(ttxn, "txn1", "p1", "*", oread, pallow),
				c(ttxn, "txn1", "p2", "*", oall, pallow),
				c(ttxn, "txn1", "p2", "*", oread, pallow),
				c(ttxn, "txn2", "deny1", "*", oall, pdeny),
				c(ttxn, "txn2", "deny1", "*", oread, pdeny),
				c(ttxn, "txn2", "deny2", "*", oall, pdeny),
				c(ttxn, "txn2", "deny2", "*", oread, pdeny),
				c(ttxn, "txn2", "p1", "*", oall, pallow),
				c(ttxn, "txn2", "p1", "*", oread, pallow),
				c(ttxn, "txn2", "p2", "*", oall, pallow),
				c(ttxn, "txn2", "p2", "*", oread, pallow),
			},
		},

		{ // This test exercises non-defaulted hosts.
			name: "non-defaulted hosts",
			in: acls{
				allowPrincipals: []string{"p"},
				allowHosts:      []string{"allowh"},
				denyPrincipals:  []string{"p"},
				denyHosts:       []string{"denyh"},
				topics:          []string{"t"},
				operations:      []string{"read"},
			},
			exp: []kmsg.CreateACLsRequestCreation{
				c(ttopic, "t", "p", "allowh", oread, pallow),
				c(ttopic, "t", "p", "denyh", oread, pdeny),
			},
		},

		{ // No creations is fine, we exit on return.
			name: "no creations is fine",
			in:   acls{allowPrincipals: []string{"p"}},
			exp:  nil,
		},

		// Error cases: these are all simple.
		{
			name: "any operation is blocked",
			in: acls{
				allowPrincipals: []string{"p"},
				operations:      []string{"any"},
			},
			expErr: true,
		},
		{
			name: "any pattern is blocked",
			in: acls{
				allowPrincipals:     []string{"p"},
				resourcePatternType: "any",
			},
			expErr: true,
		},
		{
			name:   "allow hosts with no principals fails",
			in:     acls{allowHosts: []string{"h"}},
			expErr: true,
		},
		{
			name:   "deny hosts with no principals fails",
			in:     acls{denyHosts: []string{"h"}},
			expErr: true,
		},

		//
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := test.in.createCreations()
			gotErr := err != nil

			require.Equal(t, test.expErr, gotErr, "got err? %v (%v), exp err? %v", gotErr, err, test.expErr)
			if gotErr || test.expErr {
				return
			}
			require.Equal(t, test.exp, got, "mismatched creations")
		})
	}
}

func TestCreateDeletionsAndDescribes(t *testing.T) {
	type deleteDescribe struct {
		del  kmsg.DeleteACLsRequestFilter
		desc *kmsg.DescribeACLsRequest
	}

	// p and c are two shorthand functions for use below.
	p := func(s string) *string { return &s }
	c := func(
		t kmsg.ACLResourceType,
		name *string,
		pattern kmsg.ACLResourcePatternType,
		principal *string,
		host *string,
		op kmsg.ACLOperation,
		perm kmsg.ACLPermissionType,
	) deleteDescribe {
		return deleteDescribe{
			del: kmsg.DeleteACLsRequestFilter{
				ResourceType:        t,
				ResourceName:        name,
				ResourcePatternType: pattern,
				Principal:           principal,
				Host:                host,
				Operation:           op,
				PermissionType:      perm,
			},
			desc: &kmsg.DescribeACLsRequest{
				ResourceType:        t,
				ResourceName:        name,
				ResourcePatternType: pattern,
				Principal:           principal,
				Host:                host,
				Operation:           op,
				PermissionType:      perm,
			},
		}
	}

	for _, test := range []struct {
		name   string
		in     acls
		exp    []deleteDescribe
		expErr bool
	}{
		{
			// This is a slimmed version of the combinatorial
			// expansion from the creation test above. The only
			// thing we have two of is operations.
			name: "comprehensive",

			in: acls{
				allowPrincipals: []string{"pa"},
				denyPrincipals:  []string{"pd"},
				allowHosts:      []string{"ha"},
				denyHosts:       []string{"hd"},
				topics:          []string{"t"},
				groups:          []string{"g"},
				cluster:         true,
				txnIDs:          []string{"txn"},
				operations:      []string{"read", "all"},
			},

			exp: []deleteDescribe{
				c(ttopic, p("t"), patLit, p("pa"), p("ha"), oall, pallow),
				c(ttopic, p("t"), patLit, p("pa"), p("ha"), oread, pallow),
				c(ttopic, p("t"), patLit, p("pd"), p("hd"), oall, pdeny),
				c(ttopic, p("t"), patLit, p("pd"), p("hd"), oread, pdeny),
				c(tgroup, p("g"), patLit, p("pa"), p("ha"), oall, pallow),
				c(tgroup, p("g"), patLit, p("pa"), p("ha"), oread, pallow),
				c(tgroup, p("g"), patLit, p("pd"), p("hd"), oall, pdeny),
				c(tgroup, p("g"), patLit, p("pd"), p("hd"), oread, pdeny),
				c(tcluster, p(kafkaCluster), patLit, p("pa"), p("ha"), oall, pallow),
				c(tcluster, p(kafkaCluster), patLit, p("pa"), p("ha"), oread, pallow),
				c(tcluster, p(kafkaCluster), patLit, p("pd"), p("hd"), oall, pdeny),
				c(tcluster, p(kafkaCluster), patLit, p("pd"), p("hd"), oread, pdeny),
				c(ttxn, p("txn"), patLit, p("pa"), p("ha"), oall, pallow),
				c(ttxn, p("txn"), patLit, p("pa"), p("ha"), oread, pallow),
				c(ttxn, p("txn"), patLit, p("pd"), p("hd"), oall, pdeny),
				c(ttxn, p("txn"), patLit, p("pd"), p("hd"), oread, pdeny),
			},
		},

		{
			name: "any",
			in:   acls{any: []string{"all"}},
			exp: []deleteDescribe{
				c(tany, nil, patAny, nil, nil, oany, pany),
			},
		},

		{
			name: "any individually",
			in:   acls{any: allIndividualFlags},
			exp: []deleteDescribe{
				c(ttopic, nil, patAny, nil, nil, oany, pdeny),
				c(ttopic, nil, patAny, nil, nil, oany, pallow),
				c(tgroup, nil, patAny, nil, nil, oany, pdeny),
				c(tgroup, nil, patAny, nil, nil, oany, pallow),
				c(tcluster, nil, patAny, nil, nil, oany, pdeny),
				c(tcluster, nil, patAny, nil, nil, oany, pallow),
				c(ttxn, nil, patAny, nil, nil, oany, pdeny),
				c(ttxn, nil, patAny, nil, nil, oany, pallow),
			},
		},

		{
			name: "allow principal with any hosts succeeds",
			in:   acls{allowPrincipals: []string{"."}, any: []string{allowHostFlag}},
		},

		// Error cases: these are all simple.
		{
			name:   "allow principal with no hosts fails",
			in:     acls{allowPrincipals: []string{"."}},
			expErr: true,
		},
		{
			name:   "allow hosts with no principals fails",
			in:     acls{allowHosts: []string{"."}},
			expErr: true,
		},
		{
			name:   "deny principal with no hosts fails",
			in:     acls{denyPrincipals: []string{"."}},
			expErr: true,
		},
		{
			name:   "deny hosts with no principals fails",
			in:     acls{denyHosts: []string{"."}},
			expErr: true,
		},

		//
	} {
		t.Run(test.name, func(t *testing.T) {
			// We always call createDeletionsAndDescribes with
			// describe=false. This does mean we always create in
			// the style of deletions (no backcompat listing), but
			// backcompat listing is heavily tested above.

			gotDeletes, gotDescribes, err := test.in.createDeletionsAndDescribes(false)
			gotErr := err != nil

			require.Equal(t, test.expErr, gotErr, "got err? %v (%v), exp err? %v", gotErr, err, test.expErr)
			if gotErr || test.expErr {
				return
			}
			var expDel []kmsg.DeleteACLsRequestFilter
			var expDesc []*kmsg.DescribeACLsRequest
			for _, delDesc := range test.exp {
				expDel = append(expDel, delDesc.del)
				expDesc = append(expDesc, delDesc.desc)
			}
			require.Equal(t, expDel, gotDeletes, "mismatched deletions")
			require.Equal(t, expDesc, gotDescribes, "mismatched describes")
		})
	}
}
