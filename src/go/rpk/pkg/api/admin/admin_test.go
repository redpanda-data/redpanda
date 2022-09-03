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
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

type testCall struct {
	brokerID int
	req      *http.Request
}
type reqsTest struct {
	// setup
	name     string
	nNodes   int
	leaderID int
	// action
	action func(*testing.T, *AdminAPI) error
	// assertions
	all      []string
	any      []string
	leader   []string
	none     []string
	handlers map[string]http.HandlerFunc
}

func TestAdminAPI(t *testing.T) {
	tests := []reqsTest{
		{
			name:     "delete user in 1 node cluster",
			nNodes:   1,
			leaderID: 0,
			action:   func(t *testing.T, a *AdminAPI) error { return a.DeleteUser(context.Background(), "Milo") },
			leader:   []string{"/v1/security/users/Milo"},
			none:     []string{"/v1/partitions/redpanda/controller/0", "/v1/node_config"},
		},
		{
			name:     "delete user in 3 node cluster",
			nNodes:   3,
			leaderID: 1,
			action:   func(t *testing.T, a *AdminAPI) error { return a.DeleteUser(context.Background(), "Lola") },
			all:      []string{"/v1/node_config"},
			any:      []string{"/v1/partitions/redpanda/controller/0"},
			leader:   []string{"/v1/security/users/Lola"},
		},
		{
			name:     "create user in 3 node cluster",
			nNodes:   3,
			leaderID: 1,
			action: func(t *testing.T, a *AdminAPI) error {
				return a.CreateUser(context.Background(), "Joss", "momorocks", ScramSha256)
			},
			all:    []string{"/v1/node_config"},
			any:    []string{"/v1/partitions/redpanda/controller/0"},
			leader: []string{"/v1/security/users"},
		},
		{
			name:     "list users in 3 node cluster",
			nNodes:   3,
			leaderID: 1,
			handlers: map[string]http.HandlerFunc{
				"/v1/security/users": func(rw http.ResponseWriter, r *http.Request) {
					rw.Write([]byte(`["Joss", "lola", "jeff", "tobias"]`))
					rw.WriteHeader(http.StatusOK)
				},
			},
			action: func(t *testing.T, a *AdminAPI) error {
				users, err := a.ListUsers(context.Background())
				require.NoError(t, err)
				require.Len(t, users, 4)
				return nil
			},
			any:  []string{"/v1/security/users"},
			none: []string{"/v1/partitions/redpanda/controller/0"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			urls := []string{}
			calls := []testCall{}
			mutex := sync.Mutex{}
			tServers := []*httptest.Server{}
			for i := 0; i < tt.nNodes; i += 1 {
				ts := httptest.NewServer(handlerForNode(t, i, tt, &calls, &mutex))
				tServers = append(tServers, ts)
				urls = append(urls, ts.URL)
			}

			defer func() {
				for _, ts := range tServers {
					ts.Close()
				}
			}()

			adminClient, err := NewAdminAPI(urls, BasicCredentials{}, nil)
			require.NoError(t, err)
			err = tt.action(t, adminClient)
			require.NoError(t, err)
			for _, path := range tt.all {
				checkCallToAllNodes(t, calls, path, tt.nNodes)
			}
			for _, path := range tt.any {
				checkCallToAnyNode(t, calls, path, tt.nNodes)
			}
			for _, path := range tt.leader {
				checkCallToLeader(t, calls, path, tt.leaderID)
			}
			for _, path := range tt.none {
				checkCallNone(t, calls, path, tt.nNodes)
			}
		})
	}
}

func handlerForNode(
	t *testing.T, nodeID int, tt reqsTest, calls *[]testCall, mutex *sync.Mutex,
) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		t.Logf("~~~ NodeID: %d, path: %s", nodeID, r.URL.Path)
		mutex.Lock()
		*calls = append(*calls, testCall{nodeID, r})
		mutex.Unlock()

		if h, ok := tt.handlers[r.URL.Path]; ok {
			h(w, r)
			return
		}

		switch {
		case strings.HasPrefix(r.URL.Path, "/v1/node_config"):
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(fmt.Sprintf(`{"node_id": %d}`, nodeID)))
		case strings.HasPrefix(r.URL.Path, "/v1/partitions/redpanda/controller/0"):
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(fmt.Sprintf(`{"leader_id": %d}`, tt.leaderID)))
		case strings.HasPrefix(r.URL.Path, "/v1/security/users"):
			if nodeID == tt.leaderID {
				w.WriteHeader(http.StatusOK)
			} else {
				w.WriteHeader(http.StatusInternalServerError)
			}
		}
	}
}

func checkCallToAllNodes(
	t *testing.T, calls []testCall, path string, nNodes int,
) {
	for i := 0; i < nNodes; i += 1 {
		if len(callsForPathAndNodeID(calls, path, i)) == 0 {
			require.Fail(t, fmt.Sprintf("path (%s) was expected to be called in all nodes but it wasn't called in node (%d)", path, i))
			return
		}
	}
}

func checkCallToAnyNode(
	t *testing.T, calls []testCall, path string, nNodes int,
) {
	for i := 0; i < nNodes; i += 1 {
		if len(callsForPathAndNodeID(calls, path, i)) > 0 {
			return
		}
	}
	require.Fail(t, fmt.Sprintf("path (%s) was expected to be called in any node but it wasn't called", path))
}

func checkCallToLeader(
	t *testing.T, calls []testCall, path string, leaderID int,
) {
	if len(callsForPathAndNodeID(calls, path, leaderID)) == 0 {
		require.Fail(t, fmt.Sprintf("path (%s) was expected to be called in the leader node but it wasn't", path))
	}
}

func checkCallNone(t *testing.T, calls []testCall, path string, nNodes int) {
	for i := 0; i < nNodes; i += 1 {
		if len(callsForPathAndNodeID(calls, path, i)) > 0 {
			require.Fail(t, fmt.Sprintf("path (%s) was expected to not be called but it was called in node (%d)", path, i))
		}
	}
}

func callsForPathAndNodeID(
	calls []testCall, path string, nodeID int,
) []testCall {
	return callsForPath(callsForNodeID(calls, nodeID), path)
}

func callsForPath(calls []testCall, path string) []testCall {
	filtered := []testCall{}
	for _, call := range calls {
		if call.req.URL.Path == path {
			filtered = append(filtered, call)
		}
	}
	return filtered
}

func callsForNodeID(calls []testCall, nodeID int) []testCall {
	filtered := []testCall{}
	for _, call := range calls {
		if call.brokerID == nodeID {
			filtered = append(filtered, call)
		}
	}
	return filtered
}
