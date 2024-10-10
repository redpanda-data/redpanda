// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package adminapi

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/spf13/afero"
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

			adminClient, err := NewAdminAPI(urls, new(NopAuth), nil)
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

func Test_licenseFeatureChecks(t *testing.T) {
	tests := []struct {
		name         string
		prof         *config.RpkProfile
		responseCase string // See the mapLicenseResponses below.
		expContain   string
		withErr      bool
		checkCache   func(t *testing.T, before int64, after int64)
	}{
		{
			name:         "license ok, first time call",
			prof:         &config.RpkProfile{},
			responseCase: "ok",
			expContain:   "",
		},
		{
			name: "license ok, cache valid",
			prof: &config.RpkProfile{LicenseCheck: &config.LicenseStatusCache{LastUpdate: time.Now().Add(20 * time.Minute).Unix()}},
			checkCache: func(t *testing.T, before int64, after int64) {
				// If the cache was valid, last update shouldn't have changed.
				require.Equal(t, before, after)
			},
			responseCase: "ok",
			expContain:   "",
		},
		{
			name: "license ok, old cache",
			prof: &config.RpkProfile{LicenseCheck: &config.LicenseStatusCache{LastUpdate: time.Now().AddDate(0, 0, -20).Unix()}}, // Limit is 1 hour
			checkCache: func(t *testing.T, before int64, after int64) {
				// Date should be updated.
				afterT := time.Unix(after, 0)
				require.True(t, time.Unix(before, 0).Before(afterT))
			},
			responseCase: "ok",
			expContain:   "",
		},
		{
			name:         "inViolation, first time call",
			prof:         &config.RpkProfile{},
			responseCase: "inViolation",
			expContain:   "These features require a license",
		},
		{
			name:         "inViolation, expired last check",
			prof:         &config.RpkProfile{LicenseCheck: &config.LicenseStatusCache{LastUpdate: time.Now().AddDate(0, 0, -20).Unix()}},
			responseCase: "inViolation",
			expContain:   "These features require a license",
		},
		{
			// Edge case when the license expires but the last check was less
			// than 1 hour ago.
			name:         "inViolation, cache still valid",
			prof:         &config.RpkProfile{LicenseCheck: &config.LicenseStatusCache{LastUpdate: time.Now().Add(30 * time.Minute).Unix()}},
			responseCase: "inViolation",
			// In this case, even if the license is in violation, rpk won't
			// reach the Admin API because the last check was under 15 days.
			checkCache: func(t *testing.T, before int64, after int64) {
				// At this point we don't rewrite the last check, because still
				// valid.
				require.Equal(t, before, after)
			},
			expContain: "",
		},
		{
			name:         "admin API errors, don't print",
			prof:         &config.RpkProfile{},
			withErr:      true,
			responseCase: "failedRequest",
			// If we fail to communicate with the cluster, or the request fails,
			// then we just WARN the user of what happened but won't print to
			// stdout.
			expContain: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := httptest.NewServer(licenseHandler(tt.responseCase))
			defer ts.Close()
			tt.prof.AdminAPI = config.RpkAdminAPI{Addresses: []string{ts.URL}}
			fs := afero.NewMemMapFs()
			loadedProfile := writeRpkProfileToFs(t, fs, tt.prof)
			client, err := NewHostClient(fs, loadedProfile, "0")
			require.NoError(t, err)
			got := licenseFeatureChecks(context.Background(), fs, client, loadedProfile)
			if tt.expContain == "" {
				require.Empty(t, got)
				if tt.withErr {
					return
				}
				// If we get to this point, we need to make sure that the last
				// update date was registered.
				afterProf := loadProfile(t, fs)
				require.NotEmpty(t, afterProf.LicenseCheck)
				require.NotEmpty(t, afterProf.LicenseCheck.LastUpdate)
				if tt.checkCache != nil {
					tt.checkCache(t, tt.prof.LicenseCheck.LastUpdate, afterProf.LicenseCheck.LastUpdate)
				}
				return
			}
			require.Contains(t, got, tt.expContain)
			// If we get to this point, then we shouldn't have the last
			// update registered.
			afterProf := loadProfile(t, fs)
			require.Empty(t, afterProf.LicenseCheck)
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

func writeRpkProfileToFs(t *testing.T, fs afero.Fs, p *config.RpkProfile) *config.RpkProfile {
	p.Name = "test"
	rpkyaml := config.RpkYaml{
		CurrentProfile: "test",
		Version:        6,
		Profiles:       []config.RpkProfile{*p},
	}
	err := rpkyaml.Write(fs)
	require.NoError(t, err)

	return loadProfile(t, fs)
}

func loadProfile(t *testing.T, fs afero.Fs) *config.RpkProfile {
	y, err := new(config.Params).Load(fs)
	require.NoError(t, err)
	return y.VirtualProfile()
}

type response struct {
	status int
	body   string
}

var mapLicenseResponses = map[string]response{
	"ok":            {http.StatusOK, `{"license_status": "valid", "violation": false}`},
	"inViolation":   {http.StatusOK, `{"license_status": "expired", "violation": true, "features": [{"name": "partition_auto_balancing_continuous", "enabled": true}]}`},
	"failedRequest": {http.StatusBadRequest, ""},
}

func licenseHandler(respCase string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		resp := mapLicenseResponses[respCase]
		w.WriteHeader(resp.status)
		w.Write([]byte(resp.body))
	}
}
