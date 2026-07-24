// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestFlexInt64(t *testing.T) {
	var v struct {
		Quoted flexInt64 `json:"q"`
		Bare   flexInt64 `json:"b"`
		Absent flexInt64 `json:"a"`
	}
	// Canonical proto3 JSON quotes int64; be liberal and accept bare too.
	if err := json.Unmarshal([]byte(`{"q":"42","b":7}`), &v); err != nil {
		t.Fatal(err)
	}
	if v.Quoted != 42 || v.Bare != 7 || v.Absent != 0 {
		t.Fatalf("got %d/%d/%d, want 42/7/0", v.Quoted, v.Bare, v.Absent)
	}
}

func TestBuildCreateLinkRequest(t *testing.T) {
	got, err := json.Marshal(buildCreateLinkRequest(
		[]string{"source-0:9092", "source-1:9092"}, []string{"tsv2", "cloud"}))
	if err != nil {
		t.Fatal(err)
	}
	want := `{"shadowLink":{"name":"stress-link","configurations":{` +
		`"clientOptions":{"bootstrapServers":["source-0:9092","source-1:9092"]},` +
		`"topicMetadataSyncOptions":{"autoCreateShadowTopicFilters":[` +
		`{"patternType":"PATTERN_TYPE_LITERAL","filterType":"FILTER_TYPE_INCLUDE","name":"tsv2"},` +
		`{"patternType":"PATTERN_TYPE_LITERAL","filterType":"FILTER_TYPE_INCLUDE","name":"cloud"}],` +
		`"startAtEarliest":{}}}}}`
	if string(got) != want {
		t.Fatalf("request JSON:\n got: %s\nwant: %s", got, want)
	}
}

func TestParseShadowLinkResponse(t *testing.T) {
	// Shape per proto/redpanda/core/admin/v2/shadow_link.proto: protojson
	// emits camelCase field names, int64 as strings, enums as names, and
	// omits default-valued fields.
	body := `{"shadowLink":{"name":"stress-link","status":{"state":"SHADOW_LINK_STATE_ACTIVE",
	  "shadowTopics":[{"name":"tsv2","sourceTopicName":"tsv2","status":{
	    "state":"SHADOW_TOPIC_STATE_ACTIVE",
	    "partitionInformation":[
	      {"partitionId":"1","sourceHighWatermark":"100","highWatermark":"90"},
	      {"sourceHighWatermark":"5"}]}}]}}}`
	st, err := parseShadowLinkResponse([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	if st.State != "SHADOW_LINK_STATE_ACTIVE" {
		t.Fatalf("state = %q", st.State)
	}
	if len(st.Partitions) != 2 {
		t.Fatalf("partitions = %d, want 2", len(st.Partitions))
	}
	p := st.Partitions[0]
	if p.Topic != "tsv2" || p.Partition != 1 || p.SourceHWM != 100 || p.HWM != 90 {
		t.Fatalf("partition 0 = %+v", p)
	}
	if q := st.Partitions[1]; q.Partition != 0 || q.SourceHWM != 5 || q.HWM != 0 {
		t.Fatalf("partition 1 (defaults omitted) = %+v", q)
	}
}

// testCluster returns a *cluster whose adminHosts() are controlled entirely
// by the env var it reads, so a test can point it at httptest servers.
func testCluster(t *testing.T, hosts string) *cluster {
	t.Helper()
	c := &cluster{role: "test", adminEnv: "TEST_ADMIN_HOSTS"}
	t.Setenv("TEST_ADMIN_HOSTS", hosts)
	return c
}

// hostPort strips the scheme off an httptest.Server URL (http://host:port)
// since adminHosts() entries are bare host:port.
func hostPort(url string) string {
	return strings.TrimPrefix(url, "http://")
}

func TestConnectPost(t *testing.T) {
	t.Run("definitive error short-circuits host failover", func(t *testing.T) {
		live := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusConflict)
			w.Write([]byte(`{"code":"already_exists","message":"dup"}`))
		}))
		defer live.Close()
		unused := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			t.Error("second host should not be contacted after a definitive connect error")
		}))
		defer unused.Close()

		c := testCluster(t, hostPort(live.URL)+","+hostPort(unused.URL))

		err := connectPost(c, "/some/Procedure", map[string]string{}, nil)
		var cerr *connectError
		if !errors.As(err, &cerr) {
			t.Fatalf("connectPost error = %v (%T), want *connectError", err, err)
		}
		if cerr.Code != "already_exists" {
			t.Fatalf("code = %q, want %q", cerr.Code, "already_exists")
		}
	})

	t.Run("failover to next host on connection error", func(t *testing.T) {
		live := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"name":"stress-link"}`))
		}))
		defer live.Close()

		// 127.0.0.1:1 is a reserved, always-unreachable port: dialing it
		// fails fast with a connection error rather than timing out.
		c := testCluster(t, "127.0.0.1:1,"+hostPort(live.URL))

		var out struct {
			Name string `json:"name"`
		}
		if err := connectPost(c, "/some/Procedure", map[string]string{}, &out); err != nil {
			t.Fatalf("connectPost: %v", err)
		}
		if out.Name != "stress-link" {
			t.Fatalf("out = %+v, want Name %q", out, "stress-link")
		}
	})

	t.Run("non-200 without a connect error body is a plain error", func(t *testing.T) {
		bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("internal error, not json"))
		}))
		defer bad.Close()

		c := testCluster(t, hostPort(bad.URL))

		err := connectPost(c, "/some/Procedure", map[string]string{}, nil)
		if err == nil {
			t.Fatal("connectPost: got nil error, want non-nil")
		}
		var cerr *connectError
		if errors.As(err, &cerr) {
			t.Fatalf("connectPost error = %v, want non-*connectError", cerr)
		}
	})
}
