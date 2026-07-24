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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
)

// The shadow link lives on the TARGET cluster and pulls from the source over
// the Kafka protocol. It is managed through the admin v2 ConnectRPC
// ShadowLinkService; rpadmin mounts the Connect services at "/", so the
// routes are plain POST /redpanda.core.admin.v2.ShadowLinkService/<Method>
// with JSON bodies (canonical proto3 JSON: camelCase fields, int64 as
// strings, enums as names) — no generated client needed.
const linkName = "stress-link"

const shadowLinkService = "/redpanda.core.admin.v2.ShadowLinkService/"

var httpClient = &http.Client{Timeout: 15 * time.Second}

// flexInt64 decodes a JSON int64 whether it arrives quoted (canonical proto3
// JSON) or bare (lenient encoders).
type flexInt64 int64

func (v *flexInt64) UnmarshalJSON(b []byte) error {
	s := strings.Trim(string(b), `"`)
	if s == "" || s == "null" {
		*v = 0
		return nil
	}
	n, err := strconv.ParseInt(s, 10, 64)
	*v = flexInt64(n)
	return err
}

// connectError is a ConnectRPC error body ({"code":"...","message":"..."}).
type connectError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (e *connectError) Error() string {
	return fmt.Sprintf("connect error %s: %s", e.Code, e.Message)
}

// connectPost POSTs req as JSON to the Connect procedure on the first admin
// host of c that answers, decoding a 200 response into out (out may be nil).
// Non-200 responses come back as *connectError when the body parses as one.
func connectPost(c *cluster, procedure string, req, out any) error {
	body, err := json.Marshal(req)
	if err != nil {
		return err
	}
	var lastErr error = fmt.Errorf("no admin hosts configured for %s", c.role)
	for _, h := range c.adminHosts() {
		resp, err := httpClient.Post("http://"+h+procedure, "application/json", bytes.NewReader(body))
		if err != nil {
			lastErr = err
			continue
		}
		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			cerr := &connectError{}
			if json.Unmarshal(respBody, cerr) == nil && cerr.Code != "" {
				return cerr // definitive server answer; do not try other hosts
			}
			lastErr = fmt.Errorf("POST %s -> %d: %s", procedure, resp.StatusCode, respBody)
			continue
		}
		if out == nil {
			return nil
		}
		return json.Unmarshal(respBody, out)
	}
	return lastErr
}

/* Create */

type nameFilter struct {
	PatternType string `json:"patternType"`
	FilterType  string `json:"filterType"`
	Name        string `json:"name"`
}

type clientOpts struct {
	BootstrapServers []string `json:"bootstrapServers"`
}

type topicSyncOpts struct {
	AutoCreateShadowTopicFilters []nameFilter `json:"autoCreateShadowTopicFilters"`
	// Presence of the empty message selects the oneof arm: replicate from
	// the earliest source offset.
	StartAtEarliest struct{} `json:"startAtEarliest"`
}

type linkConfigs struct {
	ClientOptions clientOpts `json:"clientOptions"`
	// Consumer-offset sync is deliberately omitted: synced source groups
	// would clobber same-named target groups, and it is out of scope.
	TopicMetadataSyncOptions topicSyncOpts `json:"topicMetadataSyncOptions"`
}

type shadowLinkSpec struct {
	Name           string      `json:"name"`
	Configurations linkConfigs `json:"configurations"`
}

type createLinkReq struct {
	ShadowLink shadowLinkSpec `json:"shadowLink"`
}

func buildCreateLinkRequest(bootstrap, topics []string) createLinkReq {
	filters := make([]nameFilter, len(topics))
	for i, t := range topics {
		filters[i] = nameFilter{
			PatternType: "PATTERN_TYPE_LITERAL",
			FilterType:  "FILTER_TYPE_INCLUDE",
			Name:        t,
		}
	}
	return createLinkReq{ShadowLink: shadowLinkSpec{
		Name: linkName,
		Configurations: linkConfigs{
			ClientOptions:            clientOpts{BootstrapServers: bootstrap},
			TopicMetadataSyncOptions: topicSyncOpts{AutoCreateShadowTopicFilters: filters},
		},
	}}
}

// createShadowLink creates the link on the target, retrying under faults.
// An already_exists answer means a previous attempt (or timeline replay)
// succeeded and is treated as success.
func createShadowLink() error {
	topics := make([]string, len(testTopics))
	for i, t := range testTopics {
		topics[i] = t.name
	}
	req := buildCreateLinkRequest(srcCluster.brokers(), topics)

	for attempt := 1; attempt <= 30; attempt++ {
		err := connectPost(dstCluster, shadowLinkService+"CreateShadowLink", req, nil)
		if err == nil {
			fmt.Printf("created shadow link %s -> source\n", linkName)
			return nil
		}
		cerr := &connectError{}
		if errors.As(err, &cerr) && cerr.Code == "already_exists" {
			fmt.Printf("shadow link %s already exists\n", linkName)
			return nil
		}
		fmt.Printf("attempt %d: create shadow link failed, retrying: %v\n", attempt, err)
		time.Sleep(2 * time.Second)
	}
	return fmt.Errorf("failed to create shadow link %s", linkName)
}

/* Status */

type partitionLag struct {
	Topic     string
	Partition int64
	SourceHWM int64 // the target's last-observed source high watermark
	HWM       int64 // the shadow partition's own high watermark
}

type linkStatus struct {
	State      string // ShadowLinkState enum name
	Partitions []partitionLag
}

type shadowLinkResp struct {
	ShadowLink struct {
		Status struct {
			State        string `json:"state"`
			ShadowTopics []struct {
				Name   string `json:"name"`
				Status struct {
					State                string `json:"state"`
					PartitionInformation []struct {
						PartitionID         flexInt64 `json:"partitionId"`
						SourceHighWatermark flexInt64 `json:"sourceHighWatermark"`
						HighWatermark       flexInt64 `json:"highWatermark"`
					} `json:"partitionInformation"`
				} `json:"status"`
			} `json:"shadowTopics"`
		} `json:"status"`
	} `json:"shadowLink"`
}

func parseShadowLinkResponse(body []byte) (linkStatus, error) {
	var resp shadowLinkResp
	if err := json.Unmarshal(body, &resp); err != nil {
		return linkStatus{}, err
	}
	st := linkStatus{State: resp.ShadowLink.Status.State}
	for _, t := range resp.ShadowLink.Status.ShadowTopics {
		for _, p := range t.Status.PartitionInformation {
			st.Partitions = append(st.Partitions, partitionLag{
				Topic:     t.Name,
				Partition: int64(p.PartitionID),
				SourceHWM: int64(p.SourceHighWatermark),
				HWM:       int64(p.HighWatermark),
			})
		}
	}
	return st, nil
}

func getShadowLink() (linkStatus, error) {
	var raw json.RawMessage
	err := connectPost(dstCluster, shadowLinkService+"GetShadowLink",
		map[string]string{"name": linkName}, &raw)
	if err != nil {
		return linkStatus{}, err
	}
	return parseShadowLinkResponse(raw)
}

/* Feature flag */

// ensureFeatureActive activates a named feature on cluster c if it is not
// already active, then waits for it. tiered_cloud_topics auto-activates on
// fresh clusters (available_policy::always), so this is normally a no-op
// safety net for images built at an older logical version.
func ensureFeatureActive(c *cluster, feature string) error {
	type featureState struct {
		Name  string `json:"name"`
		State string `json:"state"`
	}
	type featuresResp struct {
		Features []featureState `json:"features"`
	}
	state := func() (string, error) {
		var lastErr error
		for _, h := range c.adminHosts() {
			resp, err := httpClient.Get("http://" + h + "/v1/features")
			if err != nil {
				lastErr = err
				continue
			}
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				lastErr = fmt.Errorf("GET /v1/features -> %d: %s", resp.StatusCode, body)
				continue
			}
			var fr featuresResp
			if err := json.Unmarshal(body, &fr); err != nil {
				return "", err
			}
			for _, f := range fr.Features {
				if f.Name == feature {
					return f.State, nil
				}
			}
			return "", fmt.Errorf("feature %s not in /v1/features response", feature)
		}
		return "", lastErr
	}

	for attempt := 1; attempt <= 30; attempt++ {
		s, err := state()
		if err == nil && s == "active" {
			fmt.Printf("%s cluster: feature %s active\n", c.role, feature)
			return nil
		}
		if err == nil {
			fmt.Printf("%s cluster: feature %s is %q; activating\n", c.role, feature, s)
			for _, h := range c.adminHosts() {
				// A fresh reader per attempt: a shared one would be consumed
				// by the first host that accepts the connection.
				body := bytes.NewReader([]byte(`{"state":"active"}`))
				req, _ := http.NewRequest(http.MethodPut,
					"http://"+h+"/v1/features/"+feature, body)
				req.Header.Set("Content-Type", "application/json")
				resp, err := httpClient.Do(req)
				if err != nil {
					fmt.Printf("%s cluster: PUT features/%s on %s: %v\n", c.role, feature, h, err)
					continue
				}
				respBody, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				if resp.StatusCode == http.StatusOK {
					break
				}
				fmt.Printf("%s cluster: PUT features/%s on %s -> %d: %s\n", c.role, feature, h, resp.StatusCode, respBody)
			}
		} else {
			fmt.Printf("%s cluster: feature state attempt %d: %v\n", c.role, attempt, err)
		}
		time.Sleep(2 * time.Second)
	}
	return fmt.Errorf("feature %s never became active on %s cluster", feature, c.role)
}

/* Commands */

// first_create_topics_and_link: one command so ordering is unambiguous
// (multiple first_ commands have no ordering guarantee): topics on the
// source, feature flags on both clusters, then the link on the target.
// Runs once per timeline after setup_complete.
func createTopicsAndLink() error {
	cl, err := srcCluster.newClient()
	if err != nil {
		return err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	for _, t := range testTopics {
		if err := createOneTopic(adm, t); err != nil {
			return err
		}
	}
	for _, c := range []*cluster{srcCluster, dstCluster} {
		if err := ensureFeatureActive(c, "tiered_cloud_topics"); err != nil {
			return err
		}
	}
	return createShadowLink()
}

func createOneTopic(adm *kadm.Client, t testTopic) error {
	ctx := context.Background()
	for attempt := 1; attempt <= 30; attempt++ {
		resp, err := adm.CreateTopics(ctx, t.partitions, t.replicas, t.config, t.name)
		if err == nil {
			r, ok := resp[t.name]
			switch {
			case !ok:
				err = fmt.Errorf("topic %s missing from create response", t.name)
			case r.Err == nil:
				fmt.Printf("created topic %s (%d partitions, %d replicas)\n", t.name, t.partitions, t.replicas)
				return nil
			case errors.Is(r.Err, kerr.TopicAlreadyExists):
				fmt.Printf("topic %s already exists\n", t.name)
				return nil
			default:
				err = r.Err
			}
		}
		fmt.Printf("attempt %d: create %s failed, retrying: %v\n", attempt, t.name, err)
		time.Sleep(2 * time.Second)
	}
	return fmt.Errorf("failed to create topic %s", t.name)
}

// anytime_check_link: probe the link status on the target and record
// liveness. Best-effort under faults — an unreachable admin API is expected
// and not a bug; persistence comes from Antithesis rescheduling.
func checkLink() error {
	st, err := getShadowLink()
	if err != nil {
		fmt.Printf("link status unavailable (expected under faults): %v\n", err)
		return nil
	}
	active := st.State == "SHADOW_LINK_STATE_ACTIVE"
	replicated := 0
	var maxLag int64
	for _, p := range st.Partitions {
		if p.HWM > 0 {
			replicated++
		}
		if lag := p.SourceHWM - p.HWM; lag > maxLag {
			maxLag = lag
		}
		fmt.Printf("link %s/%d: source_hwm=%d hwm=%d\n", p.Topic, p.Partition, p.SourceHWM, p.HWM)
	}
	details := map[string]any{"state": st.State, "partitions": len(st.Partitions),
		"replicated": replicated, "max_lag": maxLag}
	assert.Sometimes(active, "shadow link reports ACTIVE state", details)
	assert.Sometimes(replicated > 0, "some shadow partition has replicated data", details)
	fmt.Printf("link state=%s partitions=%d replicated=%d max_lag=%d\n",
		st.State, len(st.Partitions), replicated, maxLag)
	return nil
}
