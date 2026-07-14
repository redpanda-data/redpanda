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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
)

// The cloud-topics L1 metastore / domain topic is an internal topic, so its
// replicas are moved through Redpanda's admin API rather than the Kafka
// reassignment API. With cloud_topics_num_metastore_partitions=1 there is a
// single partition to target.
const (
	metastoreNS    = "kafka_internal"
	metastoreTopic = "ct_l1_domain"
	metastorePart  = 0
)

type replica struct {
	NodeID int `json:"node_id"`
	Core   int `json:"core"`
}

type partitionInfo struct {
	Status   string    `json:"status"`
	LeaderID int       `json:"leader_id"`
	Replicas []replica `json:"replicas"`
}

type brokerInfo struct {
	NodeID   int `json:"node_id"`
	NumCores int `json:"num_cores"`
}

var httpClient = &http.Client{Timeout: 10 * time.Second}

func adminHosts() []string {
	return strings.Split(os.Getenv("ADMIN_HOSTS"), ",")
}

// adminGET issues a GET against the first admin host that answers.
func adminGET(path string, out any) error {
	var lastErr error
	for _, h := range adminHosts() {
		resp, err := httpClient.Get("http://" + h + path)
		if err != nil {
			lastErr = err
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			lastErr = fmt.Errorf("GET %s -> %d: %s", path, resp.StatusCode, body)
			continue
		}
		return json.Unmarshal(body, out)
	}
	return lastErr
}

// submitMove reassigns ns/topic/part to a fresh random replica set of the same
// replication factor. attempted is false when the move was skipped (partition
// mid-reconfiguration, too few brokers, or the admin API was unreachable);
// accepted reports whether the POST was taken. Best-effort under fault
// injection: skips and rejections are expected and not bugs.
func submitMove(ns, topic string, part int) (from, to []replica, attempted, accepted bool) {
	var brokers []brokerInfo
	if err := adminGET("/v1/brokers", &brokers); err != nil {
		return nil, nil, false, false
	}
	path := fmt.Sprintf("/v1/partitions/%s/%s/%d", ns, topic, part)
	var info partitionInfo
	if err := adminGET(path, &info); err != nil {
		return nil, nil, false, false
	}

	rf := len(info.Replicas)
	// Only move once the previous reconfiguration has settled, and only if
	// there are enough brokers for the replication factor.
	if info.Status != "done" || rf == 0 || len(brokers) < rf {
		fmt.Printf("%s/%s/%d move skipped: status=%s rf=%d brokers=%d\n",
			ns, topic, part, info.Status, rf, len(brokers))
		return info.Replicas, nil, false, false
	}

	// Pick rf distinct random brokers (Fisher-Yates via Antithesis rng).
	shuffled := make([]brokerInfo, len(brokers))
	copy(shuffled, brokers)
	for i := len(shuffled) - 1; i > 0; i-- {
		j := randN(i + 1)
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	}
	target := make([]replica, rf)
	for i := range rf {
		core := 0
		if shuffled[i].NumCores > 0 {
			core = randN(shuffled[i].NumCores)
		}
		target[i] = replica{NodeID: shuffled[i].NodeID, Core: core}
	}

	body, _ := json.Marshal(target)
	for _, h := range adminHosts() {
		resp, err := httpClient.Post(
			"http://"+h+path+"/replicas", "application/json", bytes.NewReader(body))
		if err != nil {
			continue
		}
		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			fmt.Printf("%s/%s/%d replica move %v -> %v\n", ns, topic, part, info.Replicas, target)
			return info.Replicas, target, true, true
		}
		fmt.Printf("%s/%s/%d move POST %d: %s\n", ns, topic, part, resp.StatusCode, respBody)
	}
	return info.Replicas, target, true, false
}

func haveAdminHosts() bool {
	h := adminHosts()
	return len(h) > 0 && h[0] != ""
}

// parallel_driver_move_metastore: reassign the single metastore/domain
// partition, exercising reconfiguration of cloud-topics metadata alongside the
// produce/consume workload.
func moveMetastore() error {
	if !haveAdminHosts() {
		return fmt.Errorf("ADMIN_HOSTS not set")
	}
	from, to, attempted, accepted := submitMove(metastoreNS, metastoreTopic, metastorePart)
	if attempted {
		assert.Sometimes(accepted, "cloud topics metastore replica move accepted",
			map[string]any{"from": from, "to": to})
	}
	return nil
}

// parallel_driver_move_kafka_topic: reassign a random partition of a random
// kafka test topic, exercising reconfiguration of user data alongside the
// workload.
func moveKafkaTopic() error {
	if !haveAdminHosts() {
		return fmt.Errorf("ADMIN_HOSTS not set")
	}
	t := testTopics[randN(len(testTopics))]
	part := randN(int(t.partitions))
	from, to, attempted, accepted := submitMove("kafka", t.name, part)
	if attempted {
		details := map[string]any{"topic": t.name, "partition": part, "from": from, "to": to}
		// One liveness property per topic, so a run only proves the mover
		// works once a move has been accepted on each topic somewhere.
		switch t.name {
		case fooTopic:
			assert.Sometimes(accepted, "cloud topic foo replica move accepted", details)
		case ctcTopic:
			assert.Sometimes(accepted, "cloud topic ctc replica move accepted", details)
		}
	}
	return nil
}
