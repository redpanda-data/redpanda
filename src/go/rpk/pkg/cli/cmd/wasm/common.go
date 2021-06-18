// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package wasm

import (
	"crypto/sha256"
	"encoding/binary"

	"github.com/Shopify/sarama"
	"github.com/cespare/xxhash"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/kafka"
)

/**
Create coprocessor internal topic with this config:
	cleanup.policy = compact
	replicationFactor = if cluster has 2 brokers o more
						factor is 3, otherwise it should be 1
	name = coprocessor_internal_topic
*/
func CreateCoprocessorTopic(admin sarama.ClusterAdmin) error {
	brokers, _, err := admin.DescribeCluster()
	if err != nil {
		return err
	}
	brokersLen := len(brokers)
	var replicationFactor int16 = 1
	if brokersLen > 1 {
		replicationFactor = 3
	}
	configEntry := make(map[string]*string)
	compact := "compact"
	compressionType := "zstd"
	configEntry["cleanup.policy"] = &compact
	configEntry["compression.type"] = &compressionType
	detail := sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: replicationFactor,
		ReplicaAssignment: nil,
		ConfigEntries:     configEntry,
	}
	err = admin.CreateTopic(kafka.CoprocessorTopic, &detail, false)
	if err != nil {
		return err
	}
	return nil
}

/**
Validate if the given topic exist in Cluster
*/
func ExistingTopic(admin sarama.ClusterAdmin, topic string) (bool, error) {
	topics, err := admin.ListTopics()
	if err != nil {
		return false, err
	}
	if _, val := topics[topic]; val {
		return true, err
	} else {
		return false, err
	}
}

func CreateDeployMsg(
	name string, description string, content []byte,
) sarama.ProducerMessage {
	shaValue := sha256.Sum256(content)
	var headers = []sarama.RecordHeader{
		{
			Key:   []byte("action"),
			Value: []byte("deploy"),
		}, {
			Key:   []byte("description"),
			Value: []byte(description),
		}, {
			Key:   []byte("sha256"),
			Value: shaValue[:],
		}, {
			Key:   []byte("name"),
			Value: []byte(name),
		},
	}
	id := xxhash.Sum64([]byte(name))
	binaryId := make([]byte, 8)
	binary.LittleEndian.PutUint64(binaryId, id)
	return sarama.ProducerMessage{
		Key:     sarama.ByteEncoder(binaryId),
		Topic:   kafka.CoprocessorTopic,
		Value:   sarama.ByteEncoder(content),
		Headers: headers,
	}
}

func CreateRemoveMsg(name string) sarama.ProducerMessage {
	var headers = []sarama.RecordHeader{
		{
			Key:   []byte("action"),
			Value: []byte("remove"),
		},
	}
	id := xxhash.Sum64([]byte(name))
	binaryId := make([]byte, 8)
	binary.LittleEndian.PutUint64(binaryId, id)
	return sarama.ProducerMessage{
		Key:   sarama.ByteEncoder(binaryId),
		Topic: kafka.CoprocessorTopic,
		// create empty message, the remove command doesn't need
		// information on message, just a key value
		Value:   sarama.ByteEncoder([]byte{}),
		Headers: headers,
	}
}
