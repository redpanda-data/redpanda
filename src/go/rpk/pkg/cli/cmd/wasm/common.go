package wasm

import (
	"github.com/Shopify/sarama"
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
	configEntry["cleanup.policy"] = &compact
	detail := sarama.TopicDetail{
		NumPartitions:		1,
		ReplicationFactor:	replicationFactor,
		ReplicaAssignment:	nil,
		ConfigEntries:		configEntry,
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
