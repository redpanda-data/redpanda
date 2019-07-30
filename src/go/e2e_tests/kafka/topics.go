package kafka

import (
	"fmt"
	"vectorized/e2e_tests/rand"
	"vectorized/pkg/redpanda/sandbox"

	"github.com/Shopify/sarama"
)

func CreateRandomTopic(
	sandbox sandbox.Sandbox, partitions int32, replicationFactor int16) string {
	topicName := NewRandomTopic()
	CraeteTopic(sandbox, topicName, partitions, replicationFactor)
	return topicName
}

func CraeteTopic(
	sandbox sandbox.Sandbox, topicName string, partitions int32,
	replicationFactor int16) {
	config := sarama.NewConfig()
	config.Version = sarama.V2_1_0_0
	admin, err := sarama.NewClusterAdmin(BrokerAddresses(sandbox), config)
	panicOnError(err)
	defer admin.Close()
	err = admin.CreateTopic(topicName, &sarama.TopicDetail{
		NumPartitions:     partitions,
		ReplicationFactor: replicationFactor,
	}, false)
	panicOnError(err)
}

func NewRandomTopic() string {
	return fmt.Sprintf("test-topic-%s", rand.String(8))
}
