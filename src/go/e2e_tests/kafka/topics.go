package kafka

import (
	"fmt"
	"vectorized/e2e_tests/rand"

	"github.com/Shopify/sarama"
)

func CreateRandomTopic(
	factory ClientFactory, partitions int32, replicationFactor int16,
) string {
	topicName := NewRandomTopic()
	CraeteTopic(factory, topicName, partitions, replicationFactor)
	return topicName
}

func CraeteTopic(
	factory ClientFactory,
	topicName string,
	partitions int32,
	replicationFactor int16,
) {
	config := sarama.NewConfig()
	config.Version = sarama.V2_1_0_0
	admin := factory.NewClusterAdmin(config)
	defer admin.Close()
	err := admin.CreateTopic(topicName, &sarama.TopicDetail{
		NumPartitions:     partitions,
		ReplicationFactor: replicationFactor,
	}, false)
	panicOnError(err)
}

func NewRandomTopic() string {
	return fmt.Sprintf("test-topic-%s", rand.String(8))
}
