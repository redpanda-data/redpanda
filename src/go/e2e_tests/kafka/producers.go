package kafka

import (
	"vectorized/pkg/redpanda/sandbox"

	"github.com/Shopify/sarama"
)

func CreateProducer(
	s sandbox.Sandbox, config *sarama.Config,
) sarama.SyncProducer {
	return createProducer(BrokerAddresses(s), config)
}

func CreateNodeProducer(
	s sandbox.Sandbox, nodeID int, config *sarama.Config,
) sarama.SyncProducer {
	node, err := s.Node(nodeID)
	panicOnError(err)

	return createProducer([]string{BrokerAddress(node)}, config)
}

func createProducer(
	brokers []string, config *sarama.Config,
) sarama.SyncProducer {
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	setupClientID(config)
	producer, err := sarama.NewSyncProducer(brokers, config)
	panicOnError(err)
	return producer
}

func NewProducerMessage(topic, value string) *sarama.ProducerMessage {
	return &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(value),
	}
}

func NewKeyedProducerMessage(topic, key, value string) *sarama.ProducerMessage {
	return &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.StringEncoder(value),
	}
}
