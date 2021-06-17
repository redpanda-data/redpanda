package kafka

import (
	"time"

	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
)

// internal topics
const CoprocessorTopic = "coprocessor_internal_topic"
const CoprocessorStatusTopic = "coprocessor_status_topic"

func PublishMessage(
	producer sarama.SyncProducer, produceMessage *sarama.ProducerMessage,
) error {
	ts := time.Now()
	produceMessage.Timestamp = ts
	retryConf := DefaultConfig().Producer.Retry
	part, offset, err := RetrySend(
		producer,
		produceMessage,
		uint(retryConf.Max),
		retryConf.Backoff,
	)
	if err != nil {
		log.Error("Failed to send record")
		return err
	}

	log.Infof(
		"Sent record to partition %d at offset %d with timestamp %v.",
		part,
		offset,
		ts,
	)
	return nil
}
