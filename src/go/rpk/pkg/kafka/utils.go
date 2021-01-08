package kafka

import (
	"time"

	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
)

// internal topics
const CoprocessorTopic = "coprocessor_internal_topic"

func PublishMessage(
	producer sarama.SyncProducer,
	message []byte,
	key string,
	topic string,
	header []sarama.RecordHeader,
) error {

	k := sarama.StringEncoder(key)
	ts := time.Now()

	msg := &sarama.ProducerMessage{
		Topic:		topic,
		Key:		k,
		Timestamp:	ts,
		Value:		sarama.ByteEncoder(message),
		Headers:	header,
	}

	retryConf := DefaultConfig().Producer.Retry
	part, offset, err := RetrySend(
		producer,
		msg,
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
