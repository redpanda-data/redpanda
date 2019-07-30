package kafka

import (
	"sync"
	"time"
	"vectorized/pkg/redpanda/sandbox"

	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
)

type PartitionOffset struct {
	Partition int32
	Offset    int64
}

type TopicPartitionOffset struct {
	Topic            string
	PartitionOffsets []PartitionOffset
}

func CreateConsumer(s sandbox.Sandbox, config *sarama.Config) sarama.Consumer {
	return createConsumer(BrokerAddresses(s), config)
}

func CreateNodeConsumer(
	s sandbox.Sandbox, nodeID int, config *sarama.Config,
) sarama.Consumer {
	node, err := s.Node(nodeID)
	panicOnError(err)
	return createConsumer([]string{BrokerAddress(node)}, config)
}

func createConsumer(brokers []string, config *sarama.Config) sarama.Consumer {
	setupClientID(config)
	consumer, err := sarama.NewConsumer(brokers, config)
	panicOnError(err)

	return consumer
}

func ReadMessages(
	consumer sarama.Consumer,
	waitFor time.Duration,
	topicPartitionOffsets []TopicPartitionOffset,
) []*sarama.ConsumerMessage {
	var (
		messages = make(chan *sarama.ConsumerMessage)
		wg       sync.WaitGroup
	)
	for _, tpo := range topicPartitionOffsets {
		for _, po := range tpo.PartitionOffsets {
			log.Debugf("Creating consumer for topic: '%s', partition '%d', offset '%d",
				tpo.Topic, po.Partition, po.Offset)
			pc, err := consumer.ConsumePartition(tpo.Topic, po.Partition, po.Offset)
			panicOnError(err)
			go func(pc sarama.PartitionConsumer, waitFor time.Duration) {
				time.Sleep(waitFor)
				pc.AsyncClose()
			}(pc, waitFor)
			wg.Add(1)
			go func(pc sarama.PartitionConsumer) {
				defer wg.Done()
				for message := range pc.Messages() {
					log.Debugf("Read message from partition '%d' with offset '%d'",
						message.Partition, message.Offset)
					messages <- message
				}
			}(pc)
		}
	}
	var msgSlice []*sarama.ConsumerMessage
	go collectMsgsToSlice(messages, &msgSlice)
	wg.Wait()
	close(messages)
	return msgSlice
}

func ReadMessagesFromTopic(
	consumer sarama.Consumer, waitFor time.Duration, topic string,
) []*sarama.ConsumerMessage {
	partitions, err := consumer.Partitions(topic)
	panicOnError(err)
	var partitionOffsets []PartitionOffset
	for _, partition := range partitions {
		partitionOffsets = append(partitionOffsets,
			PartitionOffset{
				Partition: partition,
				Offset:    sarama.OffsetOldest,
			})
	}
	return ReadMessages(consumer, waitFor, []TopicPartitionOffset{
		TopicPartitionOffset{
			Topic:            topic,
			PartitionOffsets: partitionOffsets,
		},
	})
}

func collectMsgsToSlice(
	c chan *sarama.ConsumerMessage, slice *[]*sarama.ConsumerMessage,
) {
	for msg := range c {
		*slice = append(*slice, msg)
	}
}
