package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Shopify/sarama"
)

var (
	brokers = flag.String("brokers", "127.0.0.1:9092", "Th Redpanda brokers to connect to, as a comma separated list")
	count   = flag.Int64("count", 100, "Optional count to run")
)

func main() {
	flag.Parse()

	brokerList := strings.Split(*brokers, ",")
	fmt.Printf("Redpanda brokers: %s\n", strings.Join(brokerList, ", "))

	topics := []string{
		"topic1",
	}

	producer, err := newProducer(brokerList)
	if err != nil {
		fmt.Printf("Error creating producer, %v", err)
		os.Exit(1)
	}

	err = runTest(producer, topics, *count)
	if err != nil {
		fmt.Printf("Error running test: %v", err)
		os.Exit(1)
	}
}

func newProducer(brokers []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()

	config.Metadata.Full = false
	config.Producer.Compression = sarama.CompressionZSTD
	config.Producer.Idempotent = true // hard
	config.Producer.Partitioner = sarama.NewReferenceHashPartitioner
	config.Producer.RequiredAcks = sarama.WaitForAll // hard
	config.Producer.Retry.Max = 10                   //hard
	config.Producer.Return.Successes = true
	config.Net.MaxOpenRequests = 1
	config.Version = sarama.V2_1_0_0

	producer, err := sarama.NewSyncProducer(brokers, config)

	if err != nil {
		panic(err)
	}

	return producer, nil
}

func prepareMessage(
	producer sarama.SyncProducer, topic string, message string,
) *sarama.ProducerMessage {
	producerMessage := &sarama.ProducerMessage{
		Topic:     topic,
		Partition: -1,
		Value:     sarama.ByteEncoder(message),
		Timestamp: time.Now(),
	}

	return producerMessage
}

func runTest(producer sarama.SyncProducer, topics []string, count int64) error {
	fmt.Printf("Starting Sarama test\n")
	message := "some message"

	msg := prepareMessage(producer, topics[0], message)
	_, _, err := producer.SendMessage(msg)

	if err != nil {
		return fmt.Errorf("SendMessage error: %w", err)
	}

	for i := 0; i < int(count); i++ {
		msg := prepareMessage(producer, topics[0], message+strconv.Itoa(i))
		_, _, err := producer.SendMessage(msg)
		if err != nil {
			return fmt.Errorf("SendMessage error: %w", err)
		}
	}

	fmt.Printf("%d messages was written\n", count)
	return nil
}
