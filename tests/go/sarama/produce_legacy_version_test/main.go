package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Shopify/sarama"
)

var (
	brokers          = flag.String("brokers", "127.0.0.1:9092", "The Redpanda brokers to connect to, as a comma separated list")
	count            = flag.Int64("count", 100, "Optional count to run")
	compression_type = flag.String("compression_type", "none", "compression.type to use")
)

func parseCompression(name string) (sarama.CompressionCodec, error) {
	switch strings.ToLower(name) {
	case "none":
		return sarama.CompressionNone, nil
	case "gzip":
		return sarama.CompressionGZIP, nil
	case "snappy":
		return sarama.CompressionSnappy, nil
	case "lz4":
		return sarama.CompressionLZ4, nil
	case "zstd":
		return sarama.CompressionZSTD, nil
	default:
		return sarama.CompressionNone, fmt.Errorf("unknown compression type: %s", name)
	}
}

func main() {
	flag.Parse()

	brokerList := strings.Split(*brokers, ",")
	fmt.Printf("Redpanda brokers: %s\n", strings.Join(brokerList, ", "))

	topics := []string{
		"topic",
	}

	ctype, err := parseCompression(*compression_type)
	if err != nil {
		log.Fatalf("failed to parse compression: %v", err)
	}

	producer, err := newProducer(brokerList, ctype)
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

func newProducer(brokers []string, compression_type sarama.CompressionCodec) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()

	config.Metadata.Full = false
	config.Producer.Compression = compression_type
	config.Producer.Return.Successes = true

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
