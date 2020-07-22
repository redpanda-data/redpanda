package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/Shopify/sarama"
)

var (
	brokers       = flag.String("brokers", "localhost:9092", "broker list")
	namespace     = flag.String("namespace", "test", "testing namespace")
	topic         = flag.String("topic", "", "topic")
	logger        = log.New(os.Stderr, "", log.LstdFlags)
	maxOffset     = flag.Int64("maxOffset", 1000, "max offset to produce")
	consumergroup sarama.ConsumerGroup
	syncproducer  sarama.SyncProducer
)

func getTopic() string {
	if *topic == "" {
		return *namespace + ".topic"
	}
	return *topic
}

func main() {
	flag.Parse()

	var err error
	syncproducer, err = newSyncProducer(strings.Split(*brokers, ","))
	if err != nil {
		panic(err)
	}
	defer syncproducer.Close()

	done := make(chan struct{})
	go SendingLoop(done)

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-shutdown:
		break
	case <-done:
		break
	}
}

func newSyncProducer(brokers []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Retry.Max = 10
	config.Producer.Return.Successes = true
	config.Version = sarama.V2_2_0_0
	return sarama.NewSyncProducer(brokers, config)
}

func SendingLoop(done chan struct{}) {
	logger.Println("SendingLoop")
	for {
		partition, offset, err := syncproducer.SendMessage(&sarama.ProducerMessage{
			Topic: getTopic(),
			Key:   sarama.ByteEncoder([]byte{0x0}),
			Value: sarama.ByteEncoder([]byte{0x0}),
		})

		if err != nil {
			fmt.Printf("Error producing /%d/%d, error:, %s", partition, offset, err)
		}

		if offset >= *maxOffset {
			break
		}
	}
	close(done)
	logger.Println("Exiting SendingLoop()")
}
