package main

import (
	"context"
	"flag"
	"log"
	"math"
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
	initialOffset = flag.Int64("initialOffset", math.MinInt64, "expected initial offset")
	markOffset    = flag.Int64("markOffset", math.MinInt64, "offset to mark as consumed")
	logger        = log.New(os.Stderr, "", log.LstdFlags)
	consumergroup sarama.ConsumerGroup
	retval        = 1
)

func getTopic() string {
	if *topic == "" {
		return *namespace + ".topic"
	}
	return *topic
}

func getConsumerName() string {
	return *namespace + "-group"
}

func main() {
	flag.Parse()

	config := sarama.NewConfig()
	config.Version = sarama.V2_2_0_0
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.ClientID = getConsumerName() + "-client"

	var err error
	client, err := sarama.NewClient(strings.Split(*brokers, ","), config)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	consumergroup, err = sarama.NewConsumerGroupFromClient(getConsumerName(), client)
	if err != nil {
		panic(err)
	}
	defer consumergroup.Close()

	done := make(chan struct{})
	go ProcessingLoop(done)

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-shutdown:
		break
	case <-done:
		break
	}

	os.Exit(retval)
}

func ProcessingLoop(done chan struct{}) {
	ctx := context.Background()

	for {
		handler := &KafkaConsumerGroupHandler{}
		err := consumergroup.Consume(ctx, []string{getTopic()}, handler)
		if err != nil {
			logger.Println("ProcessingLoop err:", err)
			break
		} else {
			logger.Println("Consume no err")
			break
		}
	}

	close(done)
}

type KafkaConsumerGroupHandler struct{}

func (KafkaConsumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (KafkaConsumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (h KafkaConsumerGroupHandler) ConsumeClaim(
	sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim,
) error {
	logger.Println("ConsumeClaim initial ", claim.InitialOffset(), "expected initial ", *initialOffset, " mark offset ", *markOffset)
	if claim.InitialOffset() == *initialOffset {
		retval = 0
	}
	if *markOffset >= 0 {
		sess.MarkOffset(getTopic(), 0, *markOffset, "a")
	}
	return nil
}
