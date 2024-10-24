/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package main

import (
	"context"
	"encoding/json"
	"flag"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/google/uuid"

	log "github.com/sirupsen/logrus"

	"github.com/redpanda-data/kgo-verifier/pkg/util"
	"golang.org/x/exp/maps"
	"golang.org/x/sync/errgroup"
)

var (
	brokers        = flag.String("brokers", "localhost:9092", "comma delimited list of brokers")
	topic          = flag.String("topic", uuid.New().String(), "topic to produce/consume from")
	debug          = flag.Bool("debug", false, "Enable verbose logging")
	sr_addr        = flag.String("schema-registry", "http://127.0.0.1:8081", "URL of schema registry")
	consumer_group = flag.String("consumer-group", uuid.New().String(), "Consumer group to use")
	protocol       = flag.String("protocol", "AVRO", "Protocol to use.  Must be AVRO or PROTOBUF")
	count          = flag.Int("count", 1, "Number of messages to produce and consume")
	security       = flag.String("security", "", "Security settings")
	timeout        = flag.Int("timeout", 30, "Timeout (in seconds) to wait for messages")

	protocolMap = map[string]Protocol{
		"AVRO":     AVRO,
		"PROTOBUF": PROTOBUF,
	}
)

type TestSerializer interface {
	CreateSerializedData(msgNum int, topic *string) ([]byte, error)
	DeserializeAndCheck(msgNum int, topic *string, buf []byte) (bool, error)
}

type TestClient struct {
	serializer TestSerializer
	srClient   schemaregistry.Client
	producer   *kafka.Producer
	consumer   *kafka.Consumer
	count      int
	timeout    time.Duration
	topic      string
}

type SecuritySettings struct {
	SecurityProtocol string `json:"security_protocol"`
	SaslMechanism    string `json:"sasl_mechanism"`
	SaslUsername     string `json:"sasl_plain_username"`
	SaslPassword     string `json:"sasl_plain_password"`
}

func main() {
	flag.Parse()

	if *debug {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}

	var securitySettings *SecuritySettings = nil

	if *security != "" {
		securitySettings = new(SecuritySettings)
		err := json.Unmarshal([]byte(*security), securitySettings)

		util.Chk(err, "Failed to parse security settings (\"%v\"): %v", *security, err)
	}

	log.Debugf("Brokers: %v", *brokers)
	log.Debugf("Topic: %v", *topic)
	log.Debugf("SR Addr: %v", *sr_addr)
	log.Debugf("Consumer Group: %v", *consumer_group)
	log.Debugf("Protocol: %v", *protocol)
	log.Debugf("Count: %v", *count)
	log.Debugf("Security Settings: %v", securitySettings)
	log.Debugf("Timeout: %v", *timeout)

	prot, ok := ParseProtocol(*protocol)

	if !ok {
		util.Die("Failed to parse protocol %v", *protocol)
	}

	tc, err := CreateTestClient(brokers, topic, sr_addr, consumer_group, prot, *count, time.Duration(*timeout)*time.Second, securitySettings)

	util.Chk(err, "Failed to create test client: %v", err)

	log.Info("Running test!")

	err = tc.RunTest()

	util.Chk(err, "Failed to execute test: %v", err)

	log.Info("Test complete!")
}

func CreateTestClient(brokers *string,
	topic *string,
	srAddr *string,
	consumerGroup *string,
	protocol Protocol,
	count int,
	timeout time.Duration,
	securitySettings *SecuritySettings,
) (tc *TestClient, err error) {

	producerConfig := make(kafka.ConfigMap)

	producerConfig["bootstrap.servers"] = *brokers

	if securitySettings != nil {
		maps.Copy(producerConfig, securitySettings.GenerateConfig())
	}

	p, err := kafka.NewProducer(&producerConfig)
	if err != nil {
		return nil, err
	}

	consumerConfig := make(kafka.ConfigMap)

	consumerConfig["bootstrap.servers"] = *brokers
	consumerConfig["group.id"] = *consumerGroup
	consumerConfig["session.timeout.ms"] = 6000
	consumerConfig["auto.offset.reset"] = "earliest"

	if securitySettings != nil {
		maps.Copy(consumerConfig, securitySettings.GenerateConfig())
	}

	c, err := kafka.NewConsumer(&consumerConfig)

	if err != nil {
		return nil, err
	}

	client, err := schemaregistry.NewClient(schemaregistry.NewConfig(*srAddr))

	if err != nil {
		return nil, err
	}

	tc = new(TestClient)

	tc.srClient = client
	tc.producer = p
	tc.consumer = c
	tc.count = count
	tc.timeout = timeout
	tc.topic = *topic

	switch protocol {
	case AVRO:
		serializer, err := NewAvroSerializer(&client)
		if err != nil {
			return nil, err
		}
		tc.serializer = serializer
	case PROTOBUF:
		serializer, err := NewProtobufSerializer(&client)
		if err != nil {
			return nil, err
		}
		tc.serializer = serializer
	}

	return tc, nil
}

type Protocol string

const (
	AVRO     Protocol = "AVRO"
	PROTOBUF Protocol = "PROTOBUF"
)

func ParseProtocol(s string) (c Protocol, ok bool) {
	c, ok = protocolMap[s]

	return c, ok
}

func (tc *TestClient) RunTest() error {

	err := tc.produceMessages()

	log.Debug("Produce done")

	if err != nil {
		return err
	}

	err = tc.consumeMessages()

	log.Debug("Consumption done")

	if err != nil {
		return err
	}

	return nil
}

func (tc *TestClient) consumeMessages() error {
	numConsumed := 0

	log.Debugf("Subscribing to topic %v", tc.topic)
	err := tc.consumer.Subscribe(tc.topic, nil)

	if err != nil {
		return err
	}

	end := time.Now().Add(tc.timeout)

	log.Debugf("Starting consumption of %v messages", tc.count)
	for numConsumed < tc.count && time.Now().Before(end) {
		ev := tc.consumer.Poll(100)

		if ev == nil {
			continue
		}

		switch e := ev.(type) {
		case *kafka.Message:
			result, err := tc.serializer.DeserializeAndCheck(numConsumed, &tc.topic, e.Value)

			if err != nil {
				return err
			}

			if !result {
				util.Die("Failed to get a message in order")
			}

			numConsumed++
		case *kafka.Error:
			return e
		default:
			log.Debugf("Ignoring unknown message: %v", e)
		}
	}

	log.Debugf("Consumed %v messages", numConsumed)

	if numConsumed != tc.count {
		util.Die("Did not consume expected number of messages: %v != %v", numConsumed, tc.count)
	}

	return nil
}

func (tc *TestClient) produceMessages() error {
	numDelivered := 0

	// Create a cancellable context that will monitor for producer events and track
	// the number of ack'ed produced messages.  Create a second context that will start
	// after all the messages are sent that will time out after a certain period of time
	// or will be cancelled once all produced messages are ack'ed.

	produceCheckContext, cancelProduceCheck := context.WithCancel(context.Background())
	produceCheckGroup, produceCheckContext := errgroup.WithContext(produceCheckContext)
	timeoutContext, timeoutCancel := context.WithCancel(produceCheckContext)

	monitorProduceFn := func(ctx context.Context) func() error {
		return func() error {
			defer timeoutCancel()
			for {
				select {
				case <-ctx.Done():
					log.Debugf("Monitor Produce received done signal: %v", ctx.Err())
					return nil
				case e := <-tc.producer.Events():
					switch ev := e.(type) {
					case *kafka.Message:
						m := ev

						if m.TopicPartition.Error != nil {
							log.Errorf("Failed to produce to topic %v: %v", tc.topic, m.TopicPartition.Error)
						} else {
							numDelivered++

							if numDelivered == tc.count {
								log.Infof("Produced expected number of messages")
								return nil
							}
						}
					case kafka.Error:
						log.Errorf("Failed to produce to topic %v: %v", tc.topic, ev)
						return ev
					default:
						log.Debugf("Ignoring event %v", ev)
					}
				}
			}
		}
	}

	produceCheckGroup.Go(monitorProduceFn(produceCheckContext))

	defer cancelProduceCheck()

	for i := 0; i < tc.count; i++ {
		payload_value, err := tc.serializer.CreateSerializedData(i, &tc.topic)
		if err != nil {
			return err
		}
		log.Debugf("payload: %v", payload_value)
		err = tc.producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &tc.topic, Partition: kafka.PartitionAny},
			Value:          payload_value,
		}, nil)

		if err != nil {
			return err
		}
	}

	produceCheckGroup.Go(func() error {
		select {
		case <-timeoutContext.Done():
			log.Debugf("Timeout context finished")
		case <-time.After(tc.timeout):
			log.Infof("Timeout expired!")
			cancelProduceCheck()

		}
		return nil
	})

	log.Debug("Waiting on producer check")
	err := produceCheckGroup.Wait()

	if err != nil {
		util.Die("Error during message production tracking: %v", err)
	}

	if numDelivered != tc.count {
		util.Die("Did not deliver enough messages: %v != %v", numDelivered, tc.count)
	}

	return nil
}

func (sr *SecuritySettings) GenerateConfig() (config kafka.ConfigMap) {
	config = make(kafka.ConfigMap)

	config["sasl.username"] = sr.SaslUsername
	config["sasl.password"] = sr.SaslPassword
	config["sasl.mechanism"] = sr.SaslMechanism
	config["security.protocol"] = sr.SecurityProtocol

	return config
}
