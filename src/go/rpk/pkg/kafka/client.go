// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package kafka

import (
	"crypto/tls"
	"fmt"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/avast/retry-go"
	log "github.com/sirupsen/logrus"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/config"
)

func DefaultConfig() *sarama.Config {
	timeout := 1 * time.Second
	conf := sarama.NewConfig()

	conf.Version = sarama.V2_4_0_0
	conf.ClientID = "rpk"

	conf.Admin.Timeout = timeout

	conf.Metadata.Timeout = timeout

	conf.Consumer.Return.Errors = true

	conf.Producer.RequiredAcks = sarama.WaitForAll
	conf.Producer.Retry.Backoff = 2 * time.Second
	conf.Producer.Retry.Max = 3
	conf.Producer.Return.Successes = true
	conf.Producer.Timeout = timeout

	return conf
}

// Overrides the default config with the redpanda config values, such as TLS.
func LoadConfig(tls *tls.Config, sasl *config.SASL) (*sarama.Config, error) {
	var err error
	c := DefaultConfig()

	if tls != nil {
		c, err = configureTLS(c, tls)
		if err != nil {
			return nil, err
		}
	}

	if sasl != nil {
		return ConfigureSASL(c, sasl)
	}
	return c, nil
}

func InitClient(brokers ...string) (sarama.Client, error) {
	// sarama shuffles the addresses, so there's no need to do it.
	return sarama.NewClient(brokers, DefaultConfig())
}

// Initializes a client using values from the configuration when possible.
func InitClientWithConf(
	tls *tls.Config, sasl *config.SASL, brokers ...string,
) (sarama.Client, error) {
	c, err := LoadConfig(tls, sasl)
	if err != nil {
		return nil, err
	}
	// sarama shuffles the addresses, so there's no need to do it.
	return sarama.NewClient(brokers, c)
}

/*
 * Gets the offset of the last message that was successfully copied to all
 * of the replicas.
 */
func HighWatermarks(
	client sarama.Client, topic string, partitionIDs []int32,
) (map[int32]int64, error) {
	leaders := make(map[*sarama.Broker][]int32)

	// Get the leader for each partition
	for _, partition := range partitionIDs {
		leader, err := client.Leader(topic, partition)
		if err != nil {
			errMsg := "Unable to get available offsets for" +
				" partition without leader." +
				" Topic: '%s', partition: '%d': %v"
			return nil, fmt.Errorf(errMsg, topic, partition, err)
		}
		leaders[leader] = append(leaders[leader], partition)
	}
	wg := sync.WaitGroup{}
	wg.Add(len(leaders))

	type result struct {
		watermarks map[int32]int64
		err        error
	}

	results := make(chan result, len(leaders))

	for leader, partitionIDs := range leaders {
		req := &sarama.OffsetRequest{
			Version: int16(1),
		}

		for _, partition := range partitionIDs {
			// Request each partition's offsets
			req.AddBlock(topic, partition, int64(-1), int32(0))
		}

		// Query leaders concurrently
		go func(leader *sarama.Broker, req *sarama.OffsetRequest) {
			resp, err := leader.GetAvailableOffsets(req)
			if err != nil {
				err := fmt.Errorf("Unable to get available offsets: %v", err)
				results <- result{err: err}
				return
			}

			watermarks := make(map[int32]int64)
			for partition, block := range resp.Blocks[topic] {
				watermarks[partition] = block.Offset
			}

			results <- result{watermarks: watermarks}
			wg.Done()

		}(leader, req)
	}

	wg.Wait()
	close(results)

	watermarks := make(map[int32]int64)
	// Collect the results
	for res := range results {
		if res.err != nil {
			return nil, res.err
		}
		for partition, offset := range res.watermarks {
			watermarks[partition] = offset
		}
	}

	return watermarks, nil
}

// Tries sending a message, and retries `retries` times waiting `backoff` time
// in between.
// sarama implements retries and backoff, but only for some errors.
func RetrySend(
	producer sarama.SyncProducer,
	message *sarama.ProducerMessage,
	retries uint,
	backoff time.Duration,
) (part int32, offset int64, err error) {
	err = retry.Do(
		func() error {
			part, offset, err = producer.SendMessage(message)
			return err
		},
		retry.Attempts(retries),
		retry.DelayType(retry.FixedDelay),
		retry.Delay(backoff),
		retry.LastErrorOnly(true),
		retry.OnRetry(func(n uint, err error) {
			log.Debugf("Sending message failed: %v", err)
			log.Debugf("Retrying (%d retries left)", retries-n)
		}),
	)
	return part, offset, err
}

func ConfigureSASL(
	saramaConf *sarama.Config, sasl *config.SASL,
) (*sarama.Config, error) {
	if sasl.Password == "" || sasl.User == "" || sasl.Mechanism == "" {
		return saramaConf, nil
	}

	saramaConf.Net.SASL.Enable = true
	saramaConf.Net.SASL.Handshake = true
	saramaConf.Net.SASL.User = sasl.User
	saramaConf.Net.SASL.Password = sasl.Password
	switch sasl.Mechanism {
	case sarama.SASLTypeSCRAMSHA256:
		saramaConf.Net.SASL.SCRAMClientGeneratorFunc =
			func() sarama.SCRAMClient {
				return &XDGSCRAMClient{HashGeneratorFcn: SHA256}
			}
		saramaConf.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
	case sarama.SASLTypeSCRAMSHA512:
		saramaConf.Net.SASL.SCRAMClientGeneratorFunc =
			func() sarama.SCRAMClient {
				return &XDGSCRAMClient{HashGeneratorFcn: SHA512}
			}
		saramaConf.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
	default:
		return nil, fmt.Errorf("unrecongnized Salted Challenge Response "+
			"Authentication Mechanism (SCRAM): '%s'.", sasl.Mechanism)
	}
	return saramaConf, nil
}

// Configures TLS for the Redpanda API IF either rpk.tls or
// redpanda.kafka_api_tls are set in the configuration. Doesn't modify the
// configuration otherwise.
func configureTLS(
	saramaConf *sarama.Config, tlsConfig *tls.Config,
) (*sarama.Config, error) {
	if tlsConfig != nil {
		saramaConf.Net.TLS.Config = tlsConfig
		saramaConf.Net.TLS.Enable = true
	}

	return saramaConf, nil
}
