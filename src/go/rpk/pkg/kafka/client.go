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
	"crypto/sha256"
	"crypto/sha512"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
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
func LoadConfig(conf *config.Config) (*sarama.Config, error) {
	c := DefaultConfig()

	c, err := configureTLS(c, conf)
	if err != nil {
		return nil, err
	}

	return configureSASL(c, conf)
}

func InitClient(brokers ...string) (sarama.Client, error) {
	// sarama shuffles the addresses, so there's no need to do it.
	return sarama.NewClient(brokers, DefaultConfig())
}

// Initializes a client using values from the configuration when possible.
func InitClientWithConf(
	conf *config.Config, brokers ...string,
) (sarama.Client, error) {
	c, err := LoadConfig(conf)
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

func configureSASL(
	saramaConf *sarama.Config, rpConf *config.Config,
) (*sarama.Config, error) {
	rpk := rpConf.Rpk
	if rpk.SCRAM.Password == "" || rpk.SCRAM.User == "" || rpk.SCRAM.Type == "" {
		return saramaConf, nil
	}

	saramaConf.Net.SASL.Enable = true
	saramaConf.Net.SASL.Handshake = true
	saramaConf.Net.SASL.User = rpk.SCRAM.User
	saramaConf.Net.SASL.Password = rpk.SCRAM.Password
	switch rpk.SCRAM.Type {
	case sarama.SASLTypeSCRAMSHA256:
		saramaConf.Net.SASL.SCRAMClientGeneratorFunc =
			func() sarama.SCRAMClient {
				return &XDGSCRAMClient{HashGeneratorFcn: sha256.New}
			}
		saramaConf.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
	case sarama.SASLTypeSCRAMSHA512:
		saramaConf.Net.SASL.SCRAMClientGeneratorFunc =
			func() sarama.SCRAMClient {
				return &XDGSCRAMClient{HashGeneratorFcn: sha512.New}
			}
		saramaConf.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
	default:
		return nil, fmt.Errorf("unrecongnized Salted Challenge Response "+
			"Authentication Mechanism (SCRAM) under rpk.scram.type: %s", rpk.SCRAM.Type)
	}
	return saramaConf, nil
}

// Configures TLS for the Redpanda API IF either rpk.tls or
// redpanda.kafka_api_tls are set in the configuration. Doesn't modify the
// configuration otherwise.
func configureTLS(
	saramaConf *sarama.Config, rpConf *config.Config,
) (*sarama.Config, error) {
	var tlsConf *tls.Config
	var err error
	rpkTls := rpConf.Rpk.TLS

	// Try to configure TLS from the available config
	switch {
	// Enable client auth if the cert & key files are set for rpk
	case rpkTls.CertFile != "" &&
		rpkTls.KeyFile != "" &&
		rpkTls.TruststoreFile != "":

		tlsConf, err = loadTLSConfig(
			rpkTls.TruststoreFile,
			rpkTls.CertFile,
			rpkTls.KeyFile,
		)
		if err != nil {
			return nil, err
		}
		log.Debug("API TLS auth enabled using rpk.tls")

	// Enable TLS (no auth) if only the CA cert file is set for rpk
	case rpkTls.TruststoreFile != "":
		caCertPool, err := loadRootCACert(rpkTls.TruststoreFile)
		if err != nil {
			return nil, err
		}
		tlsConf = &tls.Config{RootCAs: caCertPool}
		log.Debug("API TLS enabled using rpk.tls")

	default:
		log.Debug(
			"Skipping API TLS auth config. Set The cert" +
				" file, key file and truststore file" +
				" to enable it",
		)
	}
	if tlsConf != nil {
		saramaConf.Net.TLS.Config = tlsConf
		saramaConf.Net.TLS.Enable = true
	}

	return saramaConf, nil
}

func loadTLSConfig(
	truststoreFile, certFile, keyFile string,
) (*tls.Config, error) {
	caCertPool, err := loadRootCACert(truststoreFile)
	if err != nil {
		return nil, err
	}
	certs, err := loadCert(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	tlsConf := &tls.Config{RootCAs: caCertPool, Certificates: certs}

	tlsConf.BuildNameToCertificate()
	return tlsConf, nil
}

func loadRootCACert(truststoreFile string) (*x509.CertPool, error) {
	caCert, err := ioutil.ReadFile(truststoreFile)
	if err != nil {
		return nil, err
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)
	return caCertPool, nil
}

func loadCert(certFile, keyFile string) ([]tls.Certificate, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	return []tls.Certificate{cert}, nil
}
