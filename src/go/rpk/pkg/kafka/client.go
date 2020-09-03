package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"sync"
	"time"
	"vectorized/pkg/config"

	"github.com/Shopify/sarama"
	"github.com/avast/retry-go"
	log "github.com/sirupsen/logrus"
)

func DefaultConfig() *sarama.Config {
	timeout := 1 * time.Second
	conf := sarama.NewConfig()

	conf.Version = sarama.V2_4_0_0
	conf.ClientID = "rpk"

	conf.Admin.Timeout = timeout

	conf.Metadata.Timeout = timeout

	conf.Producer.RequiredAcks = sarama.WaitForAll
	conf.Producer.Retry.Backoff = 2 * time.Second
	conf.Producer.Retry.Max = 3
	conf.Producer.Return.Successes = true
	conf.Producer.Timeout = timeout

	return conf
}

func InitClient(brokers ...string) (sarama.Client, error) {
	// sarama shuffles the addresses, so there's no need to do it.
	return sarama.NewClient(brokers, DefaultConfig())
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

// Configures TLS for the Redpanda API IF either rpk.tls or
// redpanda.kafka_api_tls are set in the configuration. Doesn't modify the
// configuration otherwise.
func configureTLS(
	saramaConf *sarama.Config, rpConf *config.Config,
) (*sarama.Config, error) {
	rpkTls := rpConf.Rpk.TLS

	if rpkTls.CertFile != "" &&
		rpkTls.KeyFile != "" &&
		rpkTls.TruststoreFile != "" {

		tlsConf, err := tlsConfig(
			rpkTls.TruststoreFile,
			rpkTls.CertFile,
			rpkTls.KeyFile,
		)
		if err != nil {
			return nil, err
		}

		saramaConf.Net.TLS.Config = tlsConf
		saramaConf.Net.TLS.Enable = true

		log.Debug("API TLS auth enabled using rpk.tls")
		return saramaConf, nil
	}
	rpTls := rpConf.Redpanda.KafkaApiTLS
	if rpTls.Enabled &&
		rpTls.CertFile != "" &&
		rpTls.KeyFile != "" &&
		rpTls.TruststoreFile != "" {

		tlsConf, err := tlsConfig(
			rpTls.TruststoreFile,
			rpTls.CertFile,
			rpTls.KeyFile,
		)
		if err != nil {
			return nil, err
		}

		saramaConf.Net.TLS.Config = tlsConf
		saramaConf.Net.TLS.Enable = true

		log.Debug("API TLS auth enabled using redpanda.kafka_api_tls")
		return saramaConf, nil
	}
	log.Debug(
		"Skipping API TLS auth config. Set The cert" +
			" file, key file and truststore file" +
			" to enable it",
	)
	return saramaConf, nil
}

func tlsConfig(truststoreFile, certFile, keyFile string) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	caCert, err := ioutil.ReadFile(truststoreFile)
	if err != nil {
		return nil, err
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	tlsConf := &tls.Config{}

	tlsConf.RootCAs = caCertPool
	tlsConf.Certificates = []tls.Certificate{cert}
	tlsConf.BuildNameToCertificate()
	return tlsConf, nil
}
