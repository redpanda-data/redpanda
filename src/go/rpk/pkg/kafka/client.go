package kafka

import (
	"time"

	"github.com/Shopify/sarama"
)

func InitClient(brokers ...string) (sarama.Client, error) {
	saramaConf := sarama.NewConfig()
	saramaConf.Version = sarama.V2_4_0_0
	saramaConf.Producer.Return.Successes = true
	timeout := 1 * time.Second
	saramaConf.Admin.Timeout = timeout
	saramaConf.Metadata.Timeout = timeout
	// sarama shuffles the addresses, so there's no need to do it.
	return sarama.NewClient(brokers, saramaConf)
}
