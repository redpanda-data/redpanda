package kafka

import (
	"vectorized/pkg/redpanda/sandbox"

	"github.com/Shopify/sarama"
)

type ClientFactory interface {
	NewConsumer(config *sarama.Config) sarama.Consumer
	NewSyncProducer(config *sarama.Config) sarama.SyncProducer
	NewClusterAdmin(config *sarama.Config) sarama.ClusterAdmin
}

func NewSboxClientFactory(s sandbox.Sandbox) ClientFactory {
	return &sandboxClientFactory{
		s: s,
	}
}

func NewAddressClientFactory(address string) ClientFactory {
	return &addressClientFactory{
		addr: address,
	}
}

type sandboxClientFactory struct {
	s sandbox.Sandbox
}

func (f *sandboxClientFactory) NewConsumer(
	config *sarama.Config,
) sarama.Consumer {
	return CreateConsumer(f.s, config)
}

func (f *sandboxClientFactory) NewSyncProducer(
	config *sarama.Config,
) sarama.SyncProducer {
	return CreateProducer(f.s, config)
}

func (f *sandboxClientFactory) NewClusterAdmin(
	config *sarama.Config,
) sarama.ClusterAdmin {
	admin, err := sarama.NewClusterAdmin(BrokerAddresses(f.s), config)
	panicOnError(err)
	return admin
}

type addressClientFactory struct {
	addr string
}

func (f *addressClientFactory) NewConsumer(
	config *sarama.Config,
) sarama.Consumer {
	return createConsumer([]string{f.addr}, config)
}

func (f *addressClientFactory) NewSyncProducer(
	config *sarama.Config,
) sarama.SyncProducer {
	return createProducer([]string{f.addr}, config)
}

func (f *addressClientFactory) NewClusterAdmin(
	config *sarama.Config,
) sarama.ClusterAdmin {
	admin, err := sarama.NewClusterAdmin([]string{f.addr}, config)
	panicOnError(err)
	return admin
}
