package kafka

import (
	"fmt"
	"vectorized/pkg/redpanda/sandbox"

	"github.com/Shopify/sarama"
)

func BrokerAddresses(s sandbox.Sandbox) []string {
	var brokers []string
	nodes, err := s.Nodes()
	panicOnError(err)
	for _, node := range nodes {
		brokers = append(brokers, BrokerAddress(node))
	}
	return brokers
}

func BrokerAddress(n sandbox.Node) string {
	state, err := n.State()
	panicOnError(err)

	return fmt.Sprintf("127.0.0.1:%d", state.HostKafkaPort)
}

func panicOnError(err error) {
	if err != nil {
		panic(err)
	}
}

func setupClientID(cfg *sarama.Config) {
	if cfg.ClientID == "" || cfg.ClientID == "sarama" {
		cfg.ClientID = "v_test_client"
	}
}
