package kafka

import (
	"sync"
	"time"
	"vectorized/pkg/redpanda/sandbox"

	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
)

func WaitForSandbox(s sandbox.Sandbox) error {
	log.Debug("Waiting for sandbox to be ready")
	nodes, err := s.Nodes()
	if err != nil {
		return err
	}
	var waitGroup sync.WaitGroup
	waitGroup.Add(len(nodes))
	for _, node := range nodes {
		endpoint := BrokerAddress(node)
		go func() {
			for {
				cfg := sarama.NewConfig()
				setupClientID(cfg)
				client, err := sarama.NewClient([]string{endpoint}, cfg)
				log.Debugf("Trying to connect to broker '%s'", endpoint)
				if err != nil {
					log.Debugf("Unable to connect - %s", err.Error())
					continue
				}
				defer client.Close()
				topics, err := client.Topics()
				if err == nil && topics != nil {
					log.Debugf("Connected to broker '%s'", endpoint)
					waitGroup.Done()
					return
				}
				time.Sleep(500 * time.Millisecond)
			}
		}()
	}
	log.Debugf("Waiting for sandbox to be ready...")
	waitGroup.Wait()
	return nil
}
