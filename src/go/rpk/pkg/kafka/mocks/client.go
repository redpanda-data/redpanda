package mocks

import (
	"errors"

	"github.com/Shopify/sarama"
)

type MockClient struct {
	MockConfig             func() *sarama.Config
	MockController         func() (*sarama.Broker, error)
	MockRefreshController  func() (*sarama.Broker, error)
	MockBrokers            func() []*sarama.Broker
	MockBroker             func(brokerID int32) (*sarama.Broker, error)
	MockTopics             func() ([]string, error)
	MockPartitions         func(topic string) ([]int32, error)
	MockWritablePartitions func(topic string) ([]int32, error)
	MockLeader             func(topic string, partitionID int32) (*sarama.Broker, error)
	MockReplicas           func(topic string, partitionID int32) ([]int32, error)
	MockInSyncReplicas     func(topic string, partitionID int32) ([]int32, error)
	MockOfflineReplicas    func(topic string, partitionID int32) ([]int32, error)
	MockRefreshMetadata    func(topics ...string) error
	MockRefreshBrokers     func(addrs []string) error
	MockGetOffset          func(topic string, partition int32, time int64) (int64, error)
	MockCoordinator        func(consumerGroup string) (*sarama.Broker, error)
	MockRefreshCoordinator func(consumerGroup string) error
	MockInitProducerID     func() (*sarama.InitProducerIDResponse, error)
	MockClose              func() error
	MockClosed             func() bool
}

func (m MockClient) Config() *sarama.Config {
	if m.MockConfig != nil {
		return m.MockConfig()
	}
	return nil
}

func (m MockClient) Controller() (*sarama.Broker, error) {
	if m.MockController != nil {
		return m.MockController()
	}
	return nil, errors.New("Controller unimplemented")
}

func (m MockClient) RefreshController() (*sarama.Broker, error) {
	if m.MockRefreshController != nil {
		return m.MockRefreshController()
	}
	return nil, errors.New("RefreshController unimplemented")
}

func (m MockClient) Brokers() []*sarama.Broker {
	if m.MockBrokers != nil {
		return m.MockBrokers()
	}
	return nil
}

func (m MockClient) Broker(brokerID int32) (*sarama.Broker, error) {
	if m.MockBroker != nil {
		return m.MockBroker(brokerID)
	}
	return nil, nil
}

func (m MockClient) Topics() ([]string, error) {
	if m.MockTopics != nil {
		return m.MockTopics()
	}
	return nil, errors.New("Topics unimplemented")
}

func (m MockClient) Partitions(topic string) ([]int32, error) {
	if m.MockPartitions != nil {
		return m.MockPartitions(topic)
	}
	return nil, errors.New("Partitions unimplemented")
}

func (m MockClient) WritablePartitions(topic string) ([]int32, error) {
	if m.MockWritablePartitions != nil {
		return m.MockWritablePartitions(topic)
	}
	return nil, errors.New("WritablePartitions unimplemented")
}

func (m MockClient) Leader(
	topic string, partition int32,
) (*sarama.Broker, error) {
	if m.MockLeader != nil {
		return m.MockLeader(topic, partition)
	}
	return nil, errors.New("Leader unimplemented")
}

func (m MockClient) Replicas(topic string, partition int32) ([]int32, error) {
	if m.MockReplicas != nil {
		return m.MockReplicas(topic, partition)
	}
	return nil, errors.New("Replicas unimplemented")
}

func (m MockClient) InSyncReplicas(
	topic string, partition int32,
) ([]int32, error) {
	if m.MockInSyncReplicas != nil {
		return m.MockInSyncReplicas(topic, partition)
	}
	return nil, errors.New("InSyncReplicas unimplemented")
}

func (m MockClient) OfflineReplicas(
	topic string, partition int32,
) ([]int32, error) {
	if m.MockOfflineReplicas != nil {
		return m.MockOfflineReplicas(topic, partition)
	}
	return nil, errors.New("OfflineReplicas unimplemented")
}

func (m MockClient) RefreshMetadata(topics ...string) error {
	if m.MockRefreshMetadata != nil {
		return m.MockRefreshMetadata(topics...)
	}
	return errors.New("RefreshMetadata unimplemented")
}

func (m MockClient) RefreshBrokers(addrs []string) error {
	if m.MockRefreshBrokers != nil {
		return m.MockRefreshBrokers(addrs)
	}
	return nil
}

func (m MockClient) GetOffset(
	topic string, partition int32, time int64,
) (int64, error) {
	if m.MockGetOffset != nil {
		return m.MockGetOffset(topic, partition, time)
	}
	return -1, errors.New("GetOffset unimplemented")
}

func (m MockClient) Coordinator(group string) (*sarama.Broker, error) {
	if m.MockCoordinator != nil {
		return m.MockCoordinator(group)
	}
	return nil, errors.New("Coordinator unimplemented")
}

func (m MockClient) RefreshCoordinator(group string) error {
	if m.MockRefreshCoordinator != nil {
		return m.MockRefreshCoordinator(group)
	}
	return errors.New("RefreshCoordinator unimplemented")
}

func (m MockClient) InitProducerID() (*sarama.InitProducerIDResponse, error) {
	if m.MockInitProducerID != nil {
		return m.MockInitProducerID()
	}
	return nil, errors.New("InitProducerID unimplemented")
}

func (m MockClient) Close() error {
	if m.MockClose != nil {
		return m.MockClose()
	}
	return errors.New("Close unimplemented")
}

func (m MockClient) Closed() bool {
	if m.MockClosed != nil {
		return m.MockClosed()
	}
	return false
}
