package mocks

import "github.com/Shopify/sarama"

type MockPartitionConsumer struct {
	MockAsyncClose          func()
	MockClose               func() error
	MockMessages            func() <-chan *sarama.ConsumerMessage
	MockErrors              func() <-chan *sarama.ConsumerError
	MockHighWaterMarkOffset func() int64
}

type MockConsumer struct {
	MockTopics           func() ([]string, error)
	MockPartitions       func(topic string) ([]int32, error)
	MockHighWaterMarks   func() map[string]map[int32]int64
	MockConsumePartition func(string, int32, int64) (sarama.PartitionConsumer, error)
	MockClose            func() error
}

func (m MockPartitionConsumer) AsyncClose() {
	if m.MockAsyncClose != nil {
		m.MockAsyncClose()
	}
}

func (m MockPartitionConsumer) Close() error {
	if m.MockClose != nil {
		return m.MockClose()
	}
	return nil
}

func (m MockPartitionConsumer) Messages() <-chan *sarama.ConsumerMessage {
	if m.MockMessages != nil {
		return m.MockMessages()
	}
	return nil
}

func (m MockPartitionConsumer) Errors() <-chan *sarama.ConsumerError {
	if m.MockErrors != nil {
		return m.MockErrors()
	}
	return nil
}

func (m MockPartitionConsumer) HighWaterMarkOffset() int64 {
	if m.MockHighWaterMarkOffset != nil {
		return m.MockHighWaterMarkOffset()
	}
	return -1
}

func (m MockConsumer) Topics() ([]string, error) {
	if m.MockTopics != nil {
		return m.MockTopics()
	}
	return []string{}, nil
}

func (m MockConsumer) Partitions(topic string) ([]int32, error) {
	if m.MockPartitions != nil {
		return m.MockPartitions(topic)
	}
	return []int32{}, nil
}

func (m MockConsumer) HighWaterMarks() map[string]map[int32]int64 {
	if m.MockHighWaterMarks != nil {
		return m.MockHighWaterMarks()
	}
	return map[string]map[int32]int64{}
}

func (m MockConsumer) ConsumePartition(
	topic string, beginOffset int32, endOffset int64,
) (sarama.PartitionConsumer, error) {
	if m.MockConsumePartition != nil {
		return m.MockConsumePartition(topic, beginOffset, endOffset)
	}
	return nil, nil
}

func (m MockConsumer) Close() (err error) {
	if m.MockClose != nil {
		return m.MockClose()
	}
	return nil
}
