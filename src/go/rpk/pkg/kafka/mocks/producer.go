package mocks

import "github.com/Shopify/sarama"

type MockProducer struct {
	// add the specific funcs we'll need
	MockSendMessage		func(msg *sarama.ProducerMessage) (partition int32, offset int64, err error)
	MockSendMessages	func(msgs []*sarama.ProducerMessage) error
	MockClose		func() error
}

func (m MockProducer) SendMessage(
	msg *sarama.ProducerMessage,
) (partition int32, offset int64, err error) {
	if m.MockSendMessage != nil {
		return m.MockSendMessage(msg)
	}
	return int32(0), 0, nil
}

func (m MockProducer) SendMessages(msg []*sarama.ProducerMessage) (err error) {
	if m.MockSendMessages != nil {
		return m.MockSendMessages(msg)
	}
	return nil
}

func (m MockProducer) Close() (err error) {
	if m.MockClose != nil {
		return m.MockClose()
	}
	return nil
}
