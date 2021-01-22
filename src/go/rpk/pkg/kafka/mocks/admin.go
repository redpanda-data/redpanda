package mocks

import "github.com/Shopify/sarama"

type MockAdmin struct {
	// add the specific funcs we'll need
	MockCreateTopic	func(
		topic string,
		detail *sarama.TopicDetail,
		validateOnly bool,
	) error

	MockListTopics	func() (map[string]sarama.TopicDetail, error)

	MockDescribeTopics	func(
		topics []string,
	) (metadata []*sarama.TopicMetadata, err error)

	MockDeleteTopic	func(topic string) error

	MockCreatePartitions	func(topic string,
		count int32,
		assignment [][]int32,
		validateOnly bool,
	) error

	MockDeleteRecords	func(
		topic string,
		partitionOffsets map[int32]int64,
	) error

	MockDescribeConfig	func(
		resource sarama.ConfigResource,
	) ([]sarama.ConfigEntry, error)

	MockAlterConfig	func(
		resourceType sarama.ConfigResourceType,
		name string,
		entries map[string]*string,
		validateOnly bool,
	) error

	MockCreateACL	func(
		resource sarama.Resource,
		acl sarama.Acl,
	) error

	MockListAcls	func(
		filter sarama.AclFilter,
	) ([]sarama.ResourceAcls, error)

	MockDeleteACL	func(
		filter sarama.AclFilter,
		validateOnly bool,
	) ([]sarama.MatchingAcl,
		error)
	MockListConsumerGroups	func() (map[string]string, error)

	MockDescribeConsumerGroups	func(groups []string) ([]*sarama.GroupDescription, error)

	MockListConsumerGroupOffsets	func(
		group string,
		topicPartitions map[string][]int32,
	) (*sarama.OffsetFetchResponse, error)
	MockDeleteConsumerGroup	func(group string) error

	MockDescribeCluster	func() (
		brokers []*sarama.Broker,
		controllerID int32,
		err error)
	MockClose	func() error
}

func (m MockAdmin) CreateTopic(
	topic string, detail *sarama.TopicDetail, validateOnly bool,
) error {
	if m.MockCreateTopic != nil {
		return m.MockCreateTopic(topic, detail, validateOnly)
	}
	return nil
}

func (m MockAdmin) ListTopics() (map[string]sarama.TopicDetail, error) {
	if m.MockListTopics != nil {
		return m.MockListTopics()
	}
	return map[string]sarama.TopicDetail{}, nil
}

func (m MockAdmin) DescribeTopics(
	topics []string,
) (metadata []*sarama.TopicMetadata, err error) {
	if m.MockDescribeTopics != nil {
		return m.MockDescribeTopics(topics)
	}
	return []*sarama.TopicMetadata{{}}, nil
}

func (m MockAdmin) DeleteTopic(topic string) error {
	if m.MockDeleteTopic != nil {
		return m.MockDeleteTopic(topic)
	}
	return nil
}

func (m MockAdmin) CreatePartitions(
	topic string, count int32, assignment [][]int32, validateOnly bool,
) error {
	if m.MockCreatePartitions != nil {
		return m.MockCreatePartitions(topic, count, assignment, validateOnly)
	}
	return nil
}

func (m MockAdmin) DeleteRecords(
	topic string, partitionOffsets map[int32]int64,
) error {
	if m.MockDeleteRecords != nil {
		return m.MockDeleteRecords(topic, partitionOffsets)
	}
	return nil
}

func (m MockAdmin) DescribeConfig(
	resource sarama.ConfigResource,
) ([]sarama.ConfigEntry, error) {
	if m.MockDescribeConfig != nil {
		return m.MockDescribeConfig(resource)
	}
	return []sarama.ConfigEntry{
		{
			Name:		"cleanup.policy",
			Value:		"compact",
			Default:	true,
		},
		{
			Name:	"key",
			Value:	"value",
		},
	}, nil
}

func (m MockAdmin) AlterConfig(
	resourceType sarama.ConfigResourceType,
	name string,
	entries map[string]*string,
	validateOnly bool,
) error {
	if m.MockAlterConfig != nil {
		return m.MockAlterConfig(resourceType, name, entries, validateOnly)
	}
	return nil
}

func (m MockAdmin) CreateACL(resource sarama.Resource, acl sarama.Acl) error {
	if m.MockCreateACL != nil {
		return m.MockCreateACL(resource, acl)
	}
	return nil
}

func (m MockAdmin) ListAcls(
	filter sarama.AclFilter,
) ([]sarama.ResourceAcls, error) {
	if m.MockListAcls != nil {
		return m.MockListAcls(filter)
	}
	return []sarama.ResourceAcls{{}}, nil
}

func (m MockAdmin) DeleteACL(
	filter sarama.AclFilter, validateOnly bool,
) ([]sarama.MatchingAcl, error) {
	if m.MockDeleteACL != nil {
		return m.MockDeleteACL(filter, validateOnly)
	}
	return []sarama.MatchingAcl{{}}, nil
}

func (m MockAdmin) ListConsumerGroups() (map[string]string, error) {
	if m.MockListConsumerGroups != nil {
		return m.MockListConsumerGroups()
	}
	return map[string]string{}, nil
}

func (m MockAdmin) DescribeConsumerGroups(
	groups []string,
) ([]*sarama.GroupDescription, error) {
	if m.MockDescribeConsumerGroups != nil {
		return m.MockDescribeConsumerGroups(groups)
	}
	return []*sarama.GroupDescription{{}}, nil
}

func (m MockAdmin) ListConsumerGroupOffsets(
	group string, topicPartitions map[string][]int32,
) (*sarama.OffsetFetchResponse, error) {
	if m.MockListConsumerGroupOffsets != nil {
		return m.MockListConsumerGroupOffsets(group, topicPartitions)
	}
	return nil, nil
}

func (m MockAdmin) DeleteConsumerGroup(group string) error {
	if m.MockDeleteConsumerGroup != nil {
		return m.MockDeleteConsumerGroup(group)
	}
	return nil
}

func (m MockAdmin) DescribeCluster() (
	brokers []*sarama.Broker,
	controllerID int32,
	err error,
) {
	if m.MockDescribeCluster != nil {
		return m.MockDescribeCluster()
	}
	return []*sarama.Broker{{}}, 0, nil
}

func (m MockAdmin) Close() error {
	if m.MockClose != nil {
		return m.MockClose()
	}
	return nil
}
