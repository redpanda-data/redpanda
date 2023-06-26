package v1alpha1

// These constants define valid event severity values.
const (
	// EventTopicCreationFailure indicate and error when topic creation
	// was not successful.
	EventTopicCreationFailure string = "topicCreationFailure"
	// EventTopicDeletionFailure indicate and error when topic deletion
	// was not successful.
	EventTopicDeletionFailure string = "topicDeletionFailure"
	// EventTopicConfigurationAlteringFailure indicate and error when topic configuration altering
	// was not successful.
	EventTopicConfigurationAlteringFailure string = "topicConfigurationAlteringFailure"
	// EventTopicConfigurationDescribeFailure indicate and error when topic configuration describe
	// was not successful.
	EventTopicConfigurationDescribeFailure string = "topicConfigurationDescribeFailure"
	// EventTopicAlreadySynced indicate topic is already synced
	EventTopicAlreadySynced string = "topicAlreadySynced"
)
