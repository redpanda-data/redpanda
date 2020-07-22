package api

import (
	"bytes"
	"errors"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

type mockAdmin struct {
	// add an anonymous interface to trick Go into thinking that this struct
	// implements it 100%.
	sarama.ClusterAdmin
	// add the specific funcs we'll need
	createTopic    func(string, *sarama.TopicDetail, bool) error
	deleteTopic    func(string) error
	alterConfig    func(sarama.ConfigResourceType, string, map[string]*string, bool) error
	describeTopics func([]string) ([]*sarama.TopicMetadata, error)
	listTopics     func() (map[string]sarama.TopicDetail, error)
	describeConfig func(sarama.ConfigResource) ([]sarama.ConfigEntry, error)
}

type mockClient struct {
	// add an anonymous interface to trick Go into thinking that this struct
	// implements it 100%.
	sarama.Client
}

func (a *mockAdmin) CreateTopic(
	topic string, detail *sarama.TopicDetail, validateOnly bool,
) error {
	if a.createTopic != nil {
		return a.createTopic(topic, detail, validateOnly)
	}
	return nil
}

func (a *mockAdmin) DeleteTopic(topic string) error {
	if a.deleteTopic != nil {
		return a.deleteTopic(topic)
	}
	return nil
}

func (a *mockAdmin) AlterConfig(
	resType sarama.ConfigResourceType,
	topic string,
	config map[string]*string,
	validate bool,
) error {
	if a.alterConfig != nil {
		return a.alterConfig(resType, topic, config, validate)
	}
	return nil
}

func (a *mockAdmin) DescribeTopics(
	topics []string,
) ([]*sarama.TopicMetadata, error) {
	if a.describeTopics != nil {
		return a.describeTopics(topics)
	}
	return []*sarama.TopicMetadata{&sarama.TopicMetadata{}}, nil
}

func (a *mockAdmin) ListTopics() (map[string]sarama.TopicDetail, error) {
	if a.listTopics != nil {
		return a.listTopics()
	}
	return map[string]sarama.TopicDetail{}, nil
}

func (a *mockAdmin) DescribeConfig(
	res sarama.ConfigResource,
) ([]sarama.ConfigEntry, error) {
	if a.describeConfig != nil {
		return a.describeConfig(res)
	}
	return []sarama.ConfigEntry{
		sarama.ConfigEntry{
			Name:    "cleanup.policy",
			Value:   "compact",
			Default: true,
		},
		sarama.ConfigEntry{
			Name:  "key",
			Value: "value",
		},
	}, nil
}

func (_ *mockAdmin) Close() error {
	return nil
}

func generatePartitions(no int) []*sarama.PartitionMetadata {
	ps := []*sarama.PartitionMetadata{}
	for i := 0; i < no; i++ {
		ps = append(
			ps,
			&sarama.PartitionMetadata{
				ID:       int32(i),
				Leader:   1,
				Replicas: []int32{1},
				Isr:      []int32{1},
			},
		)
	}
	return ps
}

func TestTopicCmd(t *testing.T) {
	tests := []struct {
		name           string
		admin          *mockAdmin
		cmd            func(*sarama.ClusterAdmin) *cobra.Command
		args           []string
		expectedOutput string
		expectedErr    string
	}{
		{
			name:           "create should output info about the created topic (default values)",
			cmd:            createTopic,
			args:           []string{"San Francisco"},
			expectedOutput: "Created topic 'San Francisco'. Partitions: 1, replicas: 1, cleanup policy: 'delete'",
		},
		{
			name:           "create should output info about the created topic (custom values)",
			cmd:            createTopic,
			args:           []string{"Seattle", "--partitions", "2", "--replicas", "3", "--compact"},
			expectedOutput: "Created topic 'Seattle'. Partitions: 2, replicas: 3, cleanup policy: 'compact'",
		},
		{
			name:        "create should fail if no topic is passed",
			cmd:         createTopic,
			args:        []string{},
			expectedErr: "accepts 1 arg(s), received 0",
		},
		{
			name: "create should fail if the topic creation req fails",
			cmd:  createTopic,
			args: []string{"Chicago"},
			admin: &mockAdmin{
				createTopic: func(string, *sarama.TopicDetail, bool) error {
					return errors.New("no bueno error")
				},
			},
			expectedErr: "no bueno error",
		},
		{
			name:           "delete should output the name of the deleted topic",
			cmd:            deleteTopic,
			args:           []string{"Medellin"},
			expectedOutput: "Deleted topic 'Medellin'.",
		},
		{
			name: "delete should fail if the topic deletion req fails",
			cmd:  deleteTopic,
			args: []string{"Leticia"},
			admin: &mockAdmin{
				deleteTopic: func(string) error {
					return errors.New("that topic don't exist, yo")
				},
			},
			expectedErr: "that topic don't exist, yo",
		},
		{
			name:        "delete should fail if no topic is passed",
			cmd:         deleteTopic,
			args:        []string{},
			expectedErr: "accepts 1 arg(s), received 0",
		},
		{
			name:           "set-config should output the given config key-value pair",
			cmd:            setTopicConfig,
			args:           []string{"Panama", "somekey", "somevalue"},
			expectedOutput: "Added config 'somekey'='somevalue' to topic 'Panama'.",
		},
		{
			name: "set-config should fail if the req fails",
			cmd:  setTopicConfig,
			args: []string{"Chiriqui", "k", "v"},
			admin: &mockAdmin{
				alterConfig: func(
					sarama.ConfigResourceType,
					string,
					map[string]*string,
					bool,
				) error {
					return errors.New("can't set the config for some reason")
				},
			},
			expectedErr: "can't set the config for some reason",
		},
		{
			name:        "set-config should fail if no topic is passed",
			cmd:         setTopicConfig,
			args:        []string{},
			expectedErr: "accepts 3 arg(s), received 0",
		},
		{
			name:        "set-config should fail if no key is passed",
			cmd:         setTopicConfig,
			args:        []string{"Chepo"},
			expectedErr: "accepts 3 arg(s), received 1",
		},
		{
			name:        "set-config should fail if no value is passed",
			cmd:         setTopicConfig,
			args:        []string{"Chepo", "key"},
			expectedErr: "accepts 3 arg(s), received 2",
		},
		{
			name: "list should output the list of topics",
			cmd:  listTopics,
			admin: &mockAdmin{
				listTopics: func() (map[string]sarama.TopicDetail, error) {
					return map[string]sarama.TopicDetail{
						"tokyo": sarama.TopicDetail{
							NumPartitions:     2,
							ReplicationFactor: 3,
						},
						"kyoto": sarama.TopicDetail{
							NumPartitions:     10,
							ReplicationFactor: 2,
						},
						"fukushima": sarama.TopicDetail{
							NumPartitions:     7,
							ReplicationFactor: 3,
						},
					}, nil
				},
			},
			args: []string{},
			expectedOutput: `  Name       Partitions  Replicas  
  fukushima  7           3         
  kyoto      10          2         
  tokyo      2           3         
`,
		},
		{
			name: "list should fail if the req fails",
			cmd:  listTopics,
			args: []string{},
			admin: &mockAdmin{
				listTopics: func() (map[string]sarama.TopicDetail, error) {
					return nil, errors.New("an error happened :(")
				},
			},
			expectedErr: "an error happened :(",
		},
		{
			name: "list should output a message if there are no topics",
			cmd:  listTopics,
			admin: &mockAdmin{
				listTopics: func() (map[string]sarama.TopicDetail, error) {
					return map[string]sarama.TopicDetail{}, nil
				},
			},
			expectedOutput: "No topics found.",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var admin sarama.ClusterAdmin = &mockAdmin{}
			if tt.admin != nil {
				admin = tt.admin
			}
			var out bytes.Buffer
			cmd := tt.cmd(&admin)
			cmd.SetArgs(tt.args)
			logrus.SetOutput(&out)
			err := cmd.Execute()
			if tt.expectedErr != "" {
				require.EqualError(t, err, tt.expectedErr)
				return
			}
			require.NoError(t, err)
			require.Contains(t, out.String(), tt.expectedOutput)
		})
	}
}

func TestDescribeTopic(t *testing.T) {
	tests := []struct {
		name           string
		admin          *mockAdmin
		args           []string
		expectedOutput string
		expectedErr    string
	}{
		{
			name: "fails if the describe-topics request fails",
			admin: &mockAdmin{
				describeTopics: func(topics []string) ([]*sarama.TopicMetadata, error) {
					return nil, errors.New("it go boom")
				},
			},
			args:        []string{"Frankfurt"},
			expectedErr: "it go boom",
		},
		{
			name: "fails if the topic doesn't exist",
			admin: &mockAdmin{
				describeTopics: func(_ []string) ([]*sarama.TopicMetadata, error) {
					return []*sarama.TopicMetadata{
						&sarama.TopicMetadata{
							Err: sarama.ErrUnknownTopicOrPartition,
						},
					}, nil
				},
			},
			args:        []string{"Munich"},
			expectedErr: "topic 'Munich' not found",
		},
		{
			name: "fails if the describe-config request fails",
			admin: &mockAdmin{
				describeConfig: func(
					_ sarama.ConfigResource,
				) ([]sarama.ConfigEntry, error) {
					return nil, errors.New("describe-config failure")
				},
			},
			args:        []string{"Berlin"},
			expectedErr: "describe-config failure",
		},
		{
			name: "it should show the topic info",
			admin: &mockAdmin{
				describeTopics: func(_ []string) ([]*sarama.TopicMetadata, error) {
					return []*sarama.TopicMetadata{
						&sarama.TopicMetadata{
							Name:       "Hannover",
							Partitions: generatePartitions(10),
						},
					}, nil
				},
			},
			args: []string{"Hannover"},
			expectedOutput: `  Name            Hannover  
  Internal        false     
  Cleanup policy  compact   
  Config:         
  Name            Value     Read-only  Sensitive  
  key             value     false      false      
  Partitions      1 - 10 out of 10  
  Partition       Leader            Replicas   In-Sync Replicas  
  0               1                 [1]        [1]               
  1               1                 [1]        [1]               
  2               1                 [1]        [1]               
  3               1                 [1]        [1]               
  4               1                 [1]        [1]               
  5               1                 [1]        [1]               
  6               1                 [1]        [1]               
  7               1                 [1]        [1]               
  8               1                 [1]        [1]               
  9               1                 [1]        [1]               
`,
		},
		{
			name: "it shouldn't show the 'config' section if there's no non-default config",
			admin: &mockAdmin{
				describeTopics: func(_ []string) ([]*sarama.TopicMetadata, error) {
					return []*sarama.TopicMetadata{
						&sarama.TopicMetadata{
							Name:       "Hannover",
							Partitions: generatePartitions(1),
						},
					}, nil
				},
				describeConfig: func(_ sarama.ConfigResource) ([]sarama.ConfigEntry, error) {
					return []sarama.ConfigEntry{
						sarama.ConfigEntry{
							Name:    "cleanup.policy",
							Value:   "delete",
							Default: true,
						},
					}, nil
				},
			},
			args: []string{"Hannover"},
			expectedOutput: `  Name            Hannover  
  Internal        false     
  Cleanup policy  delete    
  Partitions      1 - 1 out of 1  
  Partition       Leader          Replicas  In-Sync Replicas  
  0               1               [1]       [1]               
`,
		},
		{
			name: "it should paginate the partitions",
			admin: &mockAdmin{
				describeTopics: func(_ []string) ([]*sarama.TopicMetadata, error) {
					return []*sarama.TopicMetadata{
						&sarama.TopicMetadata{
							Name:       "Cologne",
							Partitions: generatePartitions(12),
						},
					}, nil
				},
			},
			args: []string{"Cologne", "--page-size", "6", "--page", "1"},
			expectedOutput: `  Name            Cologne  
  Internal        false    
  Cleanup policy  compact  
  Config:         
  Name            Value    Read-only  Sensitive  
  key             value    false      false      
  Partitions      7 - 12 out of 12  
  Partition       Leader            Replicas   In-Sync Replicas  
  6               1                 [1]        [1]               
  7               1                 [1]        [1]               
  8               1                 [1]        [1]               
  9               1                 [1]        [1]               
  10              1                 [1]        [1]               
  11              1                 [1]        [1]               
`,
		},
		{
			name: "it should show the last page if the given page exceeds the # of pages",
			admin: &mockAdmin{
				describeTopics: func(_ []string) ([]*sarama.TopicMetadata, error) {
					return []*sarama.TopicMetadata{
						&sarama.TopicMetadata{
							Name:       "Cologne",
							Partitions: generatePartitions(12),
						},
					}, nil
				},
			},
			args: []string{"Cologne", "--page-size", "3", "--page", "5"},
			expectedOutput: `  Name            Cologne  
  Internal        false    
  Cleanup policy  compact  
  Config:         
  Name            Value    Read-only  Sensitive  
  key             value    false      false      
  Partitions      10 - 12 out of 12  
  Partition       Leader             Replicas   In-Sync Replicas  
  9               1                  [1]        [1]               
  10              1                  [1]        [1]               
  11              1                  [1]        [1]               
`,
		},
		{
			name: "it should show all the partitions if the page size exceeds the # of partitions",
			admin: &mockAdmin{
				describeTopics: func(_ []string) ([]*sarama.TopicMetadata, error) {
					return []*sarama.TopicMetadata{
						&sarama.TopicMetadata{
							Name:       "Cologne",
							Partitions: generatePartitions(12),
						},
					}, nil
				},
			},
			args: []string{"Cologne", "--page-size", "31", "--page", "4"},
			expectedOutput: `  Name            Cologne  
  Internal        false    
  Cleanup policy  compact  
  Config:         
  Name            Value    Read-only  Sensitive  
  key             value    false      false      
  Partitions      1 - 12 out of 12  
  Partition       Leader            Replicas   In-Sync Replicas  
  0               1                 [1]        [1]               
  1               1                 [1]        [1]               
  2               1                 [1]        [1]               
  3               1                 [1]        [1]               
  4               1                 [1]        [1]               
  5               1                 [1]        [1]               
  6               1                 [1]        [1]               
  7               1                 [1]        [1]               
  8               1                 [1]        [1]               
  9               1                 [1]        [1]               
  10              1                 [1]        [1]               
  11              1                 [1]        [1]               
`,
		},
		{
			name: "it should show all the partitions if the page size is negative",
			admin: &mockAdmin{
				describeTopics: func(_ []string) ([]*sarama.TopicMetadata, error) {
					return []*sarama.TopicMetadata{
						&sarama.TopicMetadata{
							Name:       "Cologne",
							Partitions: generatePartitions(5),
						},
					}, nil
				},
			},
			args: []string{"Cologne", "--page-size", "-1", "--page", "4"},
			expectedOutput: `  Name            Cologne  
  Internal        false    
  Cleanup policy  compact  
  Config:         
  Name            Value    Read-only  Sensitive  
  key             value    false      false      
  Partitions      1 - 5 out of 5  
  Partition       Leader          Replicas   In-Sync Replicas  
  0               1               [1]        [1]               
  1               1               [1]        [1]               
  2               1               [1]        [1]               
  3               1               [1]        [1]               
  4               1               [1]        [1]               
`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var admin sarama.ClusterAdmin = &mockAdmin{}
			var client sarama.Client = &mockClient{}
			if tt.admin != nil {
				admin = tt.admin
			}
			var out bytes.Buffer
			cmd := describeTopic(&client, &admin)
			// Disable watermarks so that the function doesn't call
			// kafka.HighWatermarks (kafka/client.go), which uses
			// an un-mockable function in sarama.Broker.
			args := append(tt.args, "--watermarks=false")
			cmd.SetArgs(args)
			logrus.SetOutput(&out)
			err := cmd.Execute()
			if tt.expectedErr != "" {
				require.EqualError(t, err, tt.expectedErr)
				return
			}
			require.NoError(t, err)
			require.Exactly(t, tt.expectedOutput, out.String())
		})
	}
}
