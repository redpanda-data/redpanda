package topic

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"gopkg.in/yaml.v3"
)

func setupTestTopics() kadm.TopicDetails {
	return kadm.TopicDetails{
		"test-topic": {
			Topic:      "test-topic",
			IsInternal: false,
			Partitions: kadm.PartitionDetails{
				0: {
					Partition:   0,
					Leader:      1,
					LeaderEpoch: 5,
					Replicas:    []int32{1, 2, 3},
					ISR:         []int32{1, 2, 3},
				},
				1: {
					Partition:       1,
					Leader:          2,
					LeaderEpoch:     3,
					Replicas:        []int32{1, 2, 3},
					ISR:             []int32{2, 3},
					OfflineReplicas: []int32{1},
				},
			},
		},
		"internal-topic": {
			Topic:      "internal-topic",
			IsInternal: true,
			Partitions: kadm.PartitionDetails{
				0: {
					Partition:   0,
					Leader:      1,
					LeaderEpoch: 1,
					Replicas:    []int32{1},
					ISR:         []int32{1},
				},
			},
		},
	}
}

type testCase struct {
	Kind   string
	Output string
}

func JSON(t *testing.T, o any) testCase {
	expected, err := json.Marshal(o)
	require.NoError(t, err)
	return testCase{Kind: "json", Output: string(expected) + "\n"}
}

func YAML(t *testing.T, o any) testCase {
	expected, err := yaml.Marshal(o)
	require.NoError(t, err)
	return testCase{Kind: "yaml", Output: string(expected) + "\n"}
}

func Text(s string) testCase {
	return testCase{Kind: "text", Output: s}
}

func TestSummarizedListView(t *testing.T) {
	topics := setupTestTopics()
	s := summarizedListView(false, topics)

	cases := []testCase{
		Text(`NAME        TOPIC-ID  PARTITIONS  REPLICAS
test-topic            2           3
`),
		JSON(t, s),
		YAML(t, s),
	}

	for _, c := range cases {
		f := config.OutFormatter{Kind: c.Kind}
		b := &strings.Builder{}
		printSummarizedListView(f, s, b)
		require.Equal(t, c.Output, b.String())
	}
}

func TestDetailedListView(t *testing.T) {
	topics := setupTestTopics()
	d := detailedListView(false, topics)

	cases := []testCase{
		Text(`test-topic, 2 partitions, 3 replicas
      PARTITION  LEADER  EPOCH  REPLICAS  OFFLINE_REPLICAS
      0          1       5      [1 2 3]   []
      1          2       3      [1 2 3]   [1]
`),
		JSON(t, d),
		YAML(t, d),
	}

	for _, c := range cases {
		f := config.OutFormatter{Kind: c.Kind}
		b := &strings.Builder{}
		printDetailedListView(f, d, b)
		require.Equal(t, c.Output, b.String())
	}
}

func TestSummarizedListViewWithInternal(t *testing.T) {
	topics := setupTestTopics()
	s := summarizedListView(true, topics)

	cases := []testCase{
		Text(`NAME            TOPIC-ID  PARTITIONS  REPLICAS
internal-topic            1           1
test-topic                2           3
`),
		JSON(t, s),
		YAML(t, s),
	}

	for _, c := range cases {
		f := config.OutFormatter{Kind: c.Kind}
		b := &strings.Builder{}
		printSummarizedListView(f, s, b)
		require.Equal(t, c.Output, b.String())
	}
}

func TestDetailedListViewWithInternal(t *testing.T) {
	topics := setupTestTopics()
	d := detailedListView(true, topics)

	cases := []testCase{
		Text(`internal-topic (internal), 1 partitions, 1 replicas
      PARTITION  LEADER  EPOCH  REPLICAS
      0          1       1      [1]

test-topic, 2 partitions, 3 replicas
      PARTITION  LEADER  EPOCH  REPLICAS  OFFLINE_REPLICAS
      0          1       5      [1 2 3]   []
      1          2       3      [1 2 3]   [1]
`),
		JSON(t, d),
		YAML(t, d),
	}

	for _, c := range cases {
		f := config.OutFormatter{Kind: c.Kind}
		b := &strings.Builder{}
		printDetailedListView(f, d, b)
		require.Equal(t, c.Output, b.String())
	}
}

func TestEmptyTopicList(t *testing.T) {
	emptyTopics := kadm.TopicDetails{}
	s := summarizedListView(false, emptyTopics)
	d := detailedListView(false, emptyTopics)

	for _, format := range []string{"text", "json", "yaml"} {
		f := config.OutFormatter{Kind: format}
		b := &strings.Builder{}

		printSummarizedListView(f, s, b)
		switch format {
		case "text":
			require.Equal(t, "NAME  TOPIC-ID  PARTITIONS  REPLICAS\n", b.String())
		case "json":
			require.Equal(t, "[]\n", b.String())
		case "yaml":
			require.Equal(t, "[]\n\n", b.String())
		}

		b.Reset()
		printDetailedListView(f, d, b)
		switch format {
		case "text":
			require.Empty(t, b.String())
		case "json":
			require.Equal(t, "[]\n", b.String())
		case "yaml":
			require.Equal(t, "[]\n\n", b.String())
		}
	}
}

// setupTestCommand sets up a complete topic command with a fake Kafka cluster
func setupTestCommand(t *testing.T, createTopics ...string) (*cobra.Command, *kfake.Cluster) {
	// Create fake Kafka cluster
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1))
	require.NoError(t, err)
	t.Cleanup(cluster.Close)

	// Create topics in the fake cluster
	if len(createTopics) > 0 {
		kgoClient, err := kgo.NewClient(
			kgo.SeedBrokers(cluster.ListenAddrs()...),
		)
		require.NoError(t, err)
		t.Cleanup(kgoClient.Close)

		adminClient := kadm.NewClient(kgoClient)
		_, err = adminClient.CreateTopics(context.Background(), 1, 1,
			map[string]*string{},
			createTopics...,
		)
		require.NoError(t, err)
	}

	// Create in-memory filesystem with config
	fs := afero.NewMemMapFs()
	configPath := "/tmp/rpk.yaml"
	configContent := testConfig(cluster.ListenAddrs())
	require.NoError(t, afero.WriteFile(fs, configPath, []byte(configContent), 0o644))

	// Create params with the config
	params := &config.Params{
		ConfigFlag: configPath,
		Formatter:  config.OutFormatter{Kind: "text"},
	}

	// Create the topic command
	cmd := NewCommand(fs, params)

	return cmd, cluster
}

// testConfig creates a minimal rpk config for testing
func testConfig(brokers []string) string {
	cfg := struct {
		Version int `yaml:"version"`
		Rpk     struct {
			KafkaAPI struct {
				Brokers []string `yaml:"brokers"`
			} `yaml:"kafka_api"`
		} `yaml:"rpk"`
	}{
		Version: 1,
	}
	cfg.Rpk.KafkaAPI.Brokers = brokers

	data, _ := yaml.Marshal(cfg)
	return string(data)
}

// captureOutput captures stdout during function execution
func captureOutput(f func()) string {
	// Create a pipe to capture output
	r, w, _ := os.Pipe()
	oldStdout := os.Stdout
	os.Stdout = w

	// Run the function
	f()

	// Restore stdout and close the writer
	w.Close()
	os.Stdout = oldStdout

	// Read the captured output
	var buf bytes.Buffer
	io.Copy(&buf, r)
	return buf.String()
}

// UnmarshalJSON read string and turn into topicID
func (t *topicID) UnmarshalJSON(b []byte) error {
	s := strings.Trim(string(b), `"`)
	if s == "null" || s == "" {
		return nil
	}

	decoded, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return fmt.Errorf("failed to decode topicID from base64: %w", err)
	}
	if len(decoded) != 16 {
		return fmt.Errorf("decoded topicID has incorrect length: got %d, want 16", len(decoded))
	}

	copy(t[:], decoded)
	return nil
}

func TestTopicListCommand_Basic(t *testing.T) {
	cmd, _ := setupTestCommand(t, "topic1", "topic2", "topic3")

	// Execute list command with JSON format
	cmd.SetArgs([]string{"list", "--format=json"})
	output := captureOutput(func() {
		cmd.Execute()
	})

	var topics []summarizedList
	err := json.Unmarshal([]byte(output), &topics)
	require.NoError(t, err)

	// Since topicID is randomly generated
	// it won't be possible to hardcode an exact
	// match, so instead, just verify
	// one was created.
	actuals := make(map[string]summarizedList)
	for _, topic := range topics {
		actuals[topic.Name] = topic
	}

	expectedNames := []string{"topic1", "topic2", "topic3"}
	for _, name := range expectedNames {
		actual, ok := actuals[name]
		require.True(t, ok, "topic %q not found", name)

		// Make sure TopicID exists.
		require.NotEqual(t, topicID{}, actual.ID, "topic %q has a nil ID", name)

		// Check predictable fields.
		require.Equal(t, 1, actual.Partitions)
		require.Equal(t, 1, actual.Replicas)
	}
}

func TestTopicListCommand_RegexFiltering(t *testing.T) {
	tests := []struct {
		name          string
		topics        []string
		regexPattern  string
		expectedNames []string
	}{
		{
			name:          "match single pattern",
			topics:        []string{"user-events", "user-login", "system-metrics", "system-logs"},
			regexPattern:  "user-.*",
			expectedNames: []string{"user-events", "user-login"},
		},
		{
			name:          "match all with wildcard",
			topics:        []string{"topic1", "topic2"},
			regexPattern:  ".*",
			expectedNames: []string{"topic1", "topic2"},
		},
		{
			name:          "exact match",
			topics:        []string{"exact-topic", "exact-topic-2", "other"},
			regexPattern:  "exact-topic",
			expectedNames: []string{"exact-topic"},
		},
		{
			name:          "no match returns empty",
			topics:        []string{"topic1", "topic2", "topic3"},
			regexPattern:  "nonexistent-.*",
			expectedNames: []string{},
		},
		{
			name:          "partial match fails due to incorrect regex",
			topics:        []string{"user-events", "events-user"},
			regexPattern:  "events",
			expectedNames: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd, _ := setupTestCommand(t, tt.topics...)

			cmd.SetArgs([]string{"list", "--regex", tt.regexPattern, "--format=json"})
			output := captureOutput(func() {
				cmd.Execute()
			})

			var actualTopics []summarizedList
			err := json.Unmarshal([]byte(output), &actualTopics)
			require.NoError(t, err)

			require.Len(t, actualTopics, len(tt.expectedNames))

			actualsByName := make(map[string]summarizedList)
			for _, topic := range actualTopics {
				actualsByName[topic.Name] = topic
			}

			for _, expectedName := range tt.expectedNames {
				actual, ok := actualsByName[expectedName]
				require.True(t, ok, "expected topic %q not found in results", expectedName)

				// Make sure UUID was created.
				require.NotEqual(t, topicID{}, actual.ID, "topic %q has a nil ID", actual.Name)

				// Check predicted fields.
				require.Equal(t, 1, actual.Partitions, "topic %q has wrong partition count", actual.Name)
				require.Equal(t, 1, actual.Replicas, "topic %q has wrong replica count", actual.Name)
			}
		})
	}
}

func TestExecuteTopicList_ErrorScenarios(t *testing.T) {
	t.Run("kafka client connection error", func(t *testing.T) {
		// Use an invalid broker address to trigger connection error quickly
		kgoClient, err := kgo.NewClient(
			kgo.SeedBrokers("invalid-broker:9092"),
			kgo.ConnIdleTimeout(time.Second),        // Minimum allowed timeout
			kgo.RequestTimeoutOverhead(time.Second), // Short timeout to fail fast
		)
		require.NoError(t, err)
		t.Cleanup(kgoClient.Close)

		adm := kadm.NewClient(kgoClient)
		result, err := executeTopicList(adm, []string{}, false)

		require.Error(t, err)
		require.Contains(t, err.Error(), "unable to request metadata")
		require.Nil(t, result)
	})

	// Create a working client for regex compilation error tests
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1))
	require.NoError(t, err)
	t.Cleanup(cluster.Close)

	kgoClient, err := kgo.NewClient(
		kgo.SeedBrokers(cluster.ListenAddrs()...),
	)
	require.NoError(t, err)
	t.Cleanup(kgoClient.Close)

	adm := kadm.NewClient(kgoClient)

	invalidRegexTests := []struct {
		name        string
		topics      []string
		useRegex    bool
		expectError string
	}{
		{
			name:        "invalid regex compilation error - unclosed bracket",
			topics:      []string{"[unclosed"},
			useRegex:    true,
			expectError: "unable to compile regex expressions",
		},
		{
			name:        "invalid regex compilation error - invalid escape sequence",
			topics:      []string{"\\k"}, // Invalid escape sequence
			useRegex:    true,
			expectError: "unable to compile regex expressions",
		},
		{
			name:        "invalid regex compilation error - invalid character class",
			topics:      []string{"[z-a]"}, // Invalid range
			useRegex:    true,
			expectError: "unable to compile regex expressions",
		},
		{
			name:        "multiple invalid regex patterns",
			topics:      []string{"valid-.*", "[unclosed", "another-valid"},
			useRegex:    true,
			expectError: "unable to compile regex expressions",
		},
	}

	for _, tt := range invalidRegexTests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executeTopicList(adm, tt.topics, tt.useRegex)

			// Verify error occurred and contains expected message
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.expectError)
			require.Nil(t, result)
		})
	}
}
