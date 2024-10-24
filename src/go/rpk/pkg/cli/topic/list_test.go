package topic

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
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
		Text(`NAME        PARTITIONS  REPLICAS
test-topic  2           3
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
		Text(`NAME            PARTITIONS  REPLICAS
internal-topic  1           1
test-topic      2           3
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
			require.Equal(t, "NAME  PARTITIONS  REPLICAS\n", b.String())
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
