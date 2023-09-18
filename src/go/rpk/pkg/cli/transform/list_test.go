package transform

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"gopkg.in/yaml.v3"

	"github.com/stretchr/testify/require"
)

func setupTestData() []adminapi.TransformMetadata {
	return []adminapi.TransformMetadata{
		{
			Name:         "foo2bar",
			InputTopic:   "foo",
			OutputTopics: []string{"bar"},
			Environment: []adminapi.EnvironmentVariable{
				{Key: "FLUBBER", Value: "enabled"},
			},
			Status: []adminapi.PartitionTransformStatus{
				{
					NodeID:    0,
					Partition: 1,
					Core:      0,
					Status:    "running",
				},
				{
					NodeID:    0,
					Partition: 2,
					Core:      1,
					Status:    "running",
				},
				{
					NodeID:    1,
					Partition: 3,
					Core:      4,
					Status:    "inactive",
				},
			},
		},
		{
			Name:         "scrubber",
			InputTopic:   "pii",
			OutputTopics: []string{"cleaned", "munged"},
			Environment: []adminapi.EnvironmentVariable{
				{Key: "FLUX_CAPACITOR", Value: "disabled"},
			},
			Status: []adminapi.PartitionTransformStatus{
				{
					NodeID:    0,
					Partition: 1,
					Core:      0,
					Status:    "errored",
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
	s = strings.TrimSpace(s)
	return testCase{Kind: "text", Output: s + "\n"}
}

func TestPrintSummaryView(t *testing.T) {
	s := summarizedView(setupTestData())
	cases := []testCase{
		Text(`
NAME      INPUT TOPIC  OUTPUT TOPIC     RUNNING
foo2bar   foo          bar              2 / 3
scrubber  pii          cleaned, munged  0 / 1
`),
		JSON(t, s),
		YAML(t, s),
	}
	for _, c := range cases {
		f := config.OutFormatter{Kind: c.Kind}
		b := &strings.Builder{}
		printSummary(f, s, b)
		require.Equal(t, c.Output, b.String())
	}
}

func TestPrintDetailView(t *testing.T) {
	d := detailView(setupTestData())
	cases := []testCase{
		Text(`
foo2bar, foo → bar
      PARTITION  NODE  CORE  STATUS
      1          0     0     running
      2          0     1     running
      3          1     4     inactive

scrubber, pii → cleaned, munged
      PARTITION  NODE  CORE  STATUS
      1          0     0     errored
`),
		JSON(t, d),
		YAML(t, d),
	}
	for _, c := range cases {
		f := config.OutFormatter{Kind: c.Kind}
		b := &strings.Builder{}
		printDetailed(f, d, b)
		require.Equal(t, c.Output, b.String())
	}
}
