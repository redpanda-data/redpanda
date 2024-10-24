/*
* Copyright 2024 Redpanda Data, Inc.
*
* Use of this software is governed by the Business Source License
* included in the file licenses/BSL.md
*
* As of the Change Date specified in that file, in accordance with
* the Business Source License, use of this software will be governed
* by the Apache License, Version 2.0
 */

package transform

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/redpanda-data/common-go/rpadmin"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"gopkg.in/yaml.v3"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestData() []rpadmin.TransformMetadata {
	return []rpadmin.TransformMetadata{
		{
			Name:         "foo2bar",
			InputTopic:   "foo",
			OutputTopics: []string{"bar"},
			Environment: []rpadmin.EnvironmentVariable{
				{Key: "FLUBBER", Value: "enabled"},
			},
			CompressionMode: "none",
			Status: []rpadmin.PartitionTransformStatus{
				{
					NodeID:    0,
					Partition: 1,
					Status:    "running",
					Lag:       1,
				},
				{
					NodeID:    0,
					Partition: 2,
					Status:    "running",
					Lag:       1,
				},
				{
					NodeID:    1,
					Partition: 3,
					Status:    "inactive",
					Lag:       5,
				},
			},
		},
		{
			Name:         "scrubber",
			InputTopic:   "pii",
			OutputTopics: []string{"cleaned", "munged"},
			Environment: []rpadmin.EnvironmentVariable{
				{Key: "FLUX_CAPACITOR", Value: "disabled"},
			},
			CompressionMode: "gzip",
			Status: []rpadmin.PartitionTransformStatus{
				{
					NodeID:    0,
					Partition: 1,
					Status:    "errored",
					Lag:       99,
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

func TestHandleEmptyInput(t *testing.T) {
	s := summarizedView([]rpadmin.TransformMetadata{})
	assert.NotNil(t, s)
	d := detailView([]rpadmin.TransformMetadata{})
	assert.NotNil(t, d)
}

func TestPrintSummaryView(t *testing.T) {
	s := summarizedView(setupTestData())
	cases := []testCase{
		Text(`
NAME      INPUT TOPIC  OUTPUT TOPIC     RUNNING  LAG
foo2bar   foo          bar              2 / 3    7
scrubber  pii          cleaned, munged  0 / 1    99
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
      PARTITION  NODE  STATUS    LAG
      1          0     running   1
      2          0     running   1
      3          1     inactive  5

scrubber, pii → cleaned, munged
      PARTITION  NODE  STATUS   LAG
      1          0     errored  99
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
