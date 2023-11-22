// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package partitions

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_parsePartition(t *testing.T) {
	for _, tt := range []struct {
		name          string
		input         string
		expNs         string
		expTopic      string
		expPartitions []int
		expErr        bool
	}{
		{
			name:          "complete",
			input:         "_redpanda_internal/topic-foo1/2,3,1",
			expNs:         "_redpanda_internal",
			expTopic:      "topic-foo1",
			expPartitions: []int{2, 3, 1},
		}, {
			name:          "topic and partitions",
			input:         "myTopic/1,2",
			expNs:         "kafka",
			expTopic:      "myTopic",
			expPartitions: []int{1, 2},
		}, {
			name:          "topic and single partition",
			input:         "myTopic/12",
			expNs:         "kafka",
			expTopic:      "myTopic",
			expPartitions: []int{12},
		}, {
			name:          "just partitions",
			input:         "1,2,3,5,8,13,21",
			expNs:         "kafka",
			expTopic:      "",
			expPartitions: []int{1, 2, 3, 5, 8, 13, 21},
		}, {
			name:          "single partition",
			input:         "13",
			expNs:         "kafka",
			expTopic:      "",
			expPartitions: []int{13},
		}, {
			name:          "topic with dot",
			input:         "my.topic.foo/1",
			expNs:         "kafka",
			expTopic:      "my.topic.foo",
			expPartitions: []int{1},
		}, {
			name:   "wrong format 1",
			input:  "thirteen",
			expErr: true,
		}, {
			name:   "wrong format 2",
			input:  "_internal|foo|1,2,3",
			expErr: true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			gotNs, gotTopic, gotPartitions, err := parsePartition(tt.input)
			if tt.expErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			require.Equal(t, tt.expNs, gotNs)
			require.Equal(t, tt.expTopic, gotTopic)
			require.Equal(t, tt.expPartitions, gotPartitions)
		})
	}
}
