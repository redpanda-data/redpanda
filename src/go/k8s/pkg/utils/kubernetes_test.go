// Copyright 2022-2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package utils_test

import (
	"fmt"
	"testing"

	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/utils"
	"github.com/stretchr/testify/require"
)

func TestGetPodOrdinal(t *testing.T) {
	tcs := []struct {
		podName         string
		clusterName     string
		expectedError   bool
		expectedOrdinal int64
	}{
		{"", "", true, -1},
		{"test", "", true, -1},
		{"pod-0", "pod", false, 0},
		{"pod-99", "pod", false, 99},
		{"", "unexpected longer cluster name", true, -1},
		{"test+0", "test", false, 0},
		{"without-ordinal-", "without-ordinal", true, -1},
	}

	for _, tc := range tcs {
		t.Run(fmt.Sprintf("pod %s and cluster %s", tc.podName, tc.clusterName), func(t *testing.T) {
			ordinal, err := utils.GetPodOrdinal(tc.podName, tc.clusterName)
			if tc.expectedError {
				require.Error(t, err)
			}
			require.Equal(t, tc.expectedOrdinal, ordinal)
		})
	}
}
