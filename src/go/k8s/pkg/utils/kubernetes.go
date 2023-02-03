// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package utils

import (
	"fmt"
	"strconv"

	corev1 "k8s.io/api/core/v1"
)

var ErrInvalidInputParameters = fmt.Errorf("invalid input parameters")

// IsPodReady tells if a given pod is ready looking at its status.
func IsPodReady(pod *corev1.Pod) bool {
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue {
			return true
		}
	}

	return false
}

func GetPodOrdinal(podName, clusterName string) (int64, error) {
	// Pod name needs to have at least 2 more characters
	if len(podName) < len(clusterName)+2 {
		return -1, fmt.Errorf("pod name (%s) and cluster name (%s): %w", podName, clusterName, ErrInvalidInputParameters)
	}

	// The +1 is for the separator between stateful set name and pod ordinal
	ordinalStr := podName[len(clusterName)+1:]
	ordinal, err := strconv.ParseInt(ordinalStr, 10, 0)
	if err != nil {
		return -1, fmt.Errorf("parsing int failed (%s): %w", ordinalStr, err)
	}
	return ordinal, nil
}
