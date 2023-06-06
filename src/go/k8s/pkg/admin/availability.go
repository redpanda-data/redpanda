// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package admin

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	vectorizedv1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/vectorized/v1alpha1"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/labels"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/utils"
)

// IsAvailableInPreFlight performs a lightweight check to verify if the admin API is available for a given cluster.
// The check can be used as a pre-fight check to wait for the API to be available before performing operations on it,
// but it does not give absolute guarantees.
func IsAvailableInPreFlight(
	ctx context.Context,
	k8sClient client.Reader,
	redpandaCluster *vectorizedv1alpha1.Cluster,
) (bool, error) {
	var pods corev1.PodList
	err := k8sClient.List(ctx, &pods, &client.ListOptions{
		Namespace:     redpandaCluster.Namespace,
		LabelSelector: labels.ForCluster(redpandaCluster).AsClientSelector(),
	})
	if err != nil {
		return false, fmt.Errorf("error while listing cluster pods for pre-flight check: %w", err)
	}

	for i := range pods.Items {
		if !utils.IsPodReady(&pods.Items[i]) {
			return false, nil
		}
	}
	return true, nil
}
