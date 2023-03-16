// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package labels_test

import (
	"reflect"
	"testing"

	redpandav1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/labels"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestLabels(t *testing.T) {
	testCluster := &redpandav1alpha1.Cluster{
		TypeMeta: metav1.TypeMeta{
			Kind:       "RedpandaCluster",
			APIVersion: "core.vectorized.io/v1alpha1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "testcluster",
			Namespace: "default",
		},
		Spec: redpandav1alpha1.ClusterSpec{},
	}
	withPartOfDefined := testCluster.DeepCopy()
	withPartOfDefined.Labels = make(map[string]string)
	withPartOfDefined.Labels[labels.PartOfKey] = "part-of-something-else"

	tests := []struct {
		name         string
		pandaCluster *redpandav1alpha1.Cluster
		expected     map[string]string
	}{
		{
			"empty inherited labels", testCluster, map[string]string{
				"app.kubernetes.io/name":       "redpanda",
				"app.kubernetes.io/instance":   "testcluster",
				"app.kubernetes.io/component":  "redpanda",
				"app.kubernetes.io/part-of":    "redpanda",
				"app.kubernetes.io/managed-by": "redpanda-operator",
			},
		},
		{
			"some inherited labels", withPartOfDefined, map[string]string{
				"app.kubernetes.io/name":       "redpanda",
				"app.kubernetes.io/instance":   "testcluster",
				"app.kubernetes.io/component":  "redpanda",
				"app.kubernetes.io/part-of":    "part-of-something-else",
				"app.kubernetes.io/managed-by": "redpanda-operator",
			},
		},
	}

	for _, tt := range tests {
		actual := labels.ForCluster(tt.pandaCluster)
		// we need to change the type to map here for deepEqual to compare the same types
		var actualMap map[string]string = actual
		if !reflect.DeepEqual(actualMap, tt.expected) {
			t.Errorf("%s: Expecting labels to be %v but got %v", tt.name, tt.expected, actual)
		}
	}
}
