// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package v1alpha1_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/utils/pointer"

	"github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources/featuregates"
)

//nolint:funlen // this is ok for a test
func TestRedpandaResourceRequirements(t *testing.T) {
	type test struct {
		name                string
		setRequestsCPU      resource.Quantity
		setRequestsMem      resource.Quantity
		setRedpandaCPU      resource.Quantity
		setRedpandaMem      resource.Quantity
		expectedRedpandaCPU resource.Quantity
		expectedRedpandaMem resource.Quantity
	}
	makeResources := func(t test) v1alpha1.RedpandaResourceRequirements {
		return v1alpha1.RedpandaResourceRequirements{
			ResourceRequirements: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceMemory: t.setRequestsMem,
					corev1.ResourceCPU:    t.setRequestsCPU,
				},
			},
			Redpanda: corev1.ResourceList{
				corev1.ResourceMemory: t.setRedpandaMem,
				corev1.ResourceCPU:    t.setRedpandaCPU,
			},
		}
	}

	t.Run("Memory", func(t *testing.T) {
		tests := []test{
			{
				name:                "RedpandaMemory is set from requests.memory",
				setRequestsMem:      resource.MustParse("3000Mi"),
				expectedRedpandaMem: resource.MustParse("2700Mi"),
			},
			{
				name:                "RedpandaMemory is set from lower redpanda.memory",
				setRequestsMem:      resource.MustParse("4000Mi"),
				setRedpandaMem:      resource.MustParse("3000Mi"),
				expectedRedpandaMem: resource.MustParse("3000Mi"),
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				rrr := makeResources(tt)
				assert.Equal(t, tt.expectedRedpandaMem.Value(), rrr.RedpandaMemory().Value())
			})
		}
	})

	t.Run("Cpu", func(t *testing.T) {
		tests := []test{
			{
				name:                "RedpandaCPU is set from integer requests.cpu",
				setRequestsCPU:      resource.MustParse("1"),
				setRequestsMem:      resource.MustParse("20Gi"),
				expectedRedpandaCPU: resource.MustParse("1"),
			},
			{
				name:                "RedpandaCPU is set from milli requests.cpu",
				setRequestsCPU:      resource.MustParse("1000m"),
				setRequestsMem:      resource.MustParse("20Gi"),
				expectedRedpandaCPU: resource.MustParse("1"),
			},
			{
				name:                "RedpandaCPU is rounded up from milli requests.cpu",
				setRequestsCPU:      resource.MustParse("1001m"),
				setRequestsMem:      resource.MustParse("20Gi"),
				expectedRedpandaCPU: resource.MustParse("2"),
			},
			{
				name:                "RedpandaCPU is set from lower redpanda.cpu",
				setRequestsCPU:      resource.MustParse("2"),
				setRequestsMem:      resource.MustParse("20Gi"),
				setRedpandaCPU:      resource.MustParse("1"),
				expectedRedpandaCPU: resource.MustParse("1"),
			},
			{
				name:                "RedpandaCPU is set from higher redpanda.cpu",
				setRequestsCPU:      resource.MustParse("1"),
				setRequestsMem:      resource.MustParse("20Gi"),
				setRedpandaCPU:      resource.MustParse("2"),
				expectedRedpandaCPU: resource.MustParse("2"),
			},
			{
				name:                "RedpandaCPU is rounded up from milli redpanda.cpu",
				setRequestsCPU:      resource.MustParse("1"),
				setRequestsMem:      resource.MustParse("20Gi"),
				setRedpandaCPU:      resource.MustParse("1001m"),
				expectedRedpandaCPU: resource.MustParse("2"),
			},
			{
				name:                "RedpandaCPU is limited by 2GiB/core",
				setRequestsCPU:      resource.MustParse("10"),
				setRequestsMem:      resource.MustParse("4Gi"),
				expectedRedpandaCPU: resource.MustParse("2"),
			},
			{
				name:                "RedpandaCPU has a minimum if requests >0",
				setRequestsCPU:      resource.MustParse("100m"),
				setRequestsMem:      resource.MustParse("100Mi"),
				expectedRedpandaCPU: resource.MustParse("1"),
			},
			{
				name:                "RedpandaCPU not set if no request",
				expectedRedpandaCPU: resource.MustParse("0"),
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				rrr := makeResources(tt)
				assert.Equal(t, tt.expectedRedpandaCPU.Value(), rrr.RedpandaCPU().Value())
			})
		}
	})
}

func TestConditions(t *testing.T) {
	earlyClock := func() time.Time {
		return time.Now().Add(1 * time.Millisecond)
	}

	t.Run("create a condition", func(t *testing.T) {
		t.Parallel()
		cluster := &v1alpha1.Cluster{}
		assert.True(t, cluster.Status.SetCondition(v1alpha1.ClusterConfiguredConditionType, corev1.ConditionTrue, "reason", "message"))
		require.Len(t, cluster.Status.Conditions, 1)
		cond := cluster.Status.Conditions[0]
		assert.Equal(t, v1alpha1.ClusterConfiguredConditionType, cond.Type)
		assert.Equal(t, corev1.ConditionTrue, cond.Status)
		assert.Equal(t, "reason", cond.Reason)
		assert.Equal(t, "message", cond.Message)
		condTime := cond.LastTransitionTime
		assert.False(t, condTime.After(time.Now()))
	})

	t.Run("update a condition", func(t *testing.T) {
		t.Parallel()
		cluster := &v1alpha1.Cluster{}
		assert.True(t, cluster.Status.SetCondition(v1alpha1.ClusterConfiguredConditionType, corev1.ConditionTrue, "reason", "message"))
		require.Len(t, cluster.Status.Conditions, 1)
		cond := cluster.Status.Conditions[0]
		condTime := cond.LastTransitionTime
		assert.True(t, cluster.Status.SetConditionUsingClock(v1alpha1.ClusterConfiguredConditionType, corev1.ConditionFalse, "reason2", "message2", earlyClock))
		require.Len(t, cluster.Status.Conditions, 1)
		cond = cluster.Status.Conditions[0]
		assert.Equal(t, v1alpha1.ClusterConfiguredConditionType, cond.Type)
		assert.Equal(t, corev1.ConditionFalse, cond.Status)
		assert.Equal(t, "reason2", cond.Reason)
		assert.Equal(t, "message2", cond.Message)
		condTime2 := cond.LastTransitionTime
		assert.True(t, condTime2.After(condTime.Time))
	})

	t.Run("update to one condition does not affect the other", func(t *testing.T) {
		t.Parallel()
		cluster := &v1alpha1.Cluster{}
		assert.True(t, cluster.Status.SetCondition(v1alpha1.ClusterConfiguredConditionType, corev1.ConditionTrue, "reason", "message"))
		assert.True(t, cluster.Status.SetCondition("otherType", corev1.ConditionFalse, "reason2", "message2"))
		require.Len(t, cluster.Status.Conditions, 2)
		condCluster := cluster.Status.Conditions[0]
		condClusterTime := condCluster.LastTransitionTime
		condOther := cluster.Status.Conditions[1]
		condOtherTime := condOther.LastTransitionTime

		assert.True(t, cluster.Status.SetConditionUsingClock("otherType", corev1.ConditionUnknown, "reason3", "message3", earlyClock))
		require.Len(t, cluster.Status.Conditions, 2)
		condCluster = cluster.Status.Conditions[0]
		assert.Equal(t, v1alpha1.ClusterConfiguredConditionType, condCluster.Type)
		assert.Equal(t, corev1.ConditionTrue, condCluster.Status)
		assert.Equal(t, "reason", condCluster.Reason)
		assert.Equal(t, "message", condCluster.Message)
		condClusterTime2 := condCluster.LastTransitionTime
		assert.Equal(t, condClusterTime2, condClusterTime)

		condOther = cluster.Status.Conditions[1]
		assert.Equal(t, v1alpha1.ClusterConditionType("otherType"), condOther.Type)
		assert.Equal(t, corev1.ConditionUnknown, condOther.Status)
		assert.Equal(t, "reason3", condOther.Reason)
		assert.Equal(t, "message3", condOther.Message)
		condOtherTime2 := condOther.LastTransitionTime
		assert.True(t, condOtherTime2.After(condOtherTime.Time))
	})

	t.Run("updating a condition with itself does not change transition time", func(t *testing.T) {
		t.Parallel()
		cluster := &v1alpha1.Cluster{}
		assert.True(t, cluster.Status.SetCondition(v1alpha1.ClusterConfiguredConditionType, corev1.ConditionTrue, "reason", "message"))
		require.Len(t, cluster.Status.Conditions, 1)
		cond := cluster.Status.Conditions[0]
		condTime := cond.LastTransitionTime
		assert.False(t, cluster.Status.SetConditionUsingClock(v1alpha1.ClusterConfiguredConditionType, corev1.ConditionTrue, "reason", "message", earlyClock))
		require.Len(t, cluster.Status.Conditions, 1)
		cond2 := cluster.Status.Conditions[0]
		assert.Equal(t, condTime, cond2.LastTransitionTime)
		assert.Equal(t, condTime, cond2.LastTransitionTime)
	})
}

func TestInitialReplicas(t *testing.T) {
	// backward compatibility. Remove when v22.2 is no longer supported.
	cluster := v1alpha1.Cluster{}
	cluster.Spec.Version = featuregates.V22_2_1.String()
	cluster.Spec.Replicas = pointer.Int32(3)
	assert.Equal(t, int32(1), cluster.GetCurrentReplicas())
	cluster.Status.Replicas = 2
	assert.Equal(t, int32(3), cluster.GetCurrentReplicas())
	cluster.Status.Replicas = 0
	cluster.Status.ReadyReplicas = 2
	assert.Equal(t, int32(3), cluster.GetCurrentReplicas())
	cluster.Status.ReadyReplicas = 0
	cluster.Status.Nodes.Internal = []string{"1", "2"}
	assert.Equal(t, int32(3), cluster.GetCurrentReplicas())
	cluster.Status.Nodes.Internal = nil
	assert.Equal(t, int32(1), cluster.GetCurrentReplicas())

	// test with latest version
	cluster = v1alpha1.Cluster{}
	cluster.Spec.Replicas = pointer.Int32(3)
	assert.Equal(t, int32(3), cluster.GetCurrentReplicas())
	cluster.Status.Replicas = 2
	assert.Equal(t, int32(3), cluster.GetCurrentReplicas())
	cluster.Status.Replicas = 0
	cluster.Status.ReadyReplicas = 2
	assert.Equal(t, int32(3), cluster.GetCurrentReplicas())
	cluster.Status.ReadyReplicas = 0
	cluster.Status.Nodes.Internal = []string{"1", "2"}
	assert.Equal(t, int32(3), cluster.GetCurrentReplicas())
	cluster.Status.Nodes.Internal = nil
	assert.Equal(t, int32(3), cluster.GetCurrentReplicas())
}
