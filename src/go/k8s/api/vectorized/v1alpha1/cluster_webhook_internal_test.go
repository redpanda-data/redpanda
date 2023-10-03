// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package v1alpha1

import (
	"reflect"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

//nolint:funlen // this is ok for a test
func TestCluster_validateAdditionalConfiguration(t *testing.T) {
	type cluster struct {
		TypeMeta   metav1.TypeMeta
		ObjectMeta metav1.ObjectMeta
		Spec       ClusterSpec
		Status     ClusterStatus
	}
	tests := []struct {
		name    string
		cluster cluster
		want    field.ErrorList
	}{
		{
			name: "old keys no internal_topic_replication_factor",
			cluster: cluster{
				Spec: ClusterSpec{
					AdditionalConfiguration: map[string]string{
						idAllocatorReplicationKey:            "3",
						transactionCoordinatorReplicationKey: "3",
						defaultTopicReplicationKey:           "3",
					},
				},
			},
			want: nil,
		},
		{
			name: "old keys and internal_topic_replication_factor equal to old keys",
			cluster: cluster{
				Spec: ClusterSpec{
					AdditionalConfiguration: map[string]string{
						idAllocatorReplicationKey:            "3",
						transactionCoordinatorReplicationKey: "3",
						defaultTopicReplicationKey:           "3",
						internalTopicReplicationFactorKey:    "3",
					},
				},
			},
			want: nil,
		},
		{
			name: "internal_topic_replication_factor with no old keys",
			cluster: cluster{
				Spec: ClusterSpec{
					AdditionalConfiguration: map[string]string{
						internalTopicReplicationFactorKey: "3",
					},
				},
			},
			want: nil,
		},
		{
			name: "internal_topic_replication_factor set to 1 with no old keys",
			cluster: cluster{
				Spec: ClusterSpec{
					Replicas: func() *int32 { i := int32(1); return &i }(),
					AdditionalConfiguration: map[string]string{
						internalTopicReplicationFactorKey: "1",
					},
				},
			},
			want: nil,
		},
		{
			name: "error if internal_topic_replication_factor is less than an old key",
			cluster: cluster{
				Spec: ClusterSpec{
					AdditionalConfiguration: map[string]string{
						idAllocatorReplicationKey:            "5",
						transactionCoordinatorReplicationKey: "3",
						defaultTopicReplicationKey:           "3",
						internalTopicReplicationFactorKey:    "3",
					},
				},
			},
			want: field.ErrorList{
				field.Invalid(
					field.NewPath("spec").Child("additionalConfiguration").Child(internalTopicReplicationFactorKey),
					3,
					"Cannot be reduced from 5"),
			},
		},
		{
			name: "error if internal_topic_replication_factor is less than 3 if there are 3 or more replicas",
			cluster: cluster{
				Spec: ClusterSpec{
					Replicas: func() *int32 { i := int32(3); return &i }(),
					AdditionalConfiguration: map[string]string{
						internalTopicReplicationFactorKey: "1",
					},
				},
			},
			want: field.ErrorList{
				field.Invalid(
					field.NewPath("spec").Child("additionalConfiguration").Child(internalTopicReplicationFactorKey),
					1,
					"Cannot be reduced from 3"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Cluster{
				TypeMeta:   tt.cluster.TypeMeta,
				ObjectMeta: tt.cluster.ObjectMeta,
				Spec:       tt.cluster.Spec,
				Status:     tt.cluster.Status,
			}
			if got := r.validateAdditionalConfiguration(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Cluster.validateAdditionalConfiguration() = %v, want %v", got, tt.want)
			}
		})
	}
}
