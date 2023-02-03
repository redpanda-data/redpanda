// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package resources //nolint:testpackage // needed to test private method

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"

	redpandav1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	adminutils "github.com/redpanda-data/redpanda/src/go/k8s/pkg/admin"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources/types"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/api/admin"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestShouldUpdate_AnnotationChange(t *testing.T) {
	var replicas int32 = 1
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "test",
		},
		TypeMeta: metav1.TypeMeta{
			Kind:       "StatefulSet",
			APIVersion: "apps/v1",
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas: &replicas,
			UpdateStrategy: appsv1.StatefulSetUpdateStrategy{
				Type: appsv1.OnDeleteStatefulSetStrategyType,
			},
			ServiceName: "test",
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "test",
					Namespace:   "default",
					Annotations: map[string]string{"test": "test"},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:    "test",
							Image:   "nginx",
							Command: []string{"nginx"},
						},
					},
				},
			},
		},
	}
	stsWithAnnotation := sts.DeepCopy()
	stsWithAnnotation.Spec.Template.Annotations = map[string]string{"test": "test2"}
	ssres := StatefulSetResource{}
	update, err := ssres.shouldUpdate(false, sts, stsWithAnnotation)
	require.NoError(t, err)
	require.True(t, update)

	// same statefulset with same annotation
	update, err = ssres.shouldUpdate(false, stsWithAnnotation, stsWithAnnotation)
	require.NoError(t, err)
	require.False(t, update)
}

func TestPutInMaintenanceMode(t *testing.T) {
	tcs := []struct {
		name              string
		maintenanceStatus *admin.MaintenanceStatus
		errorRequired     error
	}{
		{
			"maintenance finished",
			&admin.MaintenanceStatus{
				Finished: true,
			},
			nil,
		},
		{
			"maintenance draining",
			&admin.MaintenanceStatus{
				Draining: true,
			},
			ErrMaintenanceNotFinished,
		},
		{
			"maintenance failed",
			&admin.MaintenanceStatus{
				Failed: 1,
			},
			ErrMaintenanceNotFinished,
		},
		{
			"maintenance has errors",
			&admin.MaintenanceStatus{
				Errors: true,
			},
			ErrMaintenanceNotFinished,
		},
		{
			"maintenance did not finished",
			&admin.MaintenanceStatus{
				Finished: false,
			},
			ErrMaintenanceNotFinished,
		},
		{
			"maintenance was not returned",
			nil,
			ErrMaintenanceMissing,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			ssres := StatefulSetResource{
				adminAPIClientFactory: func(
					ctx context.Context,
					k8sClient client.Reader,
					redpandaCluster *redpandav1alpha1.Cluster,
					fqdn string,
					adminTLSProvider types.AdminTLSConfigProvider,
					ordinals ...int32,
				) (adminutils.AdminAPIClient, error) {
					return &adminutils.MockAdminAPI{
						Log:               ctrl.Log.WithName("testAdminAPI").WithName("mockAdminAPI"),
						MaintenanceStatus: tc.maintenanceStatus,
					}, nil
				},
			}
			err := ssres.putInMaintenanceMode(context.Background(), 0)
			if tc.errorRequired == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tc.errorRequired)
			}
		})
	}
}

func TestEvaluateRedpandaUnderReplicatedPartition(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f, err := os.Open("testdata/metrics.golden.txt")
		require.NoError(t, err)

		_, err = io.Copy(w, f)
		require.NoError(t, err)
	}))
	defer ts.Close()

	ssres := StatefulSetResource{pandaCluster: &redpandav1alpha1.Cluster{
		Spec: redpandav1alpha1.ClusterSpec{
			RestartConfig: &redpandav1alpha1.RestartConfig{},
		},
	}}

	adminURL := url.URL{
		Scheme: "http",
		Host:   ts.Listener.Addr().String(),
		Path:   "metrics",
	}

	err := ssres.evaluateUnderReplicatedPartitions(context.Background(), &adminURL)
	require.NoError(t, err)
}

func TestEvaluateAboveThresholdRedpandaUnderReplicatedPartition(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `
# HELP vectorized_cluster_partition_under_replicated_replicas Number of under replicated replicas
# TYPE vectorized_cluster_partition_under_replicated_replicas gauge
vectorized_cluster_partition_under_replicated_replicas{namespace="kafka",partition="0",shard="0",topic="test"} 1.000000
`)
	}))
	defer ts.Close()

	ssres := StatefulSetResource{pandaCluster: &redpandav1alpha1.Cluster{
		Spec: redpandav1alpha1.ClusterSpec{
			RestartConfig: &redpandav1alpha1.RestartConfig{},
		},
	}}

	adminURL := url.URL{
		Scheme: "http",
		Host:   ts.Listener.Addr().String(),
		Path:   "metrics",
	}

	err := ssres.evaluateUnderReplicatedPartitions(context.Background(), &adminURL)
	require.Error(t, err)
}

func TestEvaluateEqualThresholdInUnderReplicatedPartition(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `
# HELP vectorized_cluster_partition_under_replicated_replicas Number of under replicated replicas
# TYPE vectorized_cluster_partition_under_replicated_replicas gauge
vectorized_cluster_partition_under_replicated_replicas{namespace="kafka",partition="0",shard="0",topic="test"} 1.000000
`)
	}))
	defer ts.Close()

	ssres := StatefulSetResource{pandaCluster: &redpandav1alpha1.Cluster{
		Spec: redpandav1alpha1.ClusterSpec{
			RestartConfig: &redpandav1alpha1.RestartConfig{
				UnderReplicatedPartitionThreshold: 1,
			},
		},
	}}

	adminURL := url.URL{
		Scheme: "http",
		Host:   ts.Listener.Addr().String(),
		Path:   "metrics",
	}

	err := ssres.evaluateUnderReplicatedPartitions(context.Background(), &adminURL)
	require.NoError(t, err)
}

func TestEvaluateWithoutRestartConfigInUnderReplicatedPartition(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `
# HELP vectorized_cluster_partition_under_replicated_replicas Number of under replicated replicas
# TYPE vectorized_cluster_partition_under_replicated_replicas gauge
vectorized_cluster_partition_under_replicated_replicas{namespace="kafka",partition="0",shard="0",topic="test"} 1.000000
`)
	}))
	defer ts.Close()

	ssres := StatefulSetResource{pandaCluster: &redpandav1alpha1.Cluster{
		Spec: redpandav1alpha1.ClusterSpec{},
	}}

	adminURL := url.URL{
		Scheme: "http",
		Host:   ts.Listener.Addr().String(),
		Path:   "metrics",
	}

	err := ssres.evaluateUnderReplicatedPartitions(context.Background(), &adminURL)
	require.NoError(t, err)
}
