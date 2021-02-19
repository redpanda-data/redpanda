// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package resources_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	redpandav1alpha1 "github.com/vectorizedio/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	res "github.com/vectorizedio/redpanda/src/go/k8s/pkg/resources"
	v1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/pointer"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestEnsure(t *testing.T) {
	cluster := pandaCluster()
	stsResource := stsFromCluster(cluster)

	var newReplicas int32 = 3333

	replicasUpdatedCluster := cluster.DeepCopy()
	replicasUpdatedCluster.Spec.Replicas = &newReplicas
	replicasUpdatedSts := stsFromCluster(cluster).DeepCopy()
	replicasUpdatedSts.Spec.Replicas = &newReplicas

	newResources := corev1.ResourceList{
		corev1.ResourceCPU:	resource.MustParse("1111"),
		corev1.ResourceMemory:	resource.MustParse("2222Gi"),
	}
	resourcesUpdatedCluster := cluster.DeepCopy()
	resourcesUpdatedCluster.Spec.Resources.Requests = newResources
	resourcesUpdatedSts := stsFromCluster(cluster).DeepCopy()
	resourcesUpdatedSts.Spec.Template.Spec.Containers[0].Resources.Requests = newResources

	var tests = []struct {
		name		string
		existingObject	client.Object
		pandaCluster	*redpandav1alpha1.Cluster
		expectedObject	*v1.StatefulSet
	}{
		{"none existing", nil, cluster, stsResource},
		{"update replicas", stsResource, replicasUpdatedCluster, replicasUpdatedSts},
		{"update resources", stsResource, resourcesUpdatedCluster, resourcesUpdatedSts},
	}

	for _, tt := range tests {
		c := fake.NewClientBuilder().Build()

		err := redpandav1alpha1.AddToScheme(scheme.Scheme)
		assert.NoError(t, err, tt.name)

		if tt.existingObject != nil {
			tt.existingObject.SetResourceVersion("")

			err = c.Create(context.Background(), tt.existingObject)
			assert.NoError(t, err, tt.name)
		}

		sts := res.NewStatefulSet(c, tt.pandaCluster, scheme.Scheme, nil, ctrl.Log.WithName("test"))

		err = sts.Ensure(context.Background())
		assert.NoError(t, err, tt.name)

		actual := &v1.StatefulSet{}
		err = c.Get(context.Background(), sts.Key(), actual)
		assert.NoError(t, err, tt.name)

		if *actual.Spec.Replicas != *tt.expectedObject.Spec.Replicas || !reflect.DeepEqual(actual.Spec.Template.Spec.Containers[0].Resources.Requests, tt.expectedObject.Spec.Template.Spec.Containers[0].Resources.Requests) {
			t.Errorf("%s: expecting replicas %d and resources %v, got replicas %d and resources %v", tt.name, *actual.Spec.Replicas, actual.Spec.Template.Spec.Containers[0].Resources.Requests, *tt.expectedObject.Spec.Replicas, tt.expectedObject.Spec.Template.Spec.Containers[0].Resources.Requests)
		}
	}
}

func stsFromCluster(pandaCluster *redpandav1alpha1.Cluster) *v1.StatefulSet {
	return &v1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:	pandaCluster.Namespace,
			Name:		pandaCluster.Name,
		},
		Spec: v1.StatefulSetSpec{
			Replicas:	pandaCluster.Spec.Replicas,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Name:		pandaCluster.Name,
					Namespace:	pandaCluster.Namespace,
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:	"redpanda",
							Image:	"image:latest",
							Resources: corev1.ResourceRequirements{
								Limits:		pandaCluster.Spec.Resources.Limits,
								Requests:	pandaCluster.Spec.Resources.Requests,
							},
						},
					},
				},
			},
		},
	}
}

func pandaCluster() *redpandav1alpha1.Cluster {
	var replicas int32 = 1

	resources := corev1.ResourceList{
		corev1.ResourceCPU:	resource.MustParse("1"),
		corev1.ResourceMemory:	resource.MustParse("2Gi"),
	}

	return &redpandav1alpha1.Cluster{
		TypeMeta: metav1.TypeMeta{
			Kind:		"RedpandaCluster",
			APIVersion:	"core.vectorized.io/v1alpha1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:		"cluster",
			Namespace:	"default",
			Labels: map[string]string{
				"app": "redpanda",
			},
		},
		Spec: redpandav1alpha1.ClusterSpec{
			Image:		"image",
			Version:	"latest",
			Replicas:	pointer.Int32Ptr(replicas),
			Configuration: redpandav1alpha1.RedpandaConfig{
				KafkaAPI: redpandav1alpha1.SocketAddress{Port: 123},
			},
			Resources: corev1.ResourceRequirements{
				Limits:		resources,
				Requests:	resources,
			},
		},
	}
}
