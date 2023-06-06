// Copyright 2021 Redpanda Data, Inc.
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
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/pointer"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	vectorizedv1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/vectorized/v1alpha1"
	adminutils "github.com/redpanda-data/redpanda/src/go/k8s/pkg/admin"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/labels"
	res "github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources"
	resourcetypes "github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources/types"
)

const (
	redpandaContainerName = "redpanda"
)

//nolint:funlen // Test function can have more than 100 lines
func TestEnsure(t *testing.T) {
	cluster := pandaCluster()
	stsResource := stsFromCluster(cluster)

	newResources := corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse("1111"),
		corev1.ResourceMemory: resource.MustParse("2222Gi"),
	}
	resourcesUpdatedCluster := cluster.DeepCopy()
	resourcesUpdatedCluster.Spec.Resources.Requests = newResources
	resourcesUpdatedSts := stsFromCluster(cluster).DeepCopy()
	resourcesUpdatedSts.Spec.Template.Spec.InitContainers[0].Resources.Requests = newResources
	resourcesUpdatedSts.Spec.Template.Spec.Containers[0].Resources.Requests = newResources

	// Check that Redpanda resources don't affect Resource Requests
	resourcesUpdatedRedpandaCluster := resourcesUpdatedCluster.DeepCopy()
	resourcesUpdatedRedpandaCluster.Spec.Resources.Redpanda = corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse("1111"),
		corev1.ResourceMemory: resource.MustParse("2000Gi"),
	}

	noSidecarCluster := cluster.DeepCopy()
	noSidecarCluster.Spec.Sidecars.RpkStatus = &vectorizedv1alpha1.Sidecar{
		Enabled: false,
	}
	noSidecarSts := stsFromCluster(noSidecarCluster)

	withoutShadowIndexCacheDirectory := cluster.DeepCopy()
	withoutShadowIndexCacheDirectory.Spec.Version = "v21.10.1"
	stsWithoutSecondPersistentVolume := stsFromCluster(withoutShadowIndexCacheDirectory)
	// Remove shadow-indexing-cache from the volume claim templates
	stsWithoutSecondPersistentVolume.Spec.VolumeClaimTemplates = stsWithoutSecondPersistentVolume.Spec.VolumeClaimTemplates[:1]

	unhealthyRedpandaCluster := cluster.DeepCopy()

	tests := []struct {
		name           string
		existingObject client.Object
		pandaCluster   *vectorizedv1alpha1.Cluster
		expectedObject *v1.StatefulSet
		clusterHealth  bool
		expectedError  error
	}{
		{"none existing", nil, cluster, stsResource, true, nil},
		{"update resources", stsResource, resourcesUpdatedCluster, resourcesUpdatedSts, true, nil},
		{"update redpanda resources", stsResource, resourcesUpdatedRedpandaCluster, resourcesUpdatedSts, true, nil},
		{"disabled sidecar", nil, noSidecarCluster, noSidecarSts, true, nil},
		{"cluster without shadow index cache dir", stsResource, withoutShadowIndexCacheDirectory, stsWithoutSecondPersistentVolume, true, nil},
		{"update none healthy cluster", stsResource, unhealthyRedpandaCluster, stsResource, false, &res.RequeueAfterError{
			RequeueAfter: res.RequeueDuration,
			Msg:          "wait for cluster to become healthy (cluster restarting)",
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := fake.NewClientBuilder().Build()

			err := vectorizedv1alpha1.AddToScheme(scheme.Scheme)
			assert.NoError(t, err, tt.name)

			if tt.existingObject != nil {
				tt.existingObject.SetResourceVersion("")

				err = c.Create(context.Background(), tt.existingObject)
				assert.NoError(t, err, tt.name)
			}

			err = c.Create(context.Background(), tt.pandaCluster)
			assert.NoError(t, err)

			sts := res.NewStatefulSet(
				c,
				tt.pandaCluster,
				scheme.Scheme,
				"cluster.local",
				"servicename",
				types.NamespacedName{Name: "test", Namespace: "test"},
				TestStatefulsetTLSVolumeProvider{},
				TestAdminTLSConfigProvider{},
				"",
				res.ConfiguratorSettings{
					ConfiguratorBaseImage: "vectorized/configurator",
					ConfiguratorTag:       "latest",
					ImagePullPolicy:       "Always",
				},
				func(ctx context.Context) (string, error) { return hash, nil },
				func(ctx context.Context, k8sClient client.Reader, redpandaCluster *vectorizedv1alpha1.Cluster, fqdn string, adminTLSProvider resourcetypes.AdminTLSConfigProvider, ordinals ...int32) (adminutils.AdminAPIClient, error) {
					health := tt.clusterHealth
					adminAPI := &adminutils.MockAdminAPI{Log: ctrl.Log.WithName("testAdminAPI").WithName("mockAdminAPI")}
					adminAPI.SetClusterHealth(health)
					return adminAPI, nil
				},
				time.Second,
				ctrl.Log.WithName("test"),
				0)

			err = sts.Ensure(context.Background())
			if tt.expectedError != nil && errors.Is(err, tt.expectedError) {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err, tt.name)
			}

			actual := &v1.StatefulSet{}
			err = c.Get(context.Background(), sts.Key(), actual)
			assert.NoError(t, err, tt.name)

			actualInitResources := actual.Spec.Template.Spec.InitContainers[0].Resources
			actualRedpandaResources := actual.Spec.Template.Spec.Containers[0].Resources

			expectedInitResources := tt.expectedObject.Spec.Template.Spec.InitContainers[0].Resources
			expectedRedpandaResources := tt.expectedObject.Spec.Template.Spec.Containers[0].Resources

			assert.Equal(t, expectedRedpandaResources, actualRedpandaResources)
			assert.Equal(t, expectedInitResources, actualInitResources)
			configMapHash := actual.Spec.Template.Annotations["redpanda.vectorized.io/configmap-hash"]
			assert.Equal(t, hash, configMapHash)

			if *actual.Spec.Replicas != *tt.expectedObject.Spec.Replicas {
				t.Errorf("%s: expecting replicas %d, got replicas %d", tt.name,
					*tt.expectedObject.Spec.Replicas, *actual.Spec.Replicas)
			}

			for i := range actual.Spec.VolumeClaimTemplates {
				actual.Spec.VolumeClaimTemplates[i].Labels = nil
			}
			assert.Equal(t, tt.expectedObject.Spec.VolumeClaimTemplates, actual.Spec.VolumeClaimTemplates)
		})
	}
}

func stsFromCluster(pandaCluster *vectorizedv1alpha1.Cluster) *v1.StatefulSet {
	fileSystemMode := corev1.PersistentVolumeFilesystem

	sts := &v1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: pandaCluster.Namespace,
			Name:      pandaCluster.Name,
		},
		Spec: v1.StatefulSetSpec{
			Replicas: pandaCluster.Spec.Replicas,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Name:      pandaCluster.Name,
					Namespace: pandaCluster.Namespace,
				},
				Spec: corev1.PodSpec{
					InitContainers: []corev1.Container{{
						Name:  "redpanda-configurator",
						Image: "vectorized/configurator:latest",
						Resources: corev1.ResourceRequirements{
							Limits:   pandaCluster.Spec.Resources.Limits,
							Requests: pandaCluster.Spec.Resources.Requests,
						},
					}},
					Containers: []corev1.Container{
						{
							Name:  "redpanda",
							Image: "image:latest",
							Resources: corev1.ResourceRequirements{
								Limits:   pandaCluster.Spec.Resources.Limits,
								Requests: pandaCluster.Spec.Resources.Requests,
							},
						},
					},
				},
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{
				{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: pandaCluster.Namespace,
						Name:      "datadir",
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceStorage: pandaCluster.Spec.Storage.Capacity,
							},
						},
						StorageClassName: &pandaCluster.Spec.Storage.StorageClassName,
						VolumeMode:       &fileSystemMode,
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: pandaCluster.Namespace,
						Name:      "shadow-index-cache",
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceStorage: pandaCluster.Spec.CloudStorage.CacheStorage.Capacity,
							},
						},
						StorageClassName: &pandaCluster.Spec.CloudStorage.CacheStorage.StorageClassName,
						VolumeMode:       &fileSystemMode,
					},
				},
			},
		},
	}
	if pandaCluster.Spec.Sidecars.RpkStatus.Enabled {
		sts.Spec.Template.Spec.Containers = append(sts.Spec.Template.Spec.Containers, corev1.Container{
			Name:      "rpk-status",
			Image:     "image:latest",
			Resources: *pandaCluster.Spec.Sidecars.RpkStatus.Resources,
		})
	}
	return sts
}

func pandaCluster() *vectorizedv1alpha1.Cluster {
	var replicas int32 = 1

	resources := corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse("1"),
		corev1.ResourceMemory: resource.MustParse("2Gi"),
	}

	return &vectorizedv1alpha1.Cluster{
		TypeMeta: metav1.TypeMeta{
			Kind:       "RedpandaCluster",
			APIVersion: "core.vectorized.io/v1alpha1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cluster",
			Namespace: "default",
			Labels: map[string]string{
				"app": "redpanda",
			},
			UID: "ff2770aa-c919-43f0-8b4a-30cb7cfdaf79",
		},
		Spec: vectorizedv1alpha1.ClusterSpec{
			Image:    "image",
			Version:  "v22.3.0",
			Replicas: pointer.Int32(replicas),
			CloudStorage: vectorizedv1alpha1.CloudStorageConfig{
				Enabled: true,
				CacheStorage: &vectorizedv1alpha1.StorageSpec{
					Capacity:         resource.MustParse("10Gi"),
					StorageClassName: "local",
				},
				SecretKeyRef: corev1.ObjectReference{
					Namespace: "default",
					Name:      "archival",
				},
			},
			Configuration: vectorizedv1alpha1.RedpandaConfig{
				AdminAPI: []vectorizedv1alpha1.AdminAPI{{Port: 345}},
				KafkaAPI: []vectorizedv1alpha1.KafkaAPI{{Port: 123, AuthenticationMethod: "none"}},
			},
			Resources: vectorizedv1alpha1.RedpandaResourceRequirements{
				ResourceRequirements: corev1.ResourceRequirements{
					Limits:   resources,
					Requests: resources,
				},
				Redpanda: nil,
			},
			Sidecars: vectorizedv1alpha1.Sidecars{
				RpkStatus: &vectorizedv1alpha1.Sidecar{
					Enabled: true,
					Resources: &corev1.ResourceRequirements{
						Limits:   resources,
						Requests: resources,
					},
				},
			},
			Storage: vectorizedv1alpha1.StorageSpec{
				Capacity:         resource.MustParse("10Gi"),
				StorageClassName: "storage-class",
			},
		},
	}
}

func TestVersion(t *testing.T) {
	tests := []struct {
		Containers      []corev1.Container
		ExpectedVersion string
	}{
		{Containers: []corev1.Container{{Name: redpandaContainerName, Image: "vectorized/redpanda:v21.11.11"}}, ExpectedVersion: "v21.11.11"},
		{Containers: []corev1.Container{{Name: redpandaContainerName, Image: "vectorized/redpanda:"}}, ExpectedVersion: ""},
		// Image with no tag does not return "latest" as version.
		{Containers: []corev1.Container{{Name: redpandaContainerName, Image: "vectorized/redpanda"}}, ExpectedVersion: ""},
		{Containers: []corev1.Container{{Name: redpandaContainerName, Image: "localhost:5000/redpanda:v21.11.11"}}, ExpectedVersion: "v21.11.11"},
		{Containers: []corev1.Container{{Name: redpandaContainerName, Image: "localhost:5000/redpanda"}}, ExpectedVersion: ""},
		{Containers: []corev1.Container{{Name: "", Image: "vectorized/redpanda"}}, ExpectedVersion: ""},
	}

	for _, tt := range tests {
		sts := &res.StatefulSetResource{
			LastObservedState: &v1.StatefulSet{
				Spec: v1.StatefulSetSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: tt.Containers,
						},
					},
				},
			},
		}
		assert.Equal(t, tt.ExpectedVersion, sts.Version())
	}
}

//nolint:funlen // this is ok for a test
func TestCurrentVersion(t *testing.T) {
	redpanda := pandaCluster()
	tests := []struct {
		name             string
		pods             []corev1.Pod
		expectedReplicas int32
		expectedVersion  string
		shouldError      bool
	}{
		{"one pod with matching version", []corev1.Pod{
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test",
					Namespace: redpanda.Namespace,
				},
				Spec:   corev1.PodSpec{Containers: []corev1.Container{{Name: redpandaContainerName, Image: "vectorized/redpanda:v21.11.11"}}},
				Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}},
			},
		}, 1, "v21.11.11", false},
		{"one pod with matching version, not in ready state", []corev1.Pod{
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test",
					Namespace: redpanda.Namespace,
				},
				Spec:   corev1.PodSpec{Containers: []corev1.Container{{Name: redpandaContainerName, Image: "vectorized/redpanda:v21.11.11"}}},
				Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionFalse}}},
			},
		}, 1, "v21.11.11", true},
		{"one pod with matching version but expecting two replicas", []corev1.Pod{
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test",
					Namespace: redpanda.Namespace,
				},
				Spec:   corev1.PodSpec{Containers: []corev1.Container{{Name: redpandaContainerName, Image: "vectorized/redpanda:v21.11.11"}}},
				Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}},
			},
		}, 2, "v21.11.11", true},
		{"one pod with matching and one pod with non-matching version", []corev1.Pod{
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test",
					Namespace: redpanda.Namespace,
				},
				Spec:   corev1.PodSpec{Containers: []corev1.Container{{Name: redpandaContainerName, Image: "vectorized/redpanda:v21.11.11"}}},
				Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}},
			},
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test2",
					Namespace: redpanda.Namespace,
				},
				Spec:   corev1.PodSpec{Containers: []corev1.Container{{Name: redpandaContainerName, Image: "vectorized/redpanda:v22.2.2"}}},
				Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}},
			},
		}, 2, "v21.11.11", true},
	}

	for _, tt := range tests {
		c := fake.NewClientBuilder().Build()
		for i := range tt.pods {
			pod := tt.pods[i]
			pod.Labels = labels.ForCluster(redpanda)
			assert.NoError(t, c.Create(context.TODO(), &pod))
		}
		sts := res.NewStatefulSet(c, redpanda, scheme.Scheme,
			"cluster.local",
			"servicename",
			types.NamespacedName{Name: "test", Namespace: "test"},
			TestStatefulsetTLSVolumeProvider{},
			TestAdminTLSConfigProvider{},
			"",
			res.ConfiguratorSettings{
				ConfiguratorBaseImage: "vectorized/configurator",
				ConfiguratorTag:       "latest",
				ImagePullPolicy:       "Always",
			},
			func(ctx context.Context) (string, error) { return hash, nil },
			func(ctx context.Context, k8sClient client.Reader, redpandaCluster *vectorizedv1alpha1.Cluster, fqdn string, adminTLSProvider resourcetypes.AdminTLSConfigProvider, ordinals ...int32) (adminutils.AdminAPIClient, error) {
				return nil, nil
			},
			time.Second,
			ctrl.Log.WithName("test"),
			0)
		sts.LastObservedState = &v1.StatefulSet{
			Spec: v1.StatefulSetSpec{
				Replicas: &tt.expectedReplicas,
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{{Name: redpandaContainerName, Image: "vectorized/redpanda:v21.11.11"}},
					},
				},
			},
		}
		v, err := sts.CurrentVersion(context.TODO())
		assert.Equal(t, tt.expectedVersion, v)
		if tt.shouldError {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
		}
	}
}
