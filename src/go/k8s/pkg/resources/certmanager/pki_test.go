package certmanager_test

import (
	"context"
	"testing"

	cmapiv1 "github.com/cert-manager/cert-manager/pkg/apis/certmanager/v1"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/pointer"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	vectorizedv1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/vectorized/v1alpha1"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources/certmanager"
)

func TestKafkaAPIWithMultipleTLSListeners(t *testing.T) {
	require.NoError(t, vectorizedv1alpha1.AddToScheme(scheme.Scheme))
	require.NoError(t, cmapiv1.AddToScheme(scheme.Scheme))
	clusterWithMultipleTLS := pandaCluster().DeepCopy()
	clusterWithMultipleTLS.Spec.Configuration.KafkaAPI[0].TLS = vectorizedv1alpha1.KafkaAPITLS{Enabled: true, RequireClientAuth: true}
	clusterWithMultipleTLS.Spec.Configuration.KafkaAPI = append(clusterWithMultipleTLS.Spec.Configuration.KafkaAPI, vectorizedv1alpha1.KafkaAPI{Port: 30001, External: vectorizedv1alpha1.ExternalConnectivityConfig{Enabled: true}, TLS: vectorizedv1alpha1.KafkaAPITLS{Enabled: true}})

	testcases := []struct {
		name                 string
		cluster              vectorizedv1alpha1.Cluster
		expectedCertificates []string
	}{
		{
			name:                 "Two listeners with TLS",
			cluster:              *clusterWithMultipleTLS,
			expectedCertificates: []string{"cluster-kafka-root-certificate", "cluster-redpanda"},
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			c := fake.NewClientBuilder().Build()
			pkiRes, err := certmanager.NewPki(
				context.TODO(),
				c,
				&tc.cluster,
				"cluster.local1",
				"cluster.local",
				scheme.Scheme,
				ctrl.Log.WithName("test"))
			require.NoError(t, err)
			require.NoError(t, pkiRes.Ensure(context.TODO()))

			for _, cert := range tc.expectedCertificates {
				actual := &cmapiv1.Certificate{}
				err := c.Get(context.Background(), types.NamespacedName{Name: cert, Namespace: pandaCluster().Namespace}, actual)
				require.NoError(t, err)
			}
		})
	}
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
			Version:  "v21.11.1",
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
				KafkaAPI: []vectorizedv1alpha1.KafkaAPI{{Port: 123}},
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
