package v1alpha1_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vectorizedio/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestGetRedpandaResources(t *testing.T) {
	resources := corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse("1"),
		corev1.ResourceMemory: resource.MustParse("2Gi"),
	}
	rpkResources := corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse("0.5"),
		corev1.ResourceMemory: resource.MustParse("1Gi"),
	}
	redpandaCluster := &v1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test",
			Namespace: "",
		},
		Spec: v1alpha1.ClusterSpec{
			Resources: corev1.ResourceRequirements{
				Limits:   resources,
				Requests: resources,
			},
			Sidecars: v1alpha1.Sidecars{
				RpkStatus: &v1alpha1.Sidecar{
					Enabled: true,
					Resources: &corev1.ResourceRequirements{
						Limits:   rpkResources,
						Requests: rpkResources,
					},
				},
			},
		},
	}
	rpResources := redpandaCluster.GetRedpandaResources()
	assert.True(t, resource.MustParse("0.5").Equal(*rpResources.Limits.Cpu()), "limits cpu")
	assert.True(t, resource.MustParse("0.5").Equal(*rpResources.Requests.Cpu()), "requests cpu")
	assert.True(t, resource.MustParse("1Gi").Equal(*rpResources.Limits.Memory()), "limits memory")
	assert.True(t, resource.MustParse("1Gi").Equal(*rpResources.Requests.Memory()), "requests memory")
}
