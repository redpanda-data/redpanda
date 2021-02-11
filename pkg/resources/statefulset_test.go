package resources_test

import (
	"context"
	"testing"

	redpandav1alpha1 "github.com/vectorizedio/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	res "github.com/vectorizedio/redpanda/src/go/k8s/pkg/resources"
	v1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/pointer"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestEnsure(t *testing.T) {
	client := fake.NewClientBuilder().Build()

	err := redpandav1alpha1.AddToScheme(scheme.Scheme)
	if err != nil {
		t.Fatalf("Expecting no error but got %v", err)
	}

	sts := res.NewStatefulSet(client, pandaCluster(), scheme.Scheme, ctrl.Log.WithName("test"))

	err = sts.Ensure(context.Background())
	if err != nil {
		t.Fatalf("Expecting no error but got %v", err)
	}

	actual := &v1.StatefulSet{}

	err = client.Get(context.Background(), sts.Key(), actual)
	if err != nil {
		t.Fatalf("Expecting no error but got %v", err)
	}
}

func TestUpdate(t *testing.T) {
	cluster := pandaCluster()
	sts := stsFromCluster(cluster)

	stsWithDifferentReplicas := sts.DeepCopy()

	var newReplicas int32 = 3333
	stsWithDifferentReplicas.Spec.Replicas = &newReplicas

	stsWithDifferentResources := sts.DeepCopy()
	newResources := corev1.ResourceList{
		corev1.ResourceCPU:	resource.MustParse("1111"),
		corev1.ResourceMemory:	resource.MustParse("2222Gi"),
	}
	stsWithDifferentResources.Spec.Template.Spec.Containers[0].Resources.Requests = newResources

	tests := []struct {
		name		string
		sts		*v1.StatefulSet
		expectedUpdate	bool
	}{
		{"no Update", sts, false},
		{"replicas updated", stsWithDifferentReplicas, true},
		{"resources updated", stsWithDifferentResources, true},
	}

	for _, tt := range tests {
		updated := res.Update(tt.sts, pandaCluster(), ctrl.Log.WithName("test"))
		if updated != tt.expectedUpdate {
			t.Errorf("%s: Expected Update %t but got %t", tt.name, tt.expectedUpdate, updated)
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
							Image:	"latest",
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
