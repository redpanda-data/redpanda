package resources // nolint:testpackage // needed to test private method

import (
	"testing"

	"github.com/banzaicloud/k8s-objectmatcher/patch"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	update, err := shouldUpdate(false, sts, stsWithAnnotation)
	require.NoError(t, err)
	require.True(t, update)

	// same statefulset with same annotation
	update, err = shouldUpdate(false, stsWithAnnotation, stsWithAnnotation)
	require.NoError(t, err)
	require.False(t, update)
}

// I am keeping this test in place to document how does 3-way merge patch behave
// when it cannot retrieve original configuration from an annotation. Looking at
// the description in docs
// https://kubernetes.io/docs/tasks/manage-kubernetes-objects/declarative-config/#merge-patch-calculation
// to be able to calculate what lines should be removed, it requires the
// original configuration to compute it
func TestPatchCalculation_PodRemoved(t *testing.T) {
	podWithOneContainer := corev1.Pod{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:    "test",
					Image:   "nginx",
					Command: []string{"nginx"},
				},
			},
		},
	}
	podWithTwoContainers := corev1.Pod{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:    "test",
					Image:   "nginx",
					Command: []string{"nginx"},
				},
				{
					Name:    "test2",
					Image:   "nginx",
					Command: []string{"nginx"},
				},
			},
		},
	}

	opts := []patch.CalculateOption{
		patch.IgnoreStatusFields(),
		ignoreKubernetesTokenVolumeMounts(),
		ignoreDefaultToleration(),
	}
	patchResult, err := patch.DefaultPatchMaker.Calculate(&podWithTwoContainers, &podWithOneContainer, opts...)
	require.NoError(t, err)
	// this case, the 3-way merge runs without knowing the original
	// configuration so it cannot create diff removing the pod
	require.True(t, patchResult.IsEmpty())

	err = patch.DefaultAnnotator.SetLastAppliedAnnotation(&podWithTwoContainers)
	require.NoError(t, err)
	patchResult, err = patch.DefaultPatchMaker.Calculate(&podWithTwoContainers, &podWithOneContainer, opts...)
	require.NoError(t, err)
	// this time there's last applied annotation that we set so 3-way merge has
	// the original configuration and computes diff that removes the pod
	require.False(t, patchResult.IsEmpty())
}
