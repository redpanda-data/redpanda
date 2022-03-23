package utils_test

import (
	"encoding/json"
	"testing"

	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestPatchComputation(t *testing.T) {
	currObj := corev1.Pod{
		ObjectMeta: v1.ObjectMeta{
			Name: "res",
			Annotations: map[string]string{
				"redpanda.vectorized.io/configmap-hash": "6fb7ad08cabb116f7c1c8dc1127997a0",
				"c":                                     "d",
			},
		},
		Spec: corev1.PodSpec{
			RestartPolicy: "Always",
		},
	}
	modObj := currObj.DeepCopy()
	modObj.Annotations["e"] = "f"

	current, err := json.Marshal(currObj)
	require.NoError(t, err)
	modified, err := json.Marshal(modObj)
	require.NoError(t, err)

	opt := utils.IgnoreAnnotation("redpanda.vectorized.io/configmap-hash")
	current, modified, err = opt(current, modified)
	require.NoError(t, err)
	currObj2 := corev1.Pod{}
	err = json.Unmarshal(current, &currObj2)
	require.NoError(t, err)
	modObj2 := corev1.Pod{}
	err = json.Unmarshal(modified, &modObj2)
	require.NoError(t, err)
	assert.Empty(t, currObj2.Annotations["redpanda.vectorized.io/configmap-hash"])
	assert.Equal(t, currObj2.Annotations["c"], "d")
	assert.Empty(t, currObj2.Annotations["e"])
	assert.Len(t, currObj2.Annotations, 1)
	assert.Empty(t, modObj2.Annotations["redpanda.vectorized.io/configmap-hash"])
	assert.Equal(t, modObj2.Annotations["c"], "d")
	assert.Equal(t, modObj2.Annotations["e"], "f")
	assert.Len(t, modObj2.Annotations, 2)
}

func TestStatefulSetPatchComputation(t *testing.T) {
	sts := appsv1.StatefulSet{
		Spec: appsv1.StatefulSetSpec{
			Template: corev1.PodTemplateSpec{
				ObjectMeta: v1.ObjectMeta{
					Annotations: map[string]string{
						"a": "b",
						"c": "d",
					},
				},
			},
		},
	}

	sts2 := sts.DeepCopy()
	sts2.Spec.Template.Annotations["e"] = "f"

	current, err := json.Marshal(sts)
	require.NoError(t, err)
	modified, err := json.Marshal(sts2)
	require.NoError(t, err)

	opt := utils.IgnoreAnnotation("a")
	current, modified, err = opt(current, modified)
	require.NoError(t, err)

	stsRes1 := appsv1.StatefulSet{}
	err = json.Unmarshal(current, &stsRes1)
	require.NoError(t, err)
	assert.Len(t, stsRes1.Spec.Template.Annotations, 1)
	assert.Empty(t, stsRes1.Spec.Template.Annotations["a"])

	stsRes2 := appsv1.StatefulSet{}
	err = json.Unmarshal(modified, &stsRes2)
	require.NoError(t, err)
	assert.Len(t, stsRes2.Spec.Template.Annotations, 2)
	assert.Empty(t, stsRes2.Spec.Template.Annotations["a"])
}
