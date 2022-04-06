// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package resources // nolint:testpackage // needed to test private method

import (
	"testing"

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
	ssres := StatefulSetResource{}
	update, err := ssres.shouldUpdate(false, sts, stsWithAnnotation)
	require.NoError(t, err)
	require.True(t, update)

	// same statefulset with same annotation
	update, err = ssres.shouldUpdate(false, stsWithAnnotation, stsWithAnnotation)
	require.NoError(t, err)
	require.False(t, update)
}
