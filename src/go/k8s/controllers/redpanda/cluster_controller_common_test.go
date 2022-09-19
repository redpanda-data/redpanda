// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package redpanda_test

import (
	"context"
	"fmt"

	v1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/labels"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func resourceGetter(key client.ObjectKey, res client.Object) func() error {
	return func() error {
		return k8sClient.Get(context.Background(), key, res)
	}
}

func resourceDataGetter(
	key client.ObjectKey, res client.Object, extractor func() interface{},
) func() interface{} {
	return func() interface{} {
		err := resourceGetter(key, res)()
		if err != nil {
			return err
		}
		return extractor()
	}
}

func annotationGetter(
	key client.ObjectKey, res client.Object, name string,
) func() string {
	return func() string {
		if err := resourceGetter(key, res)(); err != nil {
			return fmt.Sprintf("client error: %+v", err)
		}
		if sts, ok := res.(*appsv1.StatefulSet); ok {
			return sts.Spec.Template.Annotations[name]
		}
		return res.GetAnnotations()[name]
	}
}

func clusterConfiguredConditionGetter(
	key client.ObjectKey,
) func() *v1alpha1.ClusterCondition {
	return func() *v1alpha1.ClusterCondition {
		var cluster v1alpha1.Cluster
		if err := k8sClient.Get(context.Background(), key, &cluster); err != nil {
			return nil
		}
		return cluster.Status.GetCondition(v1alpha1.ClusterConfiguredConditionType)
	}
}

func clusterConfiguredConditionStatusGetter(key client.ObjectKey) func() bool {
	return func() bool {
		cond := clusterConfiguredConditionGetter(key)()
		return cond != nil && cond.Status == corev1.ConditionTrue
	}
}

func clusterUpdater(
	clusterNamespacedName types.NamespacedName, upd func(*v1alpha1.Cluster),
) func() error {
	return func() error {
		cl := &v1alpha1.Cluster{}
		if err := k8sClient.Get(context.Background(), clusterNamespacedName, cl); err != nil {
			return err
		}
		upd(cl)
		return k8sClient.Update(context.Background(), cl)
	}
}

func consoleUpdater(
	consoleNamespacedName types.NamespacedName, upd func(*v1alpha1.Console),
) func() error {
	return func() error {
		con := &v1alpha1.Console{}
		if err := k8sClient.Get(context.Background(), consoleNamespacedName, con); err != nil {
			return err
		}
		upd(con)
		return k8sClient.Update(context.Background(), con)
	}
}

func statefulSetReplicasReconciler(
	key types.NamespacedName, cluster *v1alpha1.Cluster,
) func() error {
	return func() error {
		var sts appsv1.StatefulSet
		err := k8sClient.Get(context.Background(), key, &sts)
		if err != nil {
			return err
		}

		// Aligning Pods first
		var podList corev1.PodList
		err = k8sClient.List(context.Background(), &podList, &client.ListOptions{
			Namespace:     key.Namespace,
			LabelSelector: labels.ForCluster(cluster).AsClientSelector(),
		})
		if err != nil {
			return err
		}

		pods := make(map[string]bool, len(podList.Items))
		for i := range podList.Items {
			pods[podList.Items[i].Name] = true
		}

		for i := int32(0); i < *sts.Spec.Replicas; i++ {
			podName := fmt.Sprintf("%s-%d", key.Name, i)
			var pod corev1.Pod
			if pods[podName] {
				for j := range podList.Items {
					if podList.Items[j].Name == podName {
						pod = *podList.Items[j].DeepCopy()
					}
				}
			}
			pod.Name = podName
			pod.Namespace = key.Namespace
			pod.Labels = labels.ForCluster(cluster)
			pod.Annotations = sts.Spec.Template.Annotations
			pod.Spec = sts.Spec.Template.Spec

			if pods[podName] {
				delete(pods, podName)
				err = k8sClient.Update(context.Background(), &pod)
				if err != nil {
					return err
				}
			} else {
				err = k8sClient.Create(context.Background(), &pod)
				if err != nil {
					return err
				}
			}
		}

		for i := range podList.Items {
			if pods[podList.Items[i].Name] {
				err = k8sClient.Delete(context.Background(), &podList.Items[i])
				if err != nil {
					return err
				}
			}
		}

		// Aligning StatefulSet
		sts.Status.Replicas = *sts.Spec.Replicas
		sts.Status.ReadyReplicas = sts.Status.Replicas
		return k8sClient.Status().Update(context.Background(), &sts)
	}
}
