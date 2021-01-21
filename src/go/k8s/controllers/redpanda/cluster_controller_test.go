// Copyright 2021 Vectorized, Inc.
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
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/vectorizedio/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/pointer"
)

var _ = Describe("RedPandaCluster controller", func() {

	const (
		timeout		= time.Second * 30
		interval	= time.Second * 1

		kafkaPort			= 9092
		redpandaConfigurationFile	= "redpanda.yaml"
		replicas			= 1
		redpandaContainerTag		= "x"
		redpandaContainerImage		= "vectorized/redpanda"
	)

	Context("When creating RedpandaCluster", func() {
		It("Should create Redpanda cluster", func() {
			resources := corev1.ResourceList{
				corev1.ResourceCPU:	resource.MustParse("1"),
				corev1.ResourceMemory:	resource.MustParse("2Gi"),
			}

			key := types.NamespacedName{
				Name:		"redpanda-test",
				Namespace:	"default",
			}
			seedKey := types.NamespacedName{
				Namespace:	"default",
				Name:		"redpanda-test-seed",
			}
			redpandaCluster := &v1alpha1.Cluster{
				TypeMeta: metav1.TypeMeta{
					Kind:		"RedpandaCluster",
					APIVersion:	"core.vectorized.io/v1alpha1",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:		key.Name,
					Namespace:	key.Namespace,
					Labels: map[string]string{
						"app": "redpanda",
					},
				},
				Spec: v1alpha1.ClusterSpec{
					Image:		redpandaContainerImage,
					Version:	redpandaContainerTag,
					Replicas:	pointer.Int32Ptr(replicas),
					Configuration: v1alpha1.RedpandaConfig{
						KafkaAPI: v1alpha1.SocketAddress{Port: kafkaPort},
					},
					Resources: corev1.ResourceRequirements{
						Limits:		resources,
						Requests:	resources,
					},
				},
			}
			Expect(k8sClient.Create(context.Background(), redpandaCluster)).Should(Succeed())

			By("Creating headless Service")
			var svc corev1.Service
			Eventually(func() bool {
				err := k8sClient.Get(context.Background(), key, &svc)
				return err == nil &&
					svc.Spec.ClusterIP == corev1.ClusterIPNone &&
					svc.Spec.Ports[0].Port == kafkaPort &&
					validOwner(redpandaCluster, svc.OwnerReferences)
			}, timeout, interval).Should(BeTrue())

			By("Creating Configmap with the redpanda configuration")
			var cm corev1.ConfigMap
			Eventually(func() bool {
				err := k8sClient.Get(context.Background(), seedKey, &cm)
				if err != nil {
					return false
				}
				_, exist := cm.Data[redpandaConfigurationFile]
				return exist &&
					validOwner(redpandaCluster, cm.OwnerReferences)
			}, timeout, interval).Should(BeTrue())

			By("Creating StatefulSet")
			var sts appsv1.StatefulSet
			Eventually(func() bool {
				err := k8sClient.Get(context.Background(), seedKey, &sts)
				return err == nil &&
					*sts.Spec.Replicas == replicas &&
					sts.Spec.Template.Spec.Containers[0].Image == "vectorized/redpanda:"+redpandaContainerTag &&
					validOwner(redpandaCluster, sts.OwnerReferences)
			}, timeout, interval).Should(BeTrue())

			Expect(sts.Spec.Template.Spec.Containers[0].Resources.Requests).Should(Equal(resources))
			Expect(sts.Spec.Template.Spec.Containers[0].Resources.Limits).Should(Equal(resources))
		})
	})
})

func validOwner(
	cluster *v1alpha1.Cluster, owners []metav1.OwnerReference,
) bool {
	if len(owners) != 1 {
		return false
	}
	owner := owners[0]
	return owner.Name == cluster.Name && owner.Controller != nil && *owner.Controller
}
