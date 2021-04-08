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
	"github.com/vectorizedio/redpanda/src/go/k8s/controllers/redpanda"
	res "github.com/vectorizedio/redpanda/src/go/k8s/pkg/resources"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/pointer"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

var _ = Describe("RedPandaCluster controller", func() {

	const (
		timeout  = time.Second * 30
		interval = time.Second * 1

		adminPort                 = 9644
		kafkaPort                 = 9092
		redpandaConfigurationFile = "redpanda.yaml"
		replicas                  = 1
		redpandaContainerTag      = "x"
		redpandaContainerImage    = "vectorized/redpanda"
	)

	Context("When creating RedpandaCluster", func() {
		It("Should create Redpanda cluster with corresponding resources", func() {
			resources := corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("1"),
				corev1.ResourceMemory: resource.MustParse("2Gi"),
			}

			key := types.NamespacedName{
				Name:      "redpanda-test",
				Namespace: "default",
			}
			baseKey := types.NamespacedName{
				Name:      key.Name + "-base",
				Namespace: "default",
			}
			clusterRoleKey := types.NamespacedName{
				Name:      "redpanda-init-configurator",
				Namespace: "",
			}
			redpandaCluster := &v1alpha1.Cluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      key.Name,
					Namespace: key.Namespace,
					Labels: map[string]string{
						"app": "redpanda",
					},
				},
				Spec: v1alpha1.ClusterSpec{
					Image:    redpandaContainerImage,
					Version:  redpandaContainerTag,
					Replicas: pointer.Int32Ptr(replicas),
					Configuration: v1alpha1.RedpandaConfig{
						KafkaAPI: []v1alpha1.KafkaAPIListener{
							{Name: "kafka", Port: kafkaPort},
							{Name: "external", External: v1alpha1.ExternalConnectivityConfig{Enabled: true}},
						},
						AdminAPI: v1alpha1.SocketAddress{Port: adminPort},
					},
					Resources: corev1.ResourceRequirements{
						Limits:   resources,
						Requests: resources,
					},
				},
			}
			Expect(k8sClient.Create(context.Background(), redpandaCluster)).Should(Succeed())

			redpandaPod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      key.Name,
					Namespace: key.Namespace,
					Labels: map[string]string{
						"app.kubernetes.io/component": "redpanda",
						"app.kubernetes.io/instance":  "redpanda-test",
						"app.kubernetes.io/name":      "redpanda",
					},
				},
				Spec: corev1.PodSpec{
					NodeName: "test-node",
					Containers: []corev1.Container{{
						Name:  "test",
						Image: "test",
					}},
				},
				Status: corev1.PodStatus{},
			}
			Expect(k8sClient.Create(context.Background(), redpandaPod)).Should(Succeed())

			node := &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-node",
				},
				Spec: corev1.NodeSpec{},
				Status: corev1.NodeStatus{
					Addresses: []corev1.NodeAddress{{
						Type:    corev1.NodeExternalIP,
						Address: "9.8.7.6",
					}},
				},
			}
			Expect(k8sClient.Create(context.Background(), node)).Should(Succeed())

			By("Creating headless Service")
			var svc corev1.Service
			Eventually(func() bool {
				err := k8sClient.Get(context.Background(), key, &svc)
				return err == nil &&
					svc.Spec.ClusterIP == corev1.ClusterIPNone &&
					findPort(svc.Spec.Ports, "kafka") == kafkaPort &&
					findPort(svc.Spec.Ports, res.AdminPortName) == adminPort &&
					validOwner(redpandaCluster, svc.OwnerReferences)
			}, timeout, interval).Should(BeTrue())

			By("Creating NodePort Service")
			Eventually(func() bool {
				err := k8sClient.Get(context.Background(), types.NamespacedName{
					Name:      key.Name + "-external",
					Namespace: key.Namespace,
				}, &svc)
				return err == nil &&
					svc.Spec.Type == corev1.ServiceTypeNodePort &&
					findPort(svc.Spec.Ports, "external") == kafkaPort+1 &&
					findPort(svc.Spec.Ports, res.AdminPortName) == adminPort &&
					validOwner(redpandaCluster, svc.OwnerReferences)
			}, timeout, interval).Should(BeTrue())

			By("Creating Configmap with the redpanda configuration")
			var cm corev1.ConfigMap
			Eventually(func() bool {
				err := k8sClient.Get(context.Background(), baseKey, &cm)
				if err != nil {
					return false
				}
				_, exist := cm.Data[redpandaConfigurationFile]
				return exist &&
					validOwner(redpandaCluster, cm.OwnerReferences)
			}, timeout, interval).Should(BeTrue())

			By("Creating ServiceAcount")
			var sa corev1.ServiceAccount
			Eventually(func() bool {
				err := k8sClient.Get(context.Background(), key, &sa)
				return err == nil
			}, timeout, interval).Should(BeTrue())

			By("Creating ClusterRole")
			var cr v1.ClusterRole
			Eventually(func() bool {
				err := k8sClient.Get(context.Background(), clusterRoleKey, &cr)
				return err == nil &&
					cr.Rules[0].Verbs[0] == "get" &&
					cr.Rules[0].Resources[0] == "nodes"
			}, timeout, interval).Should(BeTrue())

			By("Creating ClusterRoleBinding")
			var crb v1.ClusterRoleBinding
			Eventually(func() bool {
				err := k8sClient.Get(context.Background(), clusterRoleKey, &crb)
				return err == nil &&
					crb.RoleRef.Name == clusterRoleKey.Name &&
					crb.RoleRef.Kind == "ClusterRole" &&
					crb.Subjects[0].Name == key.Name &&
					crb.Subjects[0].Kind == "ServiceAccount"
			}, timeout, interval).Should(BeTrue())

			By("Creating StatefulSet")
			var sts appsv1.StatefulSet
			Eventually(func() bool {
				err := k8sClient.Get(context.Background(), key, &sts)
				return err == nil &&
					*sts.Spec.Replicas == replicas &&
					sts.Spec.Template.Spec.Containers[0].Image == "vectorized/redpanda:"+redpandaContainerTag &&
					validOwner(redpandaCluster, sts.OwnerReferences)
			}, timeout, interval).Should(BeTrue())

			Expect(sts.Spec.Template.Spec.Containers[0].Resources.Requests).Should(Equal(resources))
			Expect(sts.Spec.Template.Spec.Containers[0].Resources.Limits).Should(Equal(resources))
			Expect(sts.Spec.Template.Spec.Containers[0].Env).Should(ContainElement(corev1.EnvVar{Name: "REDPANDA_ENVIRONMENT", Value: "kubernetes"}))

			By("Reporting nodes internal and external")
			var rc v1alpha1.Cluster
			Eventually(func() bool {
				err := k8sClient.Get(context.Background(), key, &rc)
				return err == nil &&
					len(rc.Status.Nodes.Internal) == 1 &&
					len(rc.Status.Nodes.External) == 1 &&
					len(rc.Status.Nodes.ExternalAdmin) == 1
			}, timeout, interval).Should(BeTrue())
		})
		It("creates redpanda cluster with tls enabled", func() {
			resources := corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("1"),
				corev1.ResourceMemory: resource.MustParse("2Gi"),
			}

			key := types.NamespacedName{
				Name:      "redpanda-test-tls",
				Namespace: "default",
			}
			redpandaCluster := &v1alpha1.Cluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      key.Name,
					Namespace: key.Namespace,
				},
				Spec: v1alpha1.ClusterSpec{
					Image:    redpandaContainerImage,
					Version:  redpandaContainerTag,
					Replicas: pointer.Int32Ptr(replicas),
					Configuration: v1alpha1.RedpandaConfig{
						KafkaAPI: []v1alpha1.KafkaAPIListener{
							{
								Name: "kafka",
								Port: kafkaPort,
								TLS:  v1alpha1.KafkaAPITLS{Enabled: true, RequireClientAuth: true},
							},
						},
						AdminAPI: v1alpha1.SocketAddress{Port: adminPort},
					},
					Resources: corev1.ResourceRequirements{
						Limits:   resources,
						Requests: resources,
					},
				},
			}
			Expect(k8sClient.Create(context.Background(), redpandaCluster)).Should(Succeed())

			By("Creating StatefulSet")
			var sts appsv1.StatefulSet
			Eventually(func() bool {
				err := k8sClient.Get(context.Background(), key, &sts)
				return err == nil &&
					*sts.Spec.Replicas == replicas
			}, timeout, interval).Should(BeTrue())

			var defaultMode int32 = 420
			Expect(sts.Spec.Template.Spec.Containers[0].VolumeMounts).Should(
				ContainElements(
					corev1.VolumeMount{Name: "tlscert", MountPath: "/etc/tls/certs"},
					corev1.VolumeMount{Name: "tlsca", MountPath: "/etc/tls/certs/ca"},
				))
			Expect(sts.Spec.Template.Spec.Volumes).Should(
				ContainElements(
					corev1.Volume{
						Name: "tlscert",
						VolumeSource: corev1.VolumeSource{
							Secret: &corev1.SecretVolumeSource{
								SecretName: "redpanda-test-tls-redpanda",
								Items: []corev1.KeyToPath{
									{
										Key:  "tls.key",
										Path: "tls.key",
									},
									{
										Key:  "tls.crt",
										Path: "tls.crt",
									},
								},
								DefaultMode: &defaultMode,
							}}},
					corev1.Volume{
						Name: "tlsca",
						VolumeSource: corev1.VolumeSource{
							Secret: &corev1.SecretVolumeSource{
								SecretName: "redpanda-test-tls-operator-client",
								Items: []corev1.KeyToPath{
									{
										Key:  "ca.crt",
										Path: "ca.crt",
									},
								},
								DefaultMode: &defaultMode,
							}}}))
		})
	})

	Context("Calling reconcile", func() {
		It("Should not throw error on non-existing CRB and cluster", func() {
			// this test is started with fake client that was not initialized,
			// so neither redpanda Cluster object or CRB or any other object
			// exists. This verifies that these situations are handled
			// gracefully and without error
			r := &redpanda.ClusterReconciler{
				Client: fake.NewClientBuilder().Build(),
				Log:    ctrl.Log,
				Scheme: scheme.Scheme,
			}
			_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{
				Namespace: "default",
				Name:      "nonexisting",
			}})
			Expect(err).To(Succeed())

		})
	})
})

func findPort(ports []corev1.ServicePort, name string) int32 {
	for _, port := range ports {
		if port.Name == name {
			return port.Port
		}
	}
	return 0
}

func validOwner(
	cluster *v1alpha1.Cluster, owners []metav1.OwnerReference,
) bool {
	if len(owners) != 1 {
		return false
	}

	owner := owners[0]

	return owner.Name == cluster.Name && owner.Controller != nil && *owner.Controller
}
