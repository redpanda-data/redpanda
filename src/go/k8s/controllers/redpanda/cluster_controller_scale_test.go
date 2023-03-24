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
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"gopkg.in/yaml.v2"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/pointer"

	"github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources/featuregates"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/api/admin"
)

var _ = Describe("Redpanda cluster scale resource", func() {
	const (
		timeout  = time.Second * 30
		interval = time.Millisecond * 100

		timeoutShort  = time.Millisecond * 100
		intervalShort = time.Millisecond * 20
	)

	Context("When starting up a fresh Redpanda cluster", func() {
		It("Should wait for the first replica to come up before launching the others", func() {
			By("Allowing creation of a new cluster with 3 replicas")
			key, redpandaCluster := getClusterWithReplicas("startup", 3)
			redpandaCluster.Spec.Version = featuregates.V22_2_1.String()
			Expect(k8sClient.Create(context.Background(), redpandaCluster)).Should(Succeed())

			By("Keeping the StatefulSet at single replica until initialized")
			var sts appsv1.StatefulSet
			Eventually(resourceGetter(key, &sts), timeout, interval).Should(Succeed())
			Consistently(resourceDataGetter(key, &sts, func() interface{} {
				return *sts.Spec.Replicas
			}), timeoutShort, intervalShort).Should(Equal(int32(1)))

			By("Scaling to 3 replicas only when broker appears")
			testAdminAPI.AddBroker(admin.Broker{NodeID: 0, MembershipStatus: admin.MembershipStatusActive})
			Eventually(resourceDataGetter(key, &sts, func() interface{} {
				return *sts.Spec.Replicas
			}), timeout, interval).Should(Equal(int32(3)))

			By("Deleting the cluster")
			Expect(k8sClient.Delete(context.Background(), redpandaCluster)).Should(Succeed())
		})
	})

	Context("When scaling up a cluster", func() {
		It("Should just update the StatefulSet", func() {
			By("Allowing creation of a new cluster with 2 replicas")
			key, redpandaCluster := getClusterWithReplicas("scaling", 2)
			Expect(k8sClient.Create(context.Background(), redpandaCluster)).Should(Succeed())

			By("Scaling to 2 replicas when at least one broker appears")
			testAdminAPI.AddBroker(admin.Broker{NodeID: 0, MembershipStatus: admin.MembershipStatusActive})
			var sts appsv1.StatefulSet
			Eventually(resourceDataGetter(key, &sts, func() interface{} {
				return *sts.Spec.Replicas
			}), timeout, interval).Should(Equal(int32(2)))
			Eventually(resourceDataGetter(key, redpandaCluster, func() interface{} {
				return redpandaCluster.Status.CurrentReplicas
			}), timeout, interval).Should(Equal(int32(2)), "CurrentReplicas should be 2, got %d", redpandaCluster.Status.CurrentReplicas)

			By("Scaling up when replicas increase")
			Eventually(clusterUpdater(key, func(cluster *v1alpha1.Cluster) {
				cluster.Spec.Replicas = pointer.Int32(5)
			}), timeout, interval).Should(Succeed())
			Eventually(resourceDataGetter(key, &sts, func() interface{} {
				return *sts.Spec.Replicas
			}), timeout, interval).Should(Equal(int32(5)))

			By("Deleting the cluster")
			Expect(k8sClient.Delete(context.Background(), redpandaCluster)).Should(Succeed())
		})
	})

	Context("When scaling down a cluster", func() {
		It("Should always decommission the last nodes of a cluster one at time", func() {
			By("Allowing creation of a new cluster with 3 replicas")
			key, redpandaCluster := getClusterWithReplicas("decommission", 3)
			Expect(k8sClient.Create(context.Background(), redpandaCluster)).Should(Succeed())

			By("Scaling to 3 replicas when the brokers start to appear")
			testAdminAPI.AddBroker(admin.Broker{NodeID: 0, MembershipStatus: admin.MembershipStatusActive})
			testAdminAPI.AddBroker(admin.Broker{NodeID: 1, MembershipStatus: admin.MembershipStatusActive})
			testAdminAPI.AddBroker(admin.Broker{NodeID: 2, MembershipStatus: admin.MembershipStatusActive})
			var sts appsv1.StatefulSet
			Eventually(resourceDataGetter(key, &sts, func() interface{} {
				return *sts.Spec.Replicas
			}), timeout, interval).Should(Equal(int32(3)))
			Eventually(resourceDataGetter(key, redpandaCluster, func() interface{} {
				return redpandaCluster.Status.CurrentReplicas
			}), timeout, interval).Should(Equal(int32(3)), "CurrentReplicas should be 3, got %d", redpandaCluster.Status.CurrentReplicas)

			By("Decommissioning the last node when scaling down by 2")
			Eventually(clusterUpdater(key, func(cluster *v1alpha1.Cluster) {
				cluster.Spec.Replicas = pointer.Int32(1)
			}), timeout, interval).Should(Succeed())

			By("Start decommissioning node with ordinal 2")
			Eventually(resourceDataGetter(key, redpandaCluster, func() interface{} {
				if redpandaCluster.Status.DecommissioningNode == nil {
					return nil
				}
				return *redpandaCluster.Status.DecommissioningNode
			}), timeout, interval).Should(Equal(int32(2)), "node 2 is not decommissioning:\n%s", func() string { y, _ := yaml.Marshal(redpandaCluster); return string(y) }())
			Eventually(testAdminAPI.BrokerStatusGetter(2), timeout, interval).Should(Equal(admin.MembershipStatusDraining))
			Consistently(testAdminAPI.BrokerStatusGetter(1), timeoutShort, intervalShort).Should(Equal(admin.MembershipStatusActive))
			Eventually(resourceDataGetter(key, &sts, func() interface{} {
				return *sts.Spec.Replicas
			}), timeout, interval).Should(Equal(int32(3)))

			By("Scaling down only when decommissioning is done")
			Expect(testAdminAPI.RemoveBroker(2)).To(BeTrue())
			Eventually(resourceDataGetter(key, &sts, func() interface{} {
				return *sts.Spec.Replicas
			}), timeout, interval).Should(Equal(int32(2)))
			Eventually(statefulSetReplicasReconciler(key, redpandaCluster), timeout, interval).Should(Succeed())

			By("Start decommissioning the other node")
			Eventually(testAdminAPI.BrokerStatusGetter(1), timeout, interval).Should(Equal(admin.MembershipStatusDraining))

			By("Removing the other node as well when done")
			Expect(testAdminAPI.RemoveBroker(1)).To(BeTrue())
			Eventually(resourceDataGetter(key, &sts, func() interface{} {
				return *sts.Spec.Replicas
			}), timeout, interval).Should(Equal(int32(1)))

			By("Deleting the cluster")
			Expect(k8sClient.Delete(context.Background(), redpandaCluster)).Should(Succeed())
		})

		It("Can recommission a node while decommission is in progress", func() {
			By("Allowing creation of a new cluster with 3 replicas")
			key, redpandaCluster := getClusterWithReplicas("recommission", 3)
			Expect(k8sClient.Create(context.Background(), redpandaCluster)).Should(Succeed())

			By("Scaling to 3 replicas when the brokers start to appear")
			testAdminAPI.AddBroker(admin.Broker{NodeID: 0, MembershipStatus: admin.MembershipStatusActive})
			testAdminAPI.AddBroker(admin.Broker{NodeID: 1, MembershipStatus: admin.MembershipStatusActive})
			testAdminAPI.AddBroker(admin.Broker{NodeID: 2, MembershipStatus: admin.MembershipStatusActive})
			var sts appsv1.StatefulSet
			Eventually(resourceDataGetter(key, &sts, func() interface{} {
				return *sts.Spec.Replicas
			}), timeout, interval).Should(Equal(int32(3)))
			Eventually(resourceDataGetter(key, redpandaCluster, func() interface{} {
				return redpandaCluster.Status.CurrentReplicas
			}), timeout, interval).Should(Equal(int32(3)), "CurrentReplicas should be 3, got %d", redpandaCluster.Status.CurrentReplicas)

			By("Start decommissioning node 2")
			Eventually(clusterUpdater(key, func(cluster *v1alpha1.Cluster) {
				cluster.Spec.Replicas = pointer.Int32(2)
			}), timeout, interval).Should(Succeed())
			Eventually(resourceDataGetter(key, redpandaCluster, func() interface{} {
				if redpandaCluster.Status.DecommissioningNode == nil {
					return nil
				}
				return *redpandaCluster.Status.DecommissioningNode
			}), timeout, interval).Should(Equal(int32(2)), "node 2 is not decommissioning:\n%s", func() string { y, _ := yaml.Marshal(redpandaCluster); return string(y) }())
			Eventually(testAdminAPI.BrokerStatusGetter(2), timeout, interval).Should(Equal(admin.MembershipStatusDraining))

			By("Recommissioning the node by restoring replicas")
			Eventually(clusterUpdater(key, func(cluster *v1alpha1.Cluster) {
				cluster.Spec.Replicas = pointer.Int32(3)
			}), timeout, interval).Should(Succeed())
			Eventually(testAdminAPI.BrokerStatusGetter(2), timeout, interval).Should(Equal(admin.MembershipStatusActive))

			By("Start decommissioning node 2 again")
			Eventually(clusterUpdater(key, func(cluster *v1alpha1.Cluster) {
				cluster.Spec.Replicas = pointer.Int32(2)
			}), timeout, interval).Should(Succeed())
			Eventually(resourceDataGetter(key, redpandaCluster, func() interface{} {
				if redpandaCluster.Status.DecommissioningNode == nil {
					return nil
				}
				return *redpandaCluster.Status.DecommissioningNode
			}), timeout, interval).Should(Equal(int32(2)))
			Eventually(testAdminAPI.BrokerStatusGetter(2), timeout, interval).Should(Equal(admin.MembershipStatusDraining))

			By("Recommissioning the node also when scaling to more replicas")
			Eventually(clusterUpdater(key, func(cluster *v1alpha1.Cluster) {
				cluster.Spec.Replicas = pointer.Int32(4)
			}), timeout, interval).Should(Succeed())
			Eventually(testAdminAPI.BrokerStatusGetter(2), timeout, interval).Should(Equal(admin.MembershipStatusActive))
			Eventually(resourceDataGetter(key, &sts, func() interface{} {
				return *sts.Spec.Replicas
			}), timeout, interval).Should(Equal(int32(4)))

			By("Deleting the cluster")
			Expect(k8sClient.Delete(context.Background(), redpandaCluster)).Should(Succeed())
		})

		It("Can decommission nodes that never started", func() {
			By("Allowing creation of a new cluster with 3 replicas")
			key, redpandaCluster := getClusterWithReplicas("direct-decommission", 3)
			Expect(k8sClient.Create(context.Background(), redpandaCluster)).Should(Succeed())

			By("Scaling to 3 replicas, but have last one not registered")
			testAdminAPI.AddBroker(admin.Broker{NodeID: 0, MembershipStatus: admin.MembershipStatusActive})
			testAdminAPI.AddBroker(admin.Broker{NodeID: 1, MembershipStatus: admin.MembershipStatusActive})
			var sts appsv1.StatefulSet
			Eventually(resourceDataGetter(key, &sts, func() interface{} {
				return *sts.Spec.Replicas
			}), timeout, interval).Should(Equal(int32(3)))
			Eventually(resourceDataGetter(key, redpandaCluster, func() interface{} {
				return redpandaCluster.Status.CurrentReplicas
			}), timeout, interval).Should(Equal(int32(3)), "CurrentReplicas should be 3, got %d", redpandaCluster.Status.CurrentReplicas)

			By("Doing direct scale down of node 2")
			Eventually(clusterUpdater(key, func(cluster *v1alpha1.Cluster) {
				cluster.Spec.Replicas = pointer.Int32(2)
			}), timeout, interval).Should(Succeed())
			Eventually(resourceDataGetter(key, &sts, func() interface{} {
				return *sts.Spec.Replicas
			}), timeout, interval).Should(Equal(int32(2)))
			Eventually(statefulSetReplicasReconciler(key, redpandaCluster), timeout, interval).Should(Succeed())
			Eventually(resourceDataGetter(key, redpandaCluster, func() interface{} {
				return redpandaCluster.Status.DecommissioningNode
			}), timeout, interval).Should(BeNil())

			By("Deleting the cluster")
			Expect(k8sClient.Delete(context.Background(), redpandaCluster)).Should(Succeed())
		})

		It("Can decommission nodes that were lazy to start", func() {
			By("Allowing creation of a new cluster with 3 replicas")
			key, redpandaCluster := getClusterWithReplicas("direct-decommission-lazy", 3)
			Expect(k8sClient.Create(context.Background(), redpandaCluster)).Should(Succeed())

			By("Scaling to 3 replicas, but have last one not registered")
			testAdminAPI.AddBroker(admin.Broker{NodeID: 0, MembershipStatus: admin.MembershipStatusActive})
			testAdminAPI.AddBroker(admin.Broker{NodeID: 1, MembershipStatus: admin.MembershipStatusActive})
			var sts appsv1.StatefulSet
			Eventually(resourceDataGetter(key, &sts, func() interface{} {
				return *sts.Spec.Replicas
			}), timeout, interval).Should(Equal(int32(3)))
			Eventually(resourceDataGetter(key, redpandaCluster, func() interface{} {
				return redpandaCluster.Status.CurrentReplicas
			}), timeout, interval).Should(Equal(int32(3)), "CurrentReplicas should be 3, got %d", redpandaCluster.Status.CurrentReplicas)

			By("Doing direct scale down of node 2")
			Eventually(clusterUpdater(key, func(cluster *v1alpha1.Cluster) {
				cluster.Spec.Replicas = pointer.Int32(2)
			}), timeout, interval).Should(Succeed())
			Eventually(resourceDataGetter(key, &sts, func() interface{} {
				return *sts.Spec.Replicas
			}), timeout, interval).Should(Equal(int32(2)))

			By("Finding out that the node was able to connect to the cluster before being terminated")
			testAdminAPI.AddBroker(admin.Broker{NodeID: 2, MembershipStatus: admin.MembershipStatusActive})
			Eventually(resourceDataGetter(key, redpandaCluster, func() interface{} {
				if redpandaCluster.Status.DecommissioningNode == nil {
					return nil
				}
				return *redpandaCluster.Status.DecommissioningNode
			}), timeout, interval).Should(Equal(int32(2)))

			By("Scaling the cluster back and properly decommission the node")
			Eventually(resourceDataGetter(key, &sts, func() interface{} {
				return *sts.Spec.Replicas
			}), timeout, interval).Should(Equal(int32(3)))
			Eventually(testAdminAPI.BrokerStatusGetter(2), timeout, interval).Should(Equal(admin.MembershipStatusDraining))

			By("Complete downscale after decommission")
			Expect(testAdminAPI.RemoveBroker(2)).To(BeTrue())
			Eventually(resourceDataGetter(key, &sts, func() interface{} {
				return *sts.Spec.Replicas
			}), timeout, interval).Should(Equal(int32(2)))
			Eventually(statefulSetReplicasReconciler(key, redpandaCluster), timeout, interval).Should(Succeed())
			Eventually(resourceDataGetter(key, redpandaCluster, func() interface{} {
				return redpandaCluster.Status.DecommissioningNode
			}), timeout, interval).Should(BeNil())

			By("Deleting the cluster")
			Expect(k8sClient.Delete(context.Background(), redpandaCluster)).Should(Succeed())
		})
	})
})

func getClusterWithReplicas(
	name string, replicas int32,
) (key types.NamespacedName, cluster *v1alpha1.Cluster) {
	key = types.NamespacedName{
		Name:      name,
		Namespace: "default",
	}

	cluster = &v1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      key.Name,
			Namespace: key.Namespace,
		},
		Spec: v1alpha1.ClusterSpec{
			Image:    "vectorized/redpanda",
			Version:  "dev",
			Replicas: pointer.Int32(replicas),
			Configuration: v1alpha1.RedpandaConfig{
				KafkaAPI: []v1alpha1.KafkaAPI{
					{
						Port: 9092,
					},
				},
				AdminAPI: []v1alpha1.AdminAPI{{Port: 9644}},
				RPCServer: v1alpha1.SocketAddress{
					Port: 33145,
				},
			},
			Resources: v1alpha1.RedpandaResourceRequirements{
				ResourceRequirements: corev1.ResourceRequirements{
					Limits: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("1"),
						corev1.ResourceMemory: resource.MustParse("2Gi"),
					},
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("1"),
						corev1.ResourceMemory: resource.MustParse("2Gi"),
					},
				},
				Redpanda: nil,
			},
		},
	}
	return key, cluster
}
