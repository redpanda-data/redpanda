// Copyright 2021 Redpanda Data, Inc.
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
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	redpandav1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	consolepkg "github.com/redpanda-data/redpanda/src/go/k8s/pkg/console"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/labels"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources"
	"github.com/twmb/franz-go/pkg/kadm"
	"gopkg.in/yaml.v3"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type mockKafkaAdmin struct{}

func (m *mockKafkaAdmin) CreateACLs(
	context.Context, *kadm.ACLBuilder,
) (kadm.CreateACLsResults, error) {
	return nil, nil
}

func (m *mockKafkaAdmin) DeleteACLs(
	context.Context, *kadm.ACLBuilder,
) (kadm.DeleteACLsResults, error) {
	return nil, nil
}

var _ = Describe("Console controller", func() {
	const (
		ClusterName = "test-cluster"

		ConsoleName      = "test-console"
		ConsoleNamespace = "default"

		timeout  = time.Second * 30
		interval = time.Millisecond * 100
	)

	Context("When creating Console", func() {
		ctx := context.Background()
		It("Should expose Console web app", func() {
			By("By creating a Cluster")
			key, _, redpandaCluster := getInitialTestCluster(ClusterName)
			Expect(k8sClient.Create(ctx, redpandaCluster)).Should(Succeed())
			Eventually(clusterConfiguredConditionStatusGetter(key), timeout, interval).Should(BeTrue())

			var (
				deploymentImage      = "vectorized/console:latest"
				enableSchemaRegistry = true
				enableConnect        = false
			)

			By("By creating a Console")
			console := &redpandav1alpha1.Console{
				TypeMeta: metav1.TypeMeta{
					APIVersion: "redpanda.vectorized.io/v1alpha1",
					Kind:       "Console",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:      ConsoleName,
					Namespace: ConsoleNamespace,
				},
				Spec: redpandav1alpha1.ConsoleSpec{
					ClusterKeyRef:  corev1.ObjectReference{Namespace: key.Namespace, Name: key.Name},
					SchemaRegistry: redpandav1alpha1.Schema{Enabled: enableSchemaRegistry},
					Deployment:     redpandav1alpha1.Deployment{Image: deploymentImage},
					Connect:        redpandav1alpha1.Connect{Enabled: enableConnect},
				},
			}
			Expect(k8sClient.Create(ctx, console)).Should(Succeed())

			By("By having a Secret for SASL user")
			secretLookupKey := types.NamespacedName{Name: fmt.Sprintf("%s-%s", ConsoleName, resources.ConsoleSuffix), Namespace: ConsoleNamespace}
			createdSecret := &corev1.Secret{}
			Eventually(func() bool {
				if err := k8sClient.Get(ctx, secretLookupKey, createdSecret); err != nil {
					return false
				}
				return true
			}, timeout, interval).Should(BeTrue())

			// Not checking if ACLs are created, KafkaAdmin is mocked

			By("By having a valid ConfigMap")
			createdConfigMaps := &corev1.ConfigMapList{}
			Eventually(func() bool {
				if err := k8sClient.List(ctx, createdConfigMaps, client.MatchingLabels(labels.ForConsole(console)), client.InNamespace(ConsoleNamespace)); err != nil {
					return false
				}
				if len(createdConfigMaps.Items) != 1 {
					return false
				}
				for _, cm := range createdConfigMaps.Items {
					cc := &consolepkg.ConsoleConfig{}
					if err := yaml.Unmarshal([]byte(cm.Data["config.yaml"]), cc); err != nil {
						return false
					}
					if cc.Kafka.Schema.Enabled != enableSchemaRegistry || cc.Connect.Enabled != enableConnect {
						return false
					}
				}
				return true
			}, timeout, interval).Should(BeTrue())

			By("By having a running Deployment")
			deploymentLookupKey := types.NamespacedName{Name: ConsoleName, Namespace: ConsoleNamespace}
			createdDeployment := &appsv1.Deployment{}
			Eventually(func() bool {
				if err := k8sClient.Get(ctx, deploymentLookupKey, createdDeployment); err != nil {
					return false
				}
				for _, c := range createdDeployment.Spec.Template.Spec.Containers {
					if c.Name == consolepkg.ConsoleContainerName && c.Image != deploymentImage {
						return false
					}
				}
				for _, c := range createdDeployment.Status.Conditions {
					if c.Type == appsv1.DeploymentAvailable && c.Status != corev1.ConditionTrue {
						return false
					}
				}
				return true
			}, timeout, interval).Should(BeTrue())

			By("By having a Service")
			serviceLookupKey := types.NamespacedName{Name: ConsoleName, Namespace: ConsoleNamespace}
			createdService := &corev1.Service{}
			Eventually(func() bool {
				if err := k8sClient.Get(ctx, serviceLookupKey, createdService); err != nil {
					return false
				}
				for _, port := range createdService.Spec.Ports {
					if port.Name == consolepkg.ServicePortName && port.Port != int32(console.Spec.Server.HTTPListenPort) {
						return false
					}
				}
				return true
			}, timeout, interval).Should(BeTrue())

			// TODO: Not yet discussed if gonna use Ingress, check when finalized

			By("By having the Console URLs in status")
			consoleLookupKey := types.NamespacedName{Name: ConsoleName, Namespace: ConsoleNamespace}
			createdConsole := &redpandav1alpha1.Console{}
			Eventually(func() bool {
				if err := k8sClient.Get(ctx, consoleLookupKey, createdConsole); err != nil {
					return false
				}
				internal := fmt.Sprintf("%s.%s.svc.cluster.local:%d", ConsoleName, ConsoleNamespace, console.Spec.Server.HTTPListenPort)
				// TODO: Not yet discussed how to expose externally, check when finalized
				external := ""
				if conn := createdConsole.Status.Connectivity; conn == nil || conn.Internal != internal || conn.External != external {
					return false
				}
				return true
			}, timeout, interval).Should(BeTrue())
		})
	})
})
