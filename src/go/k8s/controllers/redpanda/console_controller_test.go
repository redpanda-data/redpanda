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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	redpandav1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources"
)

var _ = Describe("Console controller", func() {
	const (
		ClusterName      = "test-cluster"
		ClusterNamespace = "default"

		ConsoleName      = "test-console"
		ConsoleNamespace = "default"

		timeout  = time.Second * 30
		interval = time.Millisecond * 100
	)

	Context("When creating Console", func() {
		ctx := context.Background()
		It("Should create User Secret", func() {
			By("By creating a Cluster")
			key, _, redpandaCluster := getInitialTestCluster(ClusterName)
			Expect(k8sClient.Create(ctx, redpandaCluster)).Should(Succeed())
			Eventually(clusterConfiguredConditionStatusGetter(key), timeout, interval).Should(BeTrue())

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
					ClusterKeyRef:  corev1.ObjectReference{Namespace: ClusterNamespace, Name: ClusterName},
					SchemaRegistry: redpandav1alpha1.Schema{Enabled: true},
					Deployment:     redpandav1alpha1.Deployment{Image: "vectorized/console:master-173596f"},
					Connect:        redpandav1alpha1.Connect{Enabled: false},
				},
			}
			Expect(k8sClient.Create(ctx, console)).Should(Succeed())

			consoleLookupKey := types.NamespacedName{Name: fmt.Sprintf("%s-%s", ConsoleName, resources.ConsoleSuffix), Namespace: ConsoleNamespace}
			createdSecret := &corev1.Secret{}
			Eventually(func() bool {
				if err := k8sClient.Get(ctx, consoleLookupKey, createdSecret); err != nil {
					return false
				}
				return true
			}, timeout, interval).Should(BeTrue())
		})
	})
})
