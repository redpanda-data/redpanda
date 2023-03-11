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
	"time"

	"github.com/fluxcd/helm-controller/api/v2beta1"
	"github.com/fluxcd/source-controller/api/v1beta2"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
)

// This is a unit test
var _ = Describe("Redpanda Controller", func() {
	const (
		ClusterName        = "redpanda-test-cluster"
		RedpandaName       = "redpanda"
		HelmRepositoryName = "redpanda-repository"

		timeout  = time.Second * 30
		interval = time.Millisecond * 100
	)
	var (
		RedpandaObj       *v1alpha1.Redpanda
		RedpandaNamespace string
		key               types.NamespacedName
		testCluster       *v1alpha1.Cluster
		namespace         *corev1.Namespace
	)

	BeforeEach(func() {
		ctx := context.Background()
		if testCluster == nil {
			key, _, testCluster, namespace = getInitialTestCluster(ClusterName)
			RedpandaNamespace = key.Namespace
		}
		// check if test cluster exists, create it if not
		if err := k8sClient.Get(ctx, key, &v1alpha1.Cluster{}); err != nil {
			if !apierrors.IsNotFound(err) {
				Expect(err).To(Equal(nil))
			}
			Expect(k8sClient.Create(ctx, namespace)).Should(Succeed())
			Expect(k8sClient.Create(ctx, testCluster)).Should(Succeed())
			Eventually(clusterConfiguredConditionStatusGetter(key), timeout, interval).Should(BeTrue())
		}
		// check if redpanda cluster exists, create it if not
		RedpandaObj = &v1alpha1.Redpanda{}
		key := client.ObjectKey{Namespace: RedpandaNamespace, Name: RedpandaName}
		if err := k8sClient.Get(ctx, key, RedpandaObj); err != nil {
			if !apierrors.IsNotFound(err) {
				Expect(err).To(Equal(nil))
			}
			RedpandaObj = &v1alpha1.Redpanda{
				TypeMeta: metav1.TypeMeta{
					APIVersion: "redpanda.vectorized.io/v1alpha1",
					Kind:       "Redpanda",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:      RedpandaName,
					Namespace: RedpandaNamespace,
				},
				Spec: v1alpha1.RedpandaSpec{
					ChartRef: v1alpha1.ChartRef{
						ChartVersion: "3.x.x",
					},
					HelmRepositoryName: HelmRepositoryName,
					ClusterSpec:        &v1alpha1.RedpandaClusterSpec{},
				},
			}

			Expect(k8sClient.Create(ctx, RedpandaObj)).Should(Succeed())
			Eventually(func() bool { return k8sClient.Get(ctx, key, RedpandaObj) == nil }, timeout, interval).Should(BeTrue())
		}
	})

	Context("When creating a Redpanda", func() {
		ctx := context.Background()

		It("Should create a HelmRepository", func() {
			key := client.ObjectKey{Namespace: RedpandaNamespace, Name: HelmRepositoryName}
			Eventually(func() bool { return k8sClient.Get(ctx, key, &v1beta2.HelmRepository{}) == nil }, timeout, interval).Should(BeTrue())
		})

		It("Should create a HelmRelease", func() {
			key := client.ObjectKey{Namespace: RedpandaNamespace, Name: RedpandaName}
			Eventually(func() bool { return k8sClient.Get(ctx, key, &v2beta1.HelmRelease{}) == nil }, timeout, interval).Should(BeTrue())
		})

		It("Should create a HelmChart", func() {
			key := client.ObjectKey{Namespace: RedpandaNamespace, Name: fmt.Sprintf("%s-%s", RedpandaNamespace, RedpandaName)}
			Eventually(func() bool { return k8sClient.Get(ctx, key, &v1beta2.HelmChart{}) == nil }, timeout, interval).Should(BeTrue())
		})

	})
})
