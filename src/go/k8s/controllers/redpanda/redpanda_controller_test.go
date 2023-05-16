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
	"strings"
	"time"

	"github.com/fluxcd/helm-controller/api/v2beta1"
	"github.com/fluxcd/source-controller/api/v1beta2"
	"github.com/moby/moby/pkg/namesgenerator"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
)

var _ = Describe("Redpanda Controller", func() {
	const (
		RedpandaClusterName = "redpanda-test"
		HelmRepositoryName  = "redpanda-repository"

		timeout  = time.Second * 30
		interval = time.Millisecond * 100
	)

	Context("When creating a Redpanda with no values file changes", func() {
		ctx := context.Background()

		var RedpandaNamespace string

		It("Should create a Redpanda cluster", func() {
			key, namespace := getRandomizedNamespacedName(RedpandaClusterName)
			RedpandaNamespace = key.Namespace

			// create the namespace object
			Expect(k8sClient.Create(ctx, namespace)).Should(Succeed())

			// check if redpanda cluster exists, create it if not
			RedpandaObj := &v1alpha1.Redpanda{}
			if err := k8sClient.Get(ctx, key, RedpandaObj); err != nil {
				if !apierrors.IsNotFound(err) {
					Expect(err).To(Equal(nil))
				}
				RedpandaObj = &v1alpha1.Redpanda{
					TypeMeta: metav1.TypeMeta{
						APIVersion: "cluster.redpanda.com/v1alpha1",
						Kind:       "Redpanda",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:      RedpandaClusterName,
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

		It("Should create a HelmRepository", func() {
			key := client.ObjectKey{Namespace: RedpandaNamespace, Name: HelmRepositoryName}
			Eventually(func() bool { return k8sClient.Get(ctx, key, &v1beta2.HelmRepository{}) == nil }, timeout, interval).Should(BeTrue())
		})

		It("Should create a HelmRelease", func() {
			key := client.ObjectKey{Namespace: RedpandaNamespace, Name: RedpandaClusterName}
			Eventually(func() bool { return k8sClient.Get(ctx, key, &v2beta1.HelmRelease{}) == nil }, timeout, interval).Should(BeTrue())
		})

		It("Should create a HelmChart", func() {
			key := client.ObjectKey{Namespace: RedpandaNamespace, Name: fmt.Sprintf("%s-%s", RedpandaNamespace, RedpandaClusterName)}
			Eventually(func() bool { return k8sClient.Get(ctx, key, &v1beta2.HelmChart{}) == nil }, timeout, interval).Should(BeTrue())
		})
	})
})

func getRandomizedNamespacedName(name string) (types.NamespacedName, *corev1.Namespace) {
	ns := strings.Replace(namesgenerator.GetRandomName(0), "_", "-", 1)
	key := types.NamespacedName{
		Name:      name,
		Namespace: ns,
	}
	namespace := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: ns,
		},
	}
	return key, namespace
}
