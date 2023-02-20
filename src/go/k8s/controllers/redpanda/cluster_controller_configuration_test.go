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
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/moby/moby/pkg/namesgenerator"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/pointer"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	"github.com/redpanda-data/redpanda/src/go/k8s/internal/testutils"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources/configuration"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/api/admin"
)

const (
	versionWithCentralizedConfiguration    = "v22.1.1-dev"
	versionWithoutCentralizedConfiguration = "v21.11.1-dev" // no centralized config, shadow index enabled
)

var _ = Describe("RedPandaCluster configuration controller", func() {
	const (
		timeout  = time.Second * 30
		interval = time.Millisecond * 100

		timeoutShort  = time.Millisecond * 100
		intervalShort = time.Millisecond * 20

		bootstrapConfigurationFile = ".bootstrap.yaml"

		configMapHashKey                = "redpanda.vectorized.io/configmap-hash"
		centralizedConfigurationHashKey = "redpanda.vectorized.io/centralized-configuration-hash"
		lastAppliedConfiguraitonHashKey = "redpanda.vectorized.io/last-applied-configuration"
	)

	gracePeriod := int64(0)
	deleteOptions := &client.DeleteOptions{
		GracePeriodSeconds: &gracePeriod,
	}

	Context("When managing a RedpandaCluster with centralized config", func() {
		It("Can initialize a cluster with centralized configuration", func() {
			By("Allowing creation of a new cluster")
			key, baseKey, redpandaCluster, namespace := getInitialTestCluster("central-initialize")
			Expect(k8sClient.Create(context.Background(), namespace)).Should(Succeed())
			Expect(k8sClient.Create(context.Background(), redpandaCluster)).Should(Succeed())

			By("Creating a Configmap with the bootstrap configuration")
			var cm corev1.ConfigMap
			Eventually(resourceGetter(baseKey, &cm), timeout, interval).Should(Succeed())

			By("Putting a .bootstrap.yaml in the configmap")
			Expect(cm.Data[bootstrapConfigurationFile]).ToNot(BeEmpty())

			By("Always adding the last-applied-configuration annotation on the configmap")
			Eventually(annotationGetter(baseKey, &cm, lastAppliedConfiguraitonHashKey), timeout, interval).ShouldNot(BeEmpty())

			By("Creating the statefulset")
			var sts appsv1.StatefulSet
			Eventually(resourceGetter(key, &appsv1.StatefulSet{})).Should(Succeed())

			By("Setting the configmap-hash annotation on the statefulset")
			Eventually(annotationGetter(key, &sts, configMapHashKey), timeout, interval).ShouldNot(BeEmpty())

			By("Not patching the admin API for any reason")
			Consistently(testAdminAPI.NumPatchesGetter(), timeoutShort, intervalShort).Should(Equal(0))

			By("Synchronizing the condition")
			Eventually(clusterConfiguredConditionStatusGetter(key), timeout, interval).Should(BeTrue())

			By("Not using the centralized-config annotation at this stage on the statefulset")
			Consistently(annotationGetter(key, &sts, centralizedConfigurationHashKey), timeoutShort, intervalShort).Should(BeEmpty())

			By("Deleting the cluster")
			Expect(k8sClient.Delete(context.Background(), redpandaCluster, deleteOptions)).Should(Succeed())
			testutils.DeleteAllInNamespace(testEnv, k8sClient, namespace)
		})

		It("Should interact with the admin API when doing changes", func() {
			By("Allowing creation of a new cluster")
			key, baseKey, redpandaCluster, namespace := getInitialTestCluster("central-changes")
			Expect(k8sClient.Create(context.Background(), namespace)).Should(Succeed())
			Expect(k8sClient.Create(context.Background(), redpandaCluster)).Should(Succeed())

			By("Creating the Configmap and the statefulset")
			Eventually(resourceGetter(baseKey, &corev1.ConfigMap{}), timeout, interval).Should(Succeed())
			Eventually(resourceGetter(key, &appsv1.StatefulSet{}), timeout, interval).Should(Succeed())

			By("Setting a configmap-hash in the statefulset")
			var sts appsv1.StatefulSet
			Eventually(annotationGetter(key, &sts, configMapHashKey), timeout, interval).ShouldNot(BeEmpty())
			configMapHash := sts.Spec.Template.Annotations[configMapHashKey]
			Expect(configMapHash).NotTo(BeEmpty())

			By("Synchronizing the configuration")
			Eventually(clusterConfiguredConditionStatusGetter(key), timeout, interval).Should(BeTrue())

			By("Accepting a change")
			testAdminAPI.RegisterPropertySchema("non-restarting", admin.ConfigPropertyMetadata{NeedsRestart: false})
			var cluster v1alpha1.Cluster
			Expect(k8sClient.Get(context.Background(), key, &cluster)).To(Succeed())
			if cluster.Spec.AdditionalConfiguration == nil {
				cluster.Spec.AdditionalConfiguration = make(map[string]string)
			}
			cluster.Spec.AdditionalConfiguration["redpanda.non-restarting"] = "the-val"
			Expect(k8sClient.Update(context.Background(), &cluster)).To(Succeed())

			By("Sending the new property to the admin API")
			Eventually(testAdminAPI.PropertyGetter("non-restarting"), timeout, interval).Should(Equal("the-val"))
			Consistently(testAdminAPI.PatchesGetter(), timeoutShort, intervalShort).Should(HaveLen(1))

			By("Synchronizing the condition")
			Eventually(clusterConfiguredConditionStatusGetter(key), timeout, interval).Should(BeTrue())

			By("Not changing the configmap-hash in the statefulset")
			Consistently(annotationGetter(key, &appsv1.StatefulSet{}, configMapHashKey), timeoutShort, intervalShort).Should(Equal(configMapHash))

			By("Not setting the centralized configuration hash in the statefulset")
			Consistently(annotationGetter(key, &appsv1.StatefulSet{}, centralizedConfigurationHashKey), timeoutShort, intervalShort).Should(BeEmpty())

			By("Marking the last applied configuration in the configmap")
			baseConfig, err := testAdminAPI.Config(context.Background(), true)

			Expect(err).To(BeNil())
			expectedAnnotation, err := json.Marshal(baseConfig)
			Expect(err).To(BeNil())
			Eventually(annotationGetter(baseKey, &corev1.ConfigMap{}, lastAppliedConfiguraitonHashKey), timeout, interval).Should(Equal(string(expectedAnnotation)))
			Consistently(annotationGetter(baseKey, &corev1.ConfigMap{}, lastAppliedConfiguraitonHashKey), timeoutShort, intervalShort).Should(Equal(string(expectedAnnotation)))

			By("Never restarting the cluster")
			Consistently(annotationGetter(key, &appsv1.StatefulSet{}, configMapHashKey), timeoutShort, intervalShort).Should(Equal(configMapHash))
			Consistently(annotationGetter(key, &appsv1.StatefulSet{}, centralizedConfigurationHashKey), timeoutShort, intervalShort).Should(BeEmpty())

			By("Deleting the cluster")
			Expect(k8sClient.Delete(context.Background(), redpandaCluster, deleteOptions)).Should(Succeed())
			testutils.DeleteAllInNamespace(testEnv, k8sClient, namespace)
		})

		It("Should remove properties from the admin API when needed", func() {
			By("Allowing creation of a new cluster")
			key, baseKey, redpandaCluster, namespace := getInitialTestCluster("central-removal")
			Expect(k8sClient.Create(context.Background(), namespace)).Should(Succeed())
			Expect(k8sClient.Create(context.Background(), redpandaCluster)).Should(Succeed())

			By("Creating the Configmap and the statefulset")
			Eventually(resourceGetter(baseKey, &corev1.ConfigMap{}), timeout, interval).Should(Succeed())
			var sts appsv1.StatefulSet
			Eventually(resourceGetter(key, &sts), timeout, interval).Should(Succeed())
			hash := sts.Spec.Template.Annotations[configMapHashKey]
			Expect(hash).NotTo(BeEmpty())

			By("Synchronizing the configuration")
			Eventually(clusterConfiguredConditionStatusGetter(key), timeout, interval).Should(BeTrue())

			By("Accepting an initial change")
			testAdminAPI.RegisterPropertySchema("p0", admin.ConfigPropertyMetadata{NeedsRestart: false})
			var cluster v1alpha1.Cluster
			Expect(k8sClient.Get(context.Background(), key, &cluster)).To(Succeed())
			if cluster.Spec.AdditionalConfiguration == nil {
				cluster.Spec.AdditionalConfiguration = make(map[string]string)
			}
			cluster.Spec.AdditionalConfiguration["redpanda.p0"] = "v0"
			Expect(k8sClient.Update(context.Background(), &cluster)).To(Succeed())

			By("Synchronizing the field with the admin API")
			Eventually(testAdminAPI.PropertyGetter("p0"), timeout, interval).Should(Equal("v0"))

			By("Synchronizing the condition")
			Eventually(clusterConfiguredConditionStatusGetter(key), timeout, interval).Should(BeTrue())

			By("Accepting two new properties")
			testAdminAPI.RegisterPropertySchema("p1", admin.ConfigPropertyMetadata{NeedsRestart: false})
			testAdminAPI.RegisterPropertySchema("p2", admin.ConfigPropertyMetadata{NeedsRestart: false})
			Expect(k8sClient.Get(context.Background(), key, &cluster)).To(Succeed())
			if cluster.Spec.AdditionalConfiguration == nil {
				cluster.Spec.AdditionalConfiguration = make(map[string]string)
			}
			cluster.Spec.AdditionalConfiguration["redpanda.p1"] = "v1"
			cluster.Spec.AdditionalConfiguration["redpanda.p2"] = "v2"
			Expect(k8sClient.Update(context.Background(), &cluster)).To(Succeed())

			By("Adding two fields to the config at once")
			Eventually(testAdminAPI.PropertyGetter("p1")).Should(Equal("v1"))
			Eventually(testAdminAPI.PropertyGetter("p2")).Should(Equal("v2"))
			patches := testAdminAPI.PatchesGetter()()
			Expect(patches).NotTo(BeEmpty())
			Expect(patches[len(patches)-1]).To(Equal(configuration.CentralConfigurationPatch{
				Upsert: map[string]interface{}{
					"p1": "v1",
					"p2": "v2",
				},
				Remove: []string{},
			}))

			By("Synchronizing the condition")
			Eventually(clusterConfiguredConditionStatusGetter(key), timeout, interval).Should(BeTrue())

			By("Accepting a deletion and a change")
			Expect(k8sClient.Get(context.Background(), key, &cluster)).To(Succeed())
			if cluster.Spec.AdditionalConfiguration == nil {
				cluster.Spec.AdditionalConfiguration = make(map[string]string)
			}
			cluster.Spec.AdditionalConfiguration["redpanda.p1"] = "v1x"
			delete(cluster.Spec.AdditionalConfiguration, "redpanda.p2")
			Expect(k8sClient.Update(context.Background(), &cluster)).To(Succeed())

			By("Producing the right patch")
			Eventually(testAdminAPI.PropertyGetter("p1")).Should(Equal("v1x"))
			patches = testAdminAPI.PatchesGetter()()
			Expect(patches).NotTo(BeEmpty())
			Expect(patches[len(patches)-1]).To(Equal(configuration.CentralConfigurationPatch{
				Upsert: map[string]interface{}{
					"p1": "v1x",
				},
				Remove: []string{"p2"},
			}))

			By("Synchronizing the condition")
			Eventually(clusterConfiguredConditionStatusGetter(key), timeout, interval).Should(BeTrue())

			By("Never restarting the cluster")
			Consistently(annotationGetter(key, &appsv1.StatefulSet{}, configMapHashKey), timeoutShort, intervalShort).Should(Equal(hash))
			Consistently(annotationGetter(key, &appsv1.StatefulSet{}, centralizedConfigurationHashKey), timeoutShort, intervalShort).Should(BeEmpty())

			By("Deleting the cluster")
			Expect(k8sClient.Delete(context.Background(), redpandaCluster, deleteOptions)).Should(Succeed())
			testutils.DeleteAllInNamespace(testEnv, k8sClient, namespace)
		})

		It("Should restart the cluster only when strictly required", func() {
			By("Allowing creation of a new cluster")
			key, baseKey, redpandaCluster, namespace := getInitialTestCluster("central-restart")
			Expect(k8sClient.Create(context.Background(), namespace)).Should(Succeed())
			Expect(k8sClient.Create(context.Background(), redpandaCluster)).Should(Succeed())

			By("Creating the Configmap and the statefulset")
			Eventually(resourceGetter(baseKey, &corev1.ConfigMap{}), timeout, interval).Should(Succeed())
			Eventually(resourceGetter(key, &appsv1.StatefulSet{}), timeout, interval).Should(Succeed())

			By("Synchronizing the condition")
			Eventually(clusterConfiguredConditionStatusGetter(key), timeout, interval).Should(BeTrue())

			By("Starting with an empty hash")
			var sts appsv1.StatefulSet
			Expect(annotationGetter(key, &sts, centralizedConfigurationHashKey)()).To(BeEmpty())
			initialConfigMapHash := sts.Spec.Template.Annotations[configMapHashKey]
			Expect(initialConfigMapHash).NotTo(BeEmpty())

			By("Accepting a change that would require restart")
			testAdminAPI.RegisterPropertySchema("prop-restart", admin.ConfigPropertyMetadata{NeedsRestart: true})
			var cluster v1alpha1.Cluster
			Expect(k8sClient.Get(context.Background(), key, &cluster)).To(Succeed())
			if cluster.Spec.AdditionalConfiguration == nil {
				cluster.Spec.AdditionalConfiguration = make(map[string]string)
			}
			const propValue = "the-value"
			cluster.Spec.AdditionalConfiguration["redpanda.prop-restart"] = propValue
			Expect(k8sClient.Update(context.Background(), &cluster)).To(Succeed())

			By("Synchronizing the field with the admin API")
			Eventually(testAdminAPI.PropertyGetter("prop-restart"), timeout, interval).Should(Equal(propValue))

			By("Synchronizing the condition")
			Eventually(clusterConfiguredConditionStatusGetter(key), timeout, interval).Should(BeTrue())

			By("Changing the statefulset hash and keeping it stable")
			Eventually(annotationGetter(key, &appsv1.StatefulSet{}, centralizedConfigurationHashKey), timeout, interval).ShouldNot(BeEmpty())
			initialCentralizedConfigHash := annotationGetter(key, &appsv1.StatefulSet{}, centralizedConfigurationHashKey)()
			Consistently(annotationGetter(key, &appsv1.StatefulSet{}, centralizedConfigurationHashKey), timeoutShort, intervalShort).Should(Equal(initialCentralizedConfigHash))
			Expect(annotationGetter(key, &appsv1.StatefulSet{}, configMapHashKey)()).To(Equal(initialConfigMapHash))

			By("Accepting another change that would not require restart")
			testAdminAPI.RegisterPropertySchema("prop-no-restart", admin.ConfigPropertyMetadata{NeedsRestart: false})
			Expect(k8sClient.Get(context.Background(), key, &cluster)).To(Succeed())
			const propValue2 = "the-value2"
			cluster.Spec.AdditionalConfiguration["redpanda.prop-no-restart"] = propValue2
			Expect(k8sClient.Update(context.Background(), &cluster)).To(Succeed())

			By("Synchronizing the new field with the admin API")
			Eventually(testAdminAPI.PropertyGetter("prop-no-restart"), timeout, interval).Should(Equal(propValue2))

			By("Synchronizing the condition")
			Eventually(clusterConfiguredConditionStatusGetter(key), timeout, interval).Should(BeTrue())

			By("Not changing the hash")
			Expect(annotationGetter(key, &appsv1.StatefulSet{}, centralizedConfigurationHashKey)()).To(Equal(initialCentralizedConfigHash))
			Consistently(annotationGetter(key, &appsv1.StatefulSet{}, centralizedConfigurationHashKey), timeoutShort, intervalShort).Should(Equal(initialCentralizedConfigHash))
			Expect(annotationGetter(key, &appsv1.StatefulSet{}, configMapHashKey)()).To(Equal(initialConfigMapHash))

			numberOfPatches := len(testAdminAPI.PatchesGetter()())

			By("Accepting a change in a node property to trigger restart")
			Expect(k8sClient.Get(context.Background(), key, &cluster)).To(Succeed())
			cluster.Spec.Configuration.DeveloperMode = !cluster.Spec.Configuration.DeveloperMode
			Expect(k8sClient.Update(context.Background(), &cluster)).To(Succeed())

			By("Changing the hash because of the node property change")
			Eventually(annotationGetter(key, &appsv1.StatefulSet{}, configMapHashKey), timeout, interval).ShouldNot(Equal(initialConfigMapHash))
			configMapHash2 := annotationGetter(key, &appsv1.StatefulSet{}, configMapHashKey)()
			Consistently(annotationGetter(key, &appsv1.StatefulSet{}, configMapHashKey), timeoutShort, intervalShort).Should(Equal(configMapHash2))

			Expect(annotationGetter(key, &appsv1.StatefulSet{}, centralizedConfigurationHashKey)()).To(Equal(initialCentralizedConfigHash))

			By("Synchronizing the condition")
			Eventually(clusterConfiguredConditionStatusGetter(key), timeout, interval).Should(BeTrue())

			By("Accepting another change to a redpanda node property to trigger restart")
			Expect(k8sClient.Get(context.Background(), key, &cluster)).To(Succeed())
			if cluster.Spec.AdditionalConfiguration == nil {
				cluster.Spec.AdditionalConfiguration = make(map[string]string)
			}
			cluster.Spec.AdditionalConfiguration["redpanda.cloud_storage_cache_directory"] = "/tmp" // node property
			Expect(k8sClient.Update(context.Background(), &cluster)).To(Succeed())

			By("Changing the hash because of the node property change")
			Eventually(annotationGetter(key, &appsv1.StatefulSet{}, configMapHashKey), timeout, interval).ShouldNot(Equal(configMapHash2))
			configMapHash3 := annotationGetter(key, &appsv1.StatefulSet{}, configMapHashKey)()
			Consistently(annotationGetter(key, &appsv1.StatefulSet{}, configMapHashKey), timeoutShort, intervalShort).Should(Equal(configMapHash3))

			Expect(annotationGetter(key, &appsv1.StatefulSet{}, centralizedConfigurationHashKey)()).To(Equal(initialCentralizedConfigHash))

			By("Synchronizing the condition")
			Eventually(clusterConfiguredConditionStatusGetter(key), timeout, interval).Should(BeTrue())

			By("Not patching the admin API for node property changes")
			Consistently(testAdminAPI.NumPatchesGetter(), timeoutShort, intervalShort).Should(Equal(numberOfPatches))

			By("Deleting the cluster")
			Expect(k8sClient.Delete(context.Background(), redpandaCluster, deleteOptions)).Should(Succeed())
			testutils.DeleteAllInNamespace(testEnv, k8sClient, namespace)
		})

		It("Should defer updating the centralized configuration when admin API is unavailable", func() {
			testAdminAPI.SetUnavailable(true)

			By("Allowing creation of a new cluster")
			key, baseKey, redpandaCluster, namespace := getInitialTestCluster("admin-unavailable")
			Expect(k8sClient.Create(context.Background(), namespace)).Should(Succeed())
			Expect(k8sClient.Create(context.Background(), redpandaCluster)).Should(Succeed())

			By("Creating the StatefulSet and the ConfigMap")
			Eventually(resourceGetter(baseKey, &corev1.ConfigMap{}), timeout, interval).Should(Succeed())
			Eventually(resourceGetter(key, &appsv1.StatefulSet{}), timeout, interval).Should(Succeed())

			By("Setting the configured condition to false until verification")
			Eventually(clusterConfiguredConditionGetter(key), timeout, interval).ShouldNot(BeNil())
			Consistently(clusterConfiguredConditionStatusGetter(key), timeoutShort, intervalShort).Should(BeFalse())

			By("Accepting a change to the properties")
			testAdminAPI.RegisterPropertySchema("prop", admin.ConfigPropertyMetadata{NeedsRestart: false})
			const propValue = "the-value"
			Eventually(clusterUpdater(key, func(cluster *v1alpha1.Cluster) {
				if cluster.Spec.AdditionalConfiguration == nil {
					cluster.Spec.AdditionalConfiguration = make(map[string]string)
				}
				cluster.Spec.AdditionalConfiguration["redpanda.prop"] = propValue
			}), timeout, interval).Should(Succeed())

			By("Maintaining the configured condition to false")
			Eventually(clusterConfiguredConditionStatusGetter(key), timeout, interval).Should(BeFalse())
			Consistently(clusterConfiguredConditionStatusGetter(key), timeoutShort, intervalShort).Should(BeFalse())

			By("Recovering when the API becomes available again")
			testAdminAPI.SetUnavailable(false)
			Eventually(clusterConfiguredConditionStatusGetter(key), timeout, interval).Should(BeTrue())
			Expect(testAdminAPI.PropertyGetter("prop")()).To(Equal(propValue))

			By("Deleting the cluster")
			Expect(k8sClient.Delete(context.Background(), redpandaCluster, deleteOptions)).Should(Succeed())
			testutils.DeleteAllInNamespace(testEnv, k8sClient, namespace)
		})
	})

	Context("When reconciling a cluster without centralized configuration", func() {
		It("Should behave like before", func() {
			By("Allowing creation of a cluster with an old version")
			key, baseKey, redpandaCluster, namespace := getInitialTestCluster("no-central")
			redpandaCluster.Spec.Version = versionWithoutCentralizedConfiguration
			Expect(k8sClient.Create(context.Background(), namespace)).Should(Succeed())
			Expect(k8sClient.Create(context.Background(), redpandaCluster)).Should(Succeed())

			By("Creating the Configmap without .bootstrap.yaml and the statefulset")
			var cm corev1.ConfigMap
			Eventually(resourceGetter(baseKey, &cm), timeout, interval).Should(Succeed())
			Expect(cm.Data[bootstrapConfigurationFile]).To(BeEmpty())
			Eventually(resourceGetter(key, &appsv1.StatefulSet{}), timeout, interval).Should(Succeed())

			By("Not using the condition")
			Consistently(clusterConfiguredConditionGetter(key), timeoutShort, intervalShort).Should(BeNil())

			By("Using the hash annotation directly")
			hash := annotationGetter(key, &appsv1.StatefulSet{}, configMapHashKey)()
			Expect(hash).NotTo(BeEmpty())
			Consistently(annotationGetter(key, &appsv1.StatefulSet{}, configMapHashKey), timeoutShort, intervalShort).Should(Equal(hash))

			By("Not using the central config hash annotation")
			Expect(annotationGetter(key, &appsv1.StatefulSet{}, centralizedConfigurationHashKey)()).To(BeEmpty())

			By("Not patching the admin API")
			Consistently(testAdminAPI.NumPatchesGetter(), timeoutShort, intervalShort).Should(Equal(0))

			By("Accepting a change to any property")
			Eventually(clusterUpdater(key, func(cluster *v1alpha1.Cluster) {
				if cluster.Spec.AdditionalConfiguration == nil {
					cluster.Spec.AdditionalConfiguration = make(map[string]string)
				}
				cluster.Spec.AdditionalConfiguration["redpanda.x"] = "any-val"
			}), timeout, interval).Should(Succeed())

			By("Changing the hash annotation again and not the other one")
			Eventually(annotationGetter(key, &appsv1.StatefulSet{}, configMapHashKey), timeout, interval).ShouldNot(Equal(hash))
			hash2 := annotationGetter(key, &appsv1.StatefulSet{}, configMapHashKey)()
			Expect(hash2).NotTo(BeEmpty())
			Consistently(annotationGetter(key, &appsv1.StatefulSet{}, configMapHashKey), timeoutShort, intervalShort).Should(Equal(hash2))
			Expect(annotationGetter(key, &appsv1.StatefulSet{}, centralizedConfigurationHashKey)()).To(BeEmpty())

			By("Deleting the cluster")
			Expect(k8sClient.Delete(context.Background(), redpandaCluster, deleteOptions)).Should(Succeed())
			testutils.DeleteAllInNamespace(testEnv, k8sClient, namespace)
		})
	})

	Context("When upgrading a cluster from a version without centralized configuration", func() {
		It("Should do a single rolling upgrade", func() {
			By("Allowing creation of a cluster with an old version")
			key, baseKey, redpandaCluster, namespace := getInitialTestCluster("upgrading")
			redpandaCluster.Spec.Version = versionWithoutCentralizedConfiguration
			Expect(k8sClient.Create(context.Background(), namespace)).Should(Succeed())
			Expect(k8sClient.Create(context.Background(), redpandaCluster)).Should(Succeed())

			By("Creating the Configmap and the statefulset")
			Eventually(resourceGetter(baseKey, &corev1.ConfigMap{}), timeout, interval).Should(Succeed())
			var sts appsv1.StatefulSet
			Eventually(resourceGetter(key, &sts), timeout, interval).Should(Succeed())
			initialHash := sts.Spec.Template.Annotations[configMapHashKey]
			Expect(initialHash).NotTo(BeEmpty())
			imageExtractor := func() interface{} {
				conts := make([]string, 0)
				for _, c := range sts.Spec.Template.Spec.Containers {
					conts = append(conts, c.Image)
				}
				return strings.Join(conts, ",")
			}
			initialImages := imageExtractor()

			By("Accepting the upgrade")
			Eventually(clusterUpdater(key, func(cluster *v1alpha1.Cluster) {
				cluster.Spec.Version = versionWithCentralizedConfiguration
			}), timeout, interval).Should(Succeed())

			By("Changing the Configmap and the statefulset accordingly in one shot")
			var cm corev1.ConfigMap
			Eventually(resourceDataGetter(baseKey, &cm, func() interface{} {
				return cm.Data[bootstrapConfigurationFile]
			}), timeout, interval).ShouldNot(BeEmpty())
			Eventually(resourceDataGetter(key, &sts, imageExtractor), timeout, interval).ShouldNot(Equal(initialImages))
			newConfigMapHash := sts.Spec.Template.Annotations[configMapHashKey]
			Expect(newConfigMapHash).NotTo(Equal(initialHash)) // If we still set some cluster properties by default, this is expected to happen
			newCentralizedConfigHash := sts.Spec.Template.Annotations[centralizedConfigurationHashKey]
			Expect(newCentralizedConfigHash).To(BeEmpty())
			Consistently(annotationGetter(key, &sts, centralizedConfigurationHashKey), timeoutShort, intervalShort).Should(BeEmpty())
			Expect(annotationGetter(key, &sts, configMapHashKey)()).To(Equal(newConfigMapHash))

			By("Not patching the admin API for any reason now")
			Consistently(testAdminAPI.NumPatchesGetter(), timeoutShort, intervalShort).Should(Equal(0))

			By("Deleting the cluster")
			Expect(k8sClient.Delete(context.Background(), redpandaCluster, deleteOptions)).Should(Succeed())
			testutils.DeleteAllInNamespace(testEnv, k8sClient, namespace)
		})

		It("Should be able to upgrade and change a cluster even if the admin API is unavailable", func() {
			testAdminAPI.SetUnavailable(true)

			By("Allowing creation of a cluster with an old version")
			key, baseKey, redpandaCluster, namespace := getInitialTestCluster("upgrading-unavailable")
			redpandaCluster.Spec.Version = versionWithoutCentralizedConfiguration
			Expect(k8sClient.Create(context.Background(), namespace)).Should(Succeed())
			Expect(k8sClient.Create(context.Background(), redpandaCluster)).Should(Succeed())

			By("Creating the Configmap and the statefulset")
			Eventually(resourceGetter(baseKey, &corev1.ConfigMap{}), timeout, interval).Should(Succeed())
			var sts appsv1.StatefulSet
			Eventually(resourceGetter(key, &sts), timeout, interval).Should(Succeed())
			initialHash := sts.Spec.Template.Annotations[configMapHashKey]
			Expect(initialHash).NotTo(BeEmpty())

			By("Reporting some state")
			Eventually(resourceDataGetter(key, redpandaCluster, func() interface{} {
				sr := redpandaCluster.Status.Nodes.SchemaRegistry
				if sr == nil {
					return nil
				}
				return sr.Internal
			}), timeout, interval).ShouldNot(BeEmpty())

			By("Accepting the upgrade")
			Eventually(clusterUpdater(key, func(cluster *v1alpha1.Cluster) {
				cluster.Spec.Version = versionWithCentralizedConfiguration
			}), timeout, interval).Should(Succeed())

			By("Changing the Configmap and the statefulset accordingly")
			var cm corev1.ConfigMap
			Eventually(resourceDataGetter(baseKey, &cm, func() interface{} {
				return cm.Data[bootstrapConfigurationFile]
			}), timeout, interval).ShouldNot(BeEmpty())
			// If we still set some cluster properties by default, this is expected to happen
			Eventually(annotationGetter(key, &sts, configMapHashKey), timeout, interval).ShouldNot(Equal(initialHash))
			configMapHash2 := annotationGetter(key, &sts, configMapHashKey)()
			Consistently(annotationGetter(key, &sts, configMapHashKey), timeoutShort, intervalShort).Should(Equal(configMapHash2))

			By("Start using the condition")
			Eventually(clusterConfiguredConditionGetter(key), timeout, interval).ShouldNot(BeNil())

			By("Accepting a change in both node and cluster properties")
			testAdminAPI.RegisterPropertySchema("prop", admin.ConfigPropertyMetadata{NeedsRestart: false})
			const propValue = "the-value"
			Eventually(clusterUpdater(key, func(cluster *v1alpha1.Cluster) {
				cluster.Spec.Configuration.DeveloperMode = !cluster.Spec.Configuration.DeveloperMode
				if cluster.Spec.AdditionalConfiguration == nil {
					cluster.Spec.AdditionalConfiguration = make(map[string]string)
				}
				cluster.Spec.AdditionalConfiguration["redpanda.prop"] = propValue
			}), timeout, interval).Should(Succeed())

			By("Redeploying the statefulset with the new changes")
			Eventually(annotationGetter(key, &sts, configMapHashKey), timeout, interval).ShouldNot(Equal(configMapHash2))
			configMapHash3 := annotationGetter(key, &sts, configMapHashKey)()
			Consistently(annotationGetter(key, &sts, configMapHashKey), timeoutShort, intervalShort).Should(Equal(configMapHash3))

			By("Reflect the problem in the condition")
			Eventually(clusterConfiguredConditionStatusGetter(key), timeout, interval).Should(BeFalse())
			Consistently(clusterConfiguredConditionStatusGetter(key), timeoutShort, intervalShort).Should(BeFalse())

			By("Recovering when the admin API is available again")
			testAdminAPI.SetUnavailable(false)
			Eventually(clusterConfiguredConditionStatusGetter(key), timeout, interval).Should(BeTrue())
			Consistently(clusterConfiguredConditionStatusGetter(key), timeoutShort, intervalShort).Should(BeTrue())

			By("Sending a single patch to the admin API")
			Eventually(testAdminAPI.NumPatchesGetter(), timeout, interval).Should(Equal(1))
			Consistently(testAdminAPI.NumPatchesGetter(), timeoutShort, intervalShort).Should(Equal(1))
			Expect(testAdminAPI.PropertyGetter("prop")()).To(Equal(propValue))

			By("Deleting the cluster")
			Expect(k8sClient.Delete(context.Background(), redpandaCluster, deleteOptions)).Should(Succeed())
			testutils.DeleteAllInNamespace(testEnv, k8sClient, namespace)
		})
	})

	Context("When setting invalid configuration on the cluster", func() {
		It("Should reflect any invalid status in the condition", func() {
			By("Allowing creation of a new cluster")
			key, baseKey, redpandaCluster, namespace := getInitialTestCluster("condition-check")
			Expect(k8sClient.Create(context.Background(), namespace)).Should(Succeed())
			Expect(k8sClient.Create(context.Background(), redpandaCluster)).Should(Succeed())

			By("Creating the Configmap and the statefulset")
			Eventually(resourceGetter(baseKey, &corev1.ConfigMap{}), timeout, interval).Should(Succeed())
			Eventually(resourceGetter(key, &appsv1.StatefulSet{}), timeout, interval).Should(Succeed())

			By("Configuring the cluster")
			Eventually(clusterConfiguredConditionStatusGetter(key), timeout, interval).Should(BeTrue())

			By("Accepting an unknown property")
			Eventually(clusterUpdater(key, func(cluster *v1alpha1.Cluster) {
				if cluster.Spec.AdditionalConfiguration == nil {
					cluster.Spec.AdditionalConfiguration = make(map[string]string)
				}
				cluster.Spec.AdditionalConfiguration["redpanda.unk"] = "nown"
			}), timeout, interval).Should(Succeed())

			By("Reflecting the issue in the condition")
			Eventually(resourceDataGetter(key, redpandaCluster, func() interface{} {
				cond := redpandaCluster.Status.GetCondition(v1alpha1.ClusterConfiguredConditionType)
				if cond == nil {
					return nil
				}
				return fmt.Sprintf("%s/%s", cond.Status, cond.Reason)
			}), timeout, interval).Should(Equal("False/Error"))
			Eventually(resourceDataGetter(key, redpandaCluster, func() interface{} {
				cond := redpandaCluster.Status.GetCondition(v1alpha1.ClusterConfiguredConditionType)
				if cond == nil {
					return nil
				}
				return cond.Message
			}), timeout, interval).Should(ContainSubstring("Unknown"))

			By("Restoring the state when fixing the property")
			Eventually(clusterUpdater(key, func(cluster *v1alpha1.Cluster) {
				delete(cluster.Spec.AdditionalConfiguration, "redpanda.unk")
			}), timeout, interval).Should(Succeed())
			Eventually(clusterConfiguredConditionStatusGetter(key), timeout, interval).Should(BeTrue())

			By("Accepting an invalid property")
			testAdminAPI.RegisterPropertySchema("inv", admin.ConfigPropertyMetadata{Description: "invalid"}) // triggers mock validation
			Eventually(clusterUpdater(key, func(cluster *v1alpha1.Cluster) {
				if cluster.Spec.AdditionalConfiguration == nil {
					cluster.Spec.AdditionalConfiguration = make(map[string]string)
				}
				cluster.Spec.AdditionalConfiguration["redpanda.inv"] = "alid"
			}), timeout, interval).Should(Succeed())

			By("Reflecting the issue in the condition")
			Eventually(resourceDataGetter(key, redpandaCluster, func() interface{} {
				cond := redpandaCluster.Status.GetCondition(v1alpha1.ClusterConfiguredConditionType)
				if cond == nil {
					return nil
				}
				return fmt.Sprintf("%s/%s", cond.Status, cond.Reason)
			}), timeout, interval).Should(Equal("False/Error"))
			Eventually(resourceDataGetter(key, redpandaCluster, func() interface{} {
				cond := redpandaCluster.Status.GetCondition(v1alpha1.ClusterConfiguredConditionType)
				if cond == nil {
					return nil
				}
				return cond.Message
			}), timeout, interval).Should(ContainSubstring("Invalid"))

			By("Restoring the state when fixing the property")
			Eventually(clusterUpdater(key, func(cluster *v1alpha1.Cluster) {
				delete(cluster.Spec.AdditionalConfiguration, "redpanda.inv")
			}), timeout, interval).Should(Succeed())
			Eventually(clusterConfiguredConditionStatusGetter(key), timeout, interval).Should(BeTrue())

			By("None of the property used here should change the hash")
			Expect(annotationGetter(key, &appsv1.StatefulSet{}, centralizedConfigurationHashKey)()).To(BeEmpty())

			By("Deleting the cluster")
			Expect(k8sClient.Delete(context.Background(), redpandaCluster, deleteOptions)).Should(Succeed())
			testutils.DeleteAllInNamespace(testEnv, k8sClient, namespace)
		})

		It("Should report direct validation errors in the condition", func() {
			// admin API will return 400 bad request on invalid configuration (default behavior)
			testAdminAPI.SetDirectValidationEnabled(true)

			By("Allowing creation of a new cluster")
			key, baseKey, redpandaCluster, namespace := getInitialTestCluster("condition-validation")
			Expect(k8sClient.Create(context.Background(), namespace)).Should(Succeed())
			Expect(k8sClient.Create(context.Background(), redpandaCluster)).Should(Succeed())

			By("Creating the Configmap and the statefulset")
			Eventually(resourceGetter(baseKey, &corev1.ConfigMap{}), timeout, interval).Should(Succeed())
			Eventually(resourceGetter(key, &appsv1.StatefulSet{}), timeout, interval).Should(Succeed())

			By("Configuring the cluster")
			Eventually(clusterConfiguredConditionStatusGetter(key), timeout, interval).Should(BeTrue())

			By("Accepting an unknown property")
			Eventually(clusterUpdater(key, func(cluster *v1alpha1.Cluster) {
				if cluster.Spec.AdditionalConfiguration == nil {
					cluster.Spec.AdditionalConfiguration = make(map[string]string)
				}
				cluster.Spec.AdditionalConfiguration["redpanda.unk"] = "nown-value"
			}), timeout, interval).Should(Succeed())

			By("Reflecting the issue in the condition")
			Eventually(resourceDataGetter(key, redpandaCluster, func() interface{} {
				cond := redpandaCluster.Status.GetCondition(v1alpha1.ClusterConfiguredConditionType)
				if cond == nil {
					return nil
				}
				return fmt.Sprintf("%s/%s/%s", cond.Status, cond.Reason, cond.Message)
			}), timeout, interval).Should(Equal("False/Error/Mock bad request message"))

			By("Restoring the state when fixing the property")
			Eventually(clusterUpdater(key, func(cluster *v1alpha1.Cluster) {
				delete(cluster.Spec.AdditionalConfiguration, "redpanda.unk")
			}), timeout, interval).Should(Succeed())
			Eventually(clusterConfiguredConditionStatusGetter(key), timeout, interval).Should(BeTrue())

			By("None of the property used here should change the hash")
			Expect(annotationGetter(key, &appsv1.StatefulSet{}, centralizedConfigurationHashKey)()).To(BeEmpty())

			By("Deleting the cluster")
			Expect(k8sClient.Delete(context.Background(), redpandaCluster, deleteOptions)).Should(Succeed())
			testutils.DeleteAllInNamespace(testEnv, k8sClient, namespace)
		})

		It("Should report configuration errors present in the .bootstrap.yaml file", func() {
			// Inject property before creating the cluster, simulating .bootstrap.yaml
			const val = "nown"
			_, err := testAdminAPI.PatchClusterConfig(context.Background(), map[string]interface{}{
				"unk": val,
			}, nil)
			Expect(err).To(BeNil())

			By("Allowing creation of a new cluster")
			key, baseKey, redpandaCluster, namespace := getInitialTestCluster("condition-bootstrap-failure")
			if redpandaCluster.Spec.AdditionalConfiguration == nil {
				redpandaCluster.Spec.AdditionalConfiguration = make(map[string]string)
			}
			redpandaCluster.Spec.AdditionalConfiguration["redpanda.unk"] = val
			Expect(k8sClient.Create(context.Background(), namespace)).Should(Succeed())
			Expect(k8sClient.Create(context.Background(), redpandaCluster)).Should(Succeed())

			By("Creating the Configmap and the statefulset")
			Eventually(resourceGetter(baseKey, &corev1.ConfigMap{}), timeout, interval).Should(Succeed())
			Eventually(resourceGetter(key, &appsv1.StatefulSet{}), timeout, interval).Should(Succeed())

			By("Always adding the last-applied-configuration annotation on the configmap")
			Eventually(annotationGetter(baseKey, &corev1.ConfigMap{}, lastAppliedConfiguraitonHashKey), timeout, interval).ShouldNot(BeEmpty())

			By("Marking the cluster as not properly configured")
			Eventually(clusterConfiguredConditionStatusGetter(key), timeout, interval).Should(BeFalse())
			Consistently(clusterConfiguredConditionStatusGetter(key), timeoutShort, intervalShort).Should(BeFalse())

			By("Restoring the state when fixing the property")
			Eventually(clusterUpdater(key, func(cluster *v1alpha1.Cluster) {
				delete(cluster.Spec.AdditionalConfiguration, "redpanda.unk")
			}), timeout, interval).Should(Succeed())
			Eventually(clusterConfiguredConditionStatusGetter(key), timeout, interval).Should(BeTrue())

			By("None of the property used here should change the hash")
			Expect(annotationGetter(key, &appsv1.StatefulSet{}, centralizedConfigurationHashKey)()).To(BeEmpty())

			By("Deleting the cluster")
			Expect(k8sClient.Delete(context.Background(), redpandaCluster, deleteOptions)).Should(Succeed())
			testutils.DeleteAllInNamespace(testEnv, k8sClient, namespace)
		})
	})

	Context("When external factors change configuration of a cluster", func() {
		It("The drift detector restore all managed properties", func() {
			// Registering two properties, one of them managed by the operator
			const (
				unmanagedProp = "unmanaged_prop"
				managedProp   = "managed_prop"

				desiredManagedPropValue = "desired-managed-value"
				unmanagedPropValue      = "unmanaged-prop-value"

				externalChangeManagedPropValue   = "external-managed-value"
				externalChangeUnmanagedPropValue = "external-unmanaged-value"
			)

			testAdminAPI.RegisterPropertySchema(managedProp, admin.ConfigPropertyMetadata{NeedsRestart: false})
			testAdminAPI.RegisterPropertySchema(unmanagedProp, admin.ConfigPropertyMetadata{NeedsRestart: false})
			testAdminAPI.SetProperty(unmanagedProp, unmanagedPropValue)

			By("Allowing creation of a new cluster")
			key, baseKey, redpandaCluster, namespace := getInitialTestCluster("central-drift-detector")
			Expect(k8sClient.Create(context.Background(), namespace)).Should(Succeed())
			Expect(k8sClient.Create(context.Background(), redpandaCluster)).Should(Succeed())

			By("Creating a Configmap and StatefulSet with the bootstrap configuration")
			Eventually(resourceGetter(baseKey, &corev1.ConfigMap{}), timeout, interval).Should(Succeed())
			Eventually(resourceGetter(key, &appsv1.StatefulSet{}), timeout, interval).Should(Succeed())

			By("Synchronizing the condition")
			Eventually(clusterConfiguredConditionStatusGetter(key), timeout, interval).Should(BeTrue())

			By("Changing the configuration")
			Eventually(clusterUpdater(key, func(cluster *v1alpha1.Cluster) {
				if cluster.Spec.AdditionalConfiguration == nil {
					cluster.Spec.AdditionalConfiguration = make(map[string]string)
				}
				cluster.Spec.AdditionalConfiguration["redpanda."+managedProp] = desiredManagedPropValue
			}), timeout, interval).Should(Succeed())

			By("Having it reflected in the configuration")
			Eventually(testAdminAPI.PropertyGetter(managedProp), timeout, interval).Should(Equal(desiredManagedPropValue))
			Eventually(testAdminAPI.PropertyGetter(unmanagedProp), timeout, interval).Should(Equal(unmanagedPropValue))

			By("Synchronizing the condition")
			Eventually(clusterConfiguredConditionStatusGetter(key), timeout, interval).Should(BeTrue())

			By("Having an external system change both properties")
			testAdminAPI.SetProperty(managedProp, externalChangeManagedPropValue)
			testAdminAPI.SetProperty(unmanagedProp, externalChangeUnmanagedPropValue)

			By("Having the managed property restored to the original value")
			Eventually(testAdminAPI.PropertyGetter(managedProp), timeout, interval).Should(Equal(desiredManagedPropValue))

			By("Leaving the unmanaged property as is")
			Expect(testAdminAPI.PropertyGetter(unmanagedProp)()).To(Equal(externalChangeUnmanagedPropValue))

			By("Deleting the cluster")
			Expect(k8sClient.Delete(context.Background(), redpandaCluster, deleteOptions)).Should(Succeed())
			testutils.DeleteAllInNamespace(testEnv, k8sClient, namespace)
		})
	})

	Context("When managing a RedpandaCluster with additional command line arguments", func() {
		It("Can initialize a cluster with centralized configuration", func() {
			args := map[string]string{
				"overprovisioned":   "",
				"default-log-level": "info",
			}

			By("Allowing creation of a new cluster")
			key, baseKey, redpandaCluster, namespace := getInitialTestCluster("test-additional-cmdline")
			redpandaCluster.Spec.Configuration.AdditionalCommandlineArguments = args
			Expect(k8sClient.Create(context.Background(), namespace)).Should(Succeed())
			Expect(k8sClient.Create(context.Background(), redpandaCluster)).Should(Succeed())

			By("Creating the Configmap and the statefulset")
			Eventually(resourceGetter(baseKey, &corev1.ConfigMap{}), timeout, interval).Should(Succeed())

			var sts appsv1.StatefulSet
			Eventually(resourceGetter(key, &appsv1.StatefulSet{})).Should(Succeed())
			Consistently(resourceDataGetter(key, &sts, func() interface{} {
				return *sts.Spec.Replicas
			}), timeoutShort, intervalShort).Should(Equal(int32(1)))

			By("Looking for the correct arguments")
			cnt := 0
			finalArgs := make(map[string]int)
			for k, v := range args {
				key := fmt.Sprintf("--%s", k)
				if v != "" {
					key = fmt.Sprintf("%s=%s", key, v)
				}
				finalArgs[key] = 1
			}
			for _, arg := range sts.Spec.Template.Spec.Containers[0].Args {
				if _, ok := finalArgs[arg]; ok {
					cnt++
				}
			}
			Expect(cnt).To(Equal(len(args)))

			By("Setting the configmap-hash annotation on the statefulset")
			Eventually(annotationGetter(key, &sts, configMapHashKey), timeout, interval).ShouldNot(BeEmpty())

			By("Not patching the admin API for any reason")
			Consistently(testAdminAPI.NumPatchesGetter(), timeoutShort, intervalShort).Should(Equal(0))

			By("Synchronizing the condition")
			Eventually(clusterConfiguredConditionStatusGetter(key), timeout, interval).Should(BeTrue())

			By("Not using the centralized-config annotation at this stage on the statefulset")
			Consistently(annotationGetter(key, &sts, centralizedConfigurationHashKey), timeoutShort, intervalShort).Should(BeEmpty())

			By("Deleting the cluster")
			Expect(k8sClient.Delete(context.Background(), redpandaCluster)).Should(Succeed())
		})
	})
})

func getInitialTestCluster(
	name string,
) (
	key types.NamespacedName,
	baseKey types.NamespacedName,
	cluster *v1alpha1.Cluster,
	namespace *corev1.Namespace,
) {
	ns := strings.Replace(namesgenerator.GetRandomName(0), "_", "-", 1)
	key = types.NamespacedName{
		Name:      name,
		Namespace: ns,
	}
	baseKey = types.NamespacedName{
		Name:      name + "-base",
		Namespace: ns,
	}

	cluster = &v1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      key.Name,
			Namespace: key.Namespace,
		},
		Spec: v1alpha1.ClusterSpec{
			Image:    "vectorized/redpanda",
			Version:  versionWithCentralizedConfiguration,
			Replicas: pointer.Int32(1),
			Configuration: v1alpha1.RedpandaConfig{
				KafkaAPI: []v1alpha1.KafkaAPI{
					{
						Port: 9092,
					},
				},
				AdminAPI: []v1alpha1.AdminAPI{{Port: 9644}},
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
	namespace = &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: ns,
		},
	}
	return key, baseKey, cluster, namespace
}
