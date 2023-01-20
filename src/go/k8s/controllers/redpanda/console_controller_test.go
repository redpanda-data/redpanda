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
	"reflect"
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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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

		ConsoleName = "test-console"

		timeout  = time.Second * 30
		interval = time.Millisecond * 100

		deploymentImage      = "vectorized/console:latest"
		enableSchemaRegistry = true
		enableConnect        = false
	)

	var (
		ConsoleNamespace string
		key              types.NamespacedName
		redpandaCluster  *redpandav1alpha1.Cluster
		namespace        *corev1.Namespace
	)

	BeforeEach(func() {
		ctx := context.Background()
		if redpandaCluster == nil {
			key, _, redpandaCluster, namespace = getInitialTestCluster(ClusterName)
			ConsoleNamespace = key.Namespace
		}
		if err := k8sClient.Get(ctx, key, &redpandav1alpha1.Cluster{}); err != nil {
			if !apierrors.IsNotFound(err) {
				Expect(err).To(Equal(nil))
			}
			Expect(k8sClient.Create(ctx, namespace)).Should(Succeed())
			Expect(k8sClient.Create(ctx, redpandaCluster)).Should(Succeed())
			Eventually(clusterConfiguredConditionStatusGetter(key), timeout, interval).Should(BeTrue())
		}
		if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: ConsoleNamespace, Name: ConsoleName}, &redpandav1alpha1.Console{}); err != nil {
			if !apierrors.IsNotFound(err) {
				Expect(err).To(Equal(nil))
			}
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
					ClusterRef:     redpandav1alpha1.NamespaceNameRef{Namespace: key.Namespace, Name: key.Name},
					SchemaRegistry: redpandav1alpha1.Schema{Enabled: enableSchemaRegistry},
					Deployment:     redpandav1alpha1.Deployment{Image: deploymentImage},
					Connect:        redpandav1alpha1.Connect{Enabled: enableConnect},
				},
			}
			Expect(k8sClient.Create(ctx, console)).Should(Succeed())
			consoleLookupKey := types.NamespacedName{Name: ConsoleName, Namespace: ConsoleNamespace}
			Eventually(func() bool {
				return k8sClient.Get(ctx, consoleLookupKey, &redpandav1alpha1.Console{}) == nil
			}, timeout, interval).Should(BeTrue())
		}
	})

	Context("When creating Console", func() {
		ctx := context.Background()
		It("Should expose Console web app", func() {
			By("Getting Console")
			consoleLookupKey := types.NamespacedName{Name: ConsoleName, Namespace: ConsoleNamespace}
			console := &redpandav1alpha1.Console{}
			Expect(k8sClient.Get(ctx, consoleLookupKey, console)).Should(Succeed())

			By("Having a Secret for SASL user")
			secretLookupKey := types.NamespacedName{Name: fmt.Sprintf("%s-%s", ConsoleName, resources.ConsoleSuffix), Namespace: ConsoleNamespace}
			createdSecret := &corev1.Secret{}
			Eventually(func() error {
				return k8sClient.Get(ctx, secretLookupKey, createdSecret)
			}, timeout, interval).Should(Succeed())

			// Not checking if ACLs are created, KafkaAdmin is mocked

			By("Having a valid ConfigMap")
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

			By("Having a running Deployment")
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

			By("Having a Service")
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

			By("Having the Console URLs in status")
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

	Context("When updating Console", func() {
		ctx := context.Background()
		It("Should not create new ConfigMap if no change on spec", func() {
			var configmapNsn string

			By("Getting Console")
			consoleLookupKey := types.NamespacedName{Name: ConsoleName, Namespace: ConsoleNamespace}
			Eventually(func() bool {
				createdConsole := &redpandav1alpha1.Console{}
				if err := k8sClient.Get(ctx, consoleLookupKey, createdConsole); err != nil {
					return false
				}

				if ref := createdConsole.Status.ConfigMapRef; ref != nil {
					configmapNsn = fmt.Sprintf("%s/%s", ref.Namespace, ref.Name)
					return true
				}
				return false
			}, timeout, interval).Should(BeTrue(), "Console %s/%s should be present with ConfigMap reference in the Status stanza", ConsoleNamespace, ConsoleName)

			By("Adding label to Console")
			Eventually(consoleUpdater(consoleLookupKey, func(console *redpandav1alpha1.Console) {
				console.SetLabels(map[string]string{"test.redpanda.vectorized.io/name": "updating-console"})
			}), timeout, interval).Should(Succeed())

			By("Checking updated console label exist")
			Eventually(func() bool {
				updatedConsole := &redpandav1alpha1.Console{}
				if err := k8sClient.Get(ctx, consoleLookupKey, updatedConsole); err != nil {
					return false
				}
				labels := updatedConsole.GetLabels()
				_, ok := labels["test.redpanda.vectorized.io/name"]
				return ok
			}, timeout, interval).Should(BeTrue())

			By("Checking updated console label content")
			Eventually(func() string {
				updatedConsole := &redpandav1alpha1.Console{}
				if err := k8sClient.Get(ctx, consoleLookupKey, updatedConsole); err != nil {
					return ""
				}
				labels := updatedConsole.GetLabels()
				return labels["test.redpanda.vectorized.io/name"]
			}, timeout, interval).Should(Equal("updating-console"))

			By("Checking ConfigMapRef exist in status")
			Eventually(func() bool {
				updatedConsole := &redpandav1alpha1.Console{}
				if err := k8sClient.Get(ctx, consoleLookupKey, updatedConsole); err != nil {
					return false
				}

				updatedRef := updatedConsole.Status.ConfigMapRef
				return updatedRef != nil
			}, timeout, interval).Should(BeTrue())

			By("Checking ConfigMapRef did not change")
			Eventually(func() string {
				updatedConsole := &redpandav1alpha1.Console{}
				if err := k8sClient.Get(ctx, consoleLookupKey, updatedConsole); err != nil {
					return "console not found"
				}
				updatedRef := updatedConsole.Status.ConfigMapRef
				if updatedRef == nil {
					return "missing config map reference"
				}
				return fmt.Sprintf("%s/%s", updatedRef.Namespace, updatedRef.Name)
			}, timeout, interval).Should(Equal(configmapNsn))
		})
	})

	Context("When updating Console with Enterprise features", func() {
		ctx := context.Background()
		It("Should create Enterprise fields in ConfigMap", func() {
			var (
				rbacName    = fmt.Sprintf("%s-rbac", ConsoleName)
				rbacDataKey = consolepkg.EnterpriseRBACDataKey
				rbacDataVal = `roleBindings:
- roleName: admin
  metadata:
  subjects:
	- kind: user
	  provider: Google
	  name: john.doe@example.com`
			)

			By("Creating Enterprise RBAC ConfigMap")
			rbac := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      rbacName,
					Namespace: ConsoleNamespace,
				},
				Data: map[string]string{
					rbacDataKey: rbacDataVal,
				},
			}
			Expect(k8sClient.Create(ctx, rbac)).Should(Succeed())

			var (
				licenseName    = fmt.Sprintf("%s-license", ConsoleName)
				licenseDataKey = "custom-license-secret-key"
				licenseDataVal = "some-random-license-string"
			)

			By("Creating Enterprise License Secret")
			license := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      licenseName,
					Namespace: ConsoleNamespace,
				},
				Data: map[string][]byte{licenseDataKey: []byte(licenseDataVal)},
			}
			Expect(k8sClient.Create(ctx, license)).Should(Succeed())

			var (
				jwtName    = fmt.Sprintf("%s-jwt", ConsoleName)
				jwtDataKey = "custom-jwt-secret-key"
				jwtDataVal = "some-random-jwt-string"
			)

			By("Creating Enterprise JWT Secret")
			jwt := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      jwtName,
					Namespace: ConsoleNamespace,
				},
				Data: map[string][]byte{jwtDataKey: []byte(jwtDataVal)},
			}
			Expect(k8sClient.Create(ctx, jwt)).Should(Succeed())

			var (
				googleName         = fmt.Sprintf("%s-google", ConsoleName)
				googleClientId     = "123456654321-abcdefghi123456abcdefghi123456ab.apps.googleusercontent.com" //nolint:stylecheck // Console uses clientId naming
				googleClientSecret = "some-random-client-secret"
			)

			By("Creating Enterprise Google Login Credentials Secret")
			google := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      googleName,
					Namespace: ConsoleNamespace,
				},
				Data: map[string][]byte{
					"clientId":     []byte(googleClientId),
					"clientSecret": []byte(googleClientSecret),
				},
			}
			Expect(k8sClient.Create(ctx, google)).Should(Succeed())

			By("Updating Console Enterprise fields")
			Eventually(consoleUpdater(types.NamespacedName{Namespace: ConsoleNamespace, Name: ConsoleName}, func(console *redpandav1alpha1.Console) {
				console.Spec.Enterprise = &redpandav1alpha1.Enterprise{
					RBAC: redpandav1alpha1.EnterpriseRBAC{
						Enabled:         true,
						RoleBindingsRef: corev1.LocalObjectReference{Name: rbacName},
					},
				}
				console.Spec.LicenseRef = &redpandav1alpha1.SecretKeyRef{
					Name:      licenseName,
					Namespace: ConsoleNamespace,
					Key:       licenseDataKey,
				}
				console.Spec.Login = &redpandav1alpha1.EnterpriseLogin{
					Enabled: true,
					JWTSecretRef: redpandav1alpha1.SecretKeyRef{
						Name:      jwtName,
						Namespace: ConsoleNamespace,
						Key:       jwtDataKey,
					},
					Google: &redpandav1alpha1.EnterpriseLoginGoogle{
						Enabled: true,
						ClientCredentialsRef: redpandav1alpha1.NamespaceNameRef{
							Name:      googleName,
							Namespace: ConsoleNamespace,
						},
					},
				}
			}), timeout, interval).Should(Succeed())

			By("Having a valid Enterprise ConfigMap")
			console := &redpandav1alpha1.Console{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: ConsoleNamespace, Name: ConsoleName}, console)).Should(Succeed())
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
					if cc.License != licenseDataVal {
						return false
					}
					isGoogleConfigInvalid := !cc.Login.Google.Enabled || cc.Login.Google.ClientID != googleClientId || cc.Login.Google.ClientSecret != googleClientSecret
					if !cc.Login.Enabled || cc.Login.JWTSecret != jwtDataVal || isGoogleConfigInvalid {
						return false
					}
				}
				return true
			}, timeout, interval).Should(BeTrue())
		})
	})

	Context("When enabling multiple Login providers", func() {
		ctx := context.Background()
		It("Should prioritize RedpandaCloud", func() {
			var (
				rpCloudDomain         = "test.auth.vectorized.io"
				rpCloudAudience       = "dev.vectorized.io"
				rpCloudAllowedOrigins = []string{"http://localhost:3000", "https://dev--redpanda-cloud.local.app/"}
			)

			By("Updating Console RedpandaCloud Login fields")
			Eventually(consoleUpdater(types.NamespacedName{Namespace: ConsoleNamespace, Name: ConsoleName}, func(console *redpandav1alpha1.Console) {
				console.Spec.Login.RedpandaCloud = &redpandav1alpha1.EnterpriseLoginRedpandaCloud{
					Enabled:        true,
					Domain:         rpCloudDomain,
					Audience:       rpCloudAudience,
					AllowedOrigins: rpCloudAllowedOrigins,
				}
			}), timeout, interval).Should(Succeed())

			By("Having only RedpandaCloud provider in ConfigMap")
			console := &redpandav1alpha1.Console{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: ConsoleNamespace, Name: ConsoleName}, console)).Should(Succeed())
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
					if cc.Login.Google != nil {
						return false
					}
					rpCloudConfig := cc.Login.RedpandaCloud
					if !rpCloudConfig.Enabled || rpCloudConfig.Domain != rpCloudDomain || rpCloudConfig.Audience != rpCloudAudience || !reflect.DeepEqual(rpCloudAllowedOrigins, rpCloudConfig.AllowedOrigins) {
						return false
					}
				}
				return true
			}, timeout, interval).Should(BeTrue())
		})
	})

	Context("When using prometheus endpoint", func() {
		ctx := context.Background()
		It("Should prioritize RedpandaCloud", func() {
			var (
				secretName  = "basic-auth"
				password    = "password"
				passwordKey = "password"
			)
			By("Creating Basic auth secret")
			jwt := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      secretName,
					Namespace: ConsoleNamespace,
				},
				Data: map[string][]byte{passwordKey: []byte(password)},
			}
			Expect(k8sClient.Create(ctx, jwt)).Should(Succeed())

			By("Updating Console RedpandaCloud Login fields")
			Eventually(consoleUpdater(types.NamespacedName{Namespace: ConsoleNamespace, Name: ConsoleName}, func(console *redpandav1alpha1.Console) {
				console.Spec.Cloud = &redpandav1alpha1.CloudConfig{
					PrometheusEndpoint: &redpandav1alpha1.PrometheusEndpointConfig{
						Enabled: true,
						BasicAuth: redpandav1alpha1.BasicAuthConfig{
							Username: "username",
							PasswordRef: redpandav1alpha1.SecretKeyRef{
								Name:      secretName,
								Namespace: ConsoleNamespace,
								Key:       passwordKey,
							},
						},
						Prometheus: &redpandav1alpha1.PrometheusConfig{
							Address: "address",
							Jobs: []redpandav1alpha1.PrometheusScraperJobConfig{
								{
									JobName:    "job",
									KeepLabels: []string{"label"},
								},
							},
							TargetRefreshInterval: &metav1.Duration{Duration: 1 * time.Minute},
						},
					},
				}
			}), timeout, interval).Should(Succeed())

			By("Having only RedpandaCloud provider in ConfigMap")
			console := &redpandav1alpha1.Console{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: ConsoleNamespace, Name: ConsoleName}, console)).Should(Succeed())
			createdConfigMaps := &corev1.ConfigMapList{}
			var latestConfig *consolepkg.ConsoleConfig
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
					if !cc.Cloud.PrometheusEndpoint.Enabled {
						return false
					}
					latestConfig = cc
				}
				return true
			}, timeout, interval).Should(BeTrue())
			prometheusConfig := latestConfig.Cloud.PrometheusEndpoint

			Expect(prometheusConfig.BasicAuth.Username).Should(Equal("username"))
			Expect(prometheusConfig.Prometheus.Address).Should(Equal("address"))
			Expect(prometheusConfig.Prometheus.Jobs[0].JobName).Should(Equal("job"))
			Expect(prometheusConfig.Prometheus.Jobs[0].KeepLabels[0]).Should(Equal("label"))

			By("Having a deployment with password injected")
			deploymentLookupKey := types.NamespacedName{Name: ConsoleName, Namespace: ConsoleNamespace}
			createdDeployment := &appsv1.Deployment{}
			Eventually(func() bool {
				if err := k8sClient.Get(ctx, deploymentLookupKey, createdDeployment); err != nil {
					return false
				}
				return true
			}, timeout, interval).Should(BeTrue())
			envs := createdDeployment.Spec.Template.Spec.Containers[0].Env

			Expect(envs[0].Name).Should(Equal("CLOUD_PROMETHEUSENDPOINT_BASICAUTH_PASSWORD"))
			Expect(envs[0].ValueFrom.SecretKeyRef.Key).Should(Equal(passwordKey))
			Expect(envs[0].ValueFrom.SecretKeyRef.Name).Should(Equal(secretName))
		})
	})

	Context("When ConfigMap is deleted", func() {
		ctx := context.Background()
		It("Should reconcile and recreate the ConfigMap", func() {
			By("Getting Console")
			consoleLookupKey := types.NamespacedName{Name: ConsoleName, Namespace: ConsoleNamespace}
			createdConsole := &redpandav1alpha1.Console{}
			Expect(k8sClient.Get(ctx, consoleLookupKey, createdConsole)).Should(Succeed())

			By("Getting the ConfigMap")
			createdConfigMaps := &corev1.ConfigMapList{}
			Expect(k8sClient.List(ctx, createdConfigMaps, client.MatchingLabels(labels.ForConsole(createdConsole)), client.InNamespace(ConsoleNamespace))).Should(Succeed())
			Expect(len(createdConfigMaps.Items)).To(Equal(1))

			By("Deleting the ConfigMap")
			Expect(k8sClient.Delete(ctx, &createdConfigMaps.Items[0])).Should(Succeed())
			Eventually(func() bool {
				createdConfigMaps := &corev1.ConfigMapList{}
				if err := k8sClient.List(ctx, createdConfigMaps, client.MatchingLabels(labels.ForConsole(createdConsole)), client.InNamespace(ConsoleNamespace)); err != nil {
					return false
				}
				if len(createdConfigMaps.Items) != 1 {
					return false
				}
				return true
			}, timeout, interval).Should(BeTrue())
		})
	})
})
