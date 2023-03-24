package redpanda_test

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	redpandacontrollers "github.com/redpanda-data/redpanda/src/go/k8s/controllers/redpanda"
	adminutils "github.com/redpanda-data/redpanda/src/go/k8s/pkg/admin"
	consolepkg "github.com/redpanda-data/redpanda/src/go/k8s/pkg/console"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources/types"
	"github.com/redpanda-data/redpanda/src/go/k8s/webhooks/redpanda"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
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

//nolint:funlen // Test using testEnv needs to be long
func TestDoNotValidateWhenDeleted(t *testing.T) {
	testEnv := &envtest.Environment{
		CRDDirectoryPaths: []string{filepath.Join("..", "..", "config", "crd", "bases")},
		WebhookInstallOptions: envtest.WebhookInstallOptions{
			Paths: []string{filepath.Join("..", "..", "config", "webhook")},
		},
	}

	cfg, err := testEnv.Start()
	defer testEnv.Stop() //nolint:errcheck // in test test env error is not relevant
	require.NoError(t, err)
	require.NotNil(t, cfg)

	err = scheme.AddToScheme(scheme.Scheme)
	require.NoError(t, err)
	err = v1alpha1.AddToScheme(scheme.Scheme)
	require.NoError(t, err)

	testAdminAPI := &adminutils.MockAdminAPI{Log: ctrl.Log.WithName("testAdminAPI").WithName("mockAdminAPI")}
	testAdminAPIFactory := func(
		_ context.Context,
		_ client.Reader,
		_ *v1alpha1.Cluster,
		_ string,
		_ types.AdminTLSConfigProvider,
		ordinals ...int32,
	) (adminutils.AdminAPIClient, error) {
		if len(ordinals) == 1 {
			return &adminutils.ScopedMockAdminAPI{
				MockAdminAPI: testAdminAPI,
				Ordinal:      ordinals[0],
			}, nil
		}
		return testAdminAPI, nil
	}

	webhookInstallOptions := &testEnv.WebhookInstallOptions
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:             scheme.Scheme,
		Host:               webhookInstallOptions.LocalServingHost,
		Port:               webhookInstallOptions.LocalServingPort,
		CertDir:            webhookInstallOptions.LocalServingCertDir,
		LeaderElection:     false,
		MetricsBindAddress: "0",
	})
	require.NoError(t, err)

	testStore := consolepkg.NewStore(mgr.GetClient(), mgr.GetScheme())
	testKafkaAdmin := &mockKafkaAdmin{}
	testKafkaAdminFactory := func(context.Context, client.Client, *v1alpha1.Cluster, *consolepkg.Store) (consolepkg.KafkaAdminClient, error) {
		return testKafkaAdmin, nil
	}

	err = (&v1alpha1.Cluster{}).SetupWebhookWithManager(mgr)
	require.NoError(t, err)
	hookServer := mgr.GetWebhookServer()

	err = (&redpandacontrollers.ConsoleReconciler{
		Client:                  mgr.GetClient(),
		Scheme:                  mgr.GetScheme(),
		Log:                     ctrl.Log.WithName("controllers").WithName("redpanda").WithName("Console"),
		AdminAPIClientFactory:   testAdminAPIFactory,
		Store:                   testStore,
		EventRecorder:           mgr.GetEventRecorderFor("Console"),
		KafkaAdminClientFactory: testKafkaAdminFactory,
	}).WithClusterDomain("").SetupWithManager(mgr)
	require.NoError(t, err)

	hookServer.Register("/mutate-redpanda-vectorized-io-v1alpha1-console", &webhook.Admission{Handler: &redpanda.ConsoleDefaulter{Client: mgr.GetClient()}})
	hookServer.Register("/validate-redpanda-vectorized-io-v1alpha1-console", &webhook.Admission{Handler: &redpanda.ConsoleValidator{Client: mgr.GetClient()}})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		err = mgr.Start(ctx)
		if err != nil {
			require.NoError(t, err)
		}
	}()

	for !mgr.GetCache().WaitForCacheSync(ctx) {
	}

	dialer := &net.Dialer{Timeout: time.Second}
	addrPort := fmt.Sprintf("%s:%d", webhookInstallOptions.LocalServingHost, webhookInstallOptions.LocalServingPort)
	conn, err := tls.DialWithDialer(dialer, "tcp", addrPort, &tls.Config{InsecureSkipVerify: true}) //nolint:gosec // in test TLS verification is not necessary

	for err != nil {
		time.Sleep(1 * time.Second)
		conn, err = tls.DialWithDialer(dialer, "tcp", addrPort, &tls.Config{InsecureSkipVerify: true}) //nolint:gosec // in test TLS verification is not necessary
	}

	conn.Close()

	c, err := client.New(testEnv.Config, client.Options{Scheme: scheme.Scheme})
	require.NoError(t, err)

	one := int32(1)
	cluster := v1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cluster",
			Namespace: "default",
		},
		Spec: v1alpha1.ClusterSpec{
			Replicas: &one,
			Configuration: v1alpha1.RedpandaConfig{
				KafkaAPI: []v1alpha1.KafkaAPI{
					{
						Port: 9091,
					},
				},
				AdminAPI: []v1alpha1.AdminAPI{
					{
						Port: 8080,
					},
				},
			},
		},
	}

	console := v1alpha1.Console{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "console",
			Namespace: "default",
		},
		Spec: v1alpha1.ConsoleSpec{
			Server: v1alpha1.Server{
				HTTPListenPort: 8080,
			},
			ClusterRef: v1alpha1.NamespaceNameRef{
				Name:      "cluster",
				Namespace: "default",
			},
			Deployment: v1alpha1.Deployment{
				Image: "vectorized/console:master-173596f",
			},
			Connect: v1alpha1.Connect{Enabled: true},
			Cloud: &v1alpha1.CloudConfig{
				PrometheusEndpoint: &v1alpha1.PrometheusEndpointConfig{
					Enabled: true,
					BasicAuth: v1alpha1.BasicAuthConfig{
						Username: "test",
						PasswordRef: v1alpha1.SecretKeyRef{
							Name:      "prom-pass",
							Namespace: "default",
							Key:       "pass",
						},
					},
					Prometheus: &v1alpha1.PrometheusConfig{
						Address: "test",
						Jobs: []v1alpha1.PrometheusScraperJobConfig{
							{JobName: "test", KeepLabels: []string{"test"}},
						},
					},
				},
			},
		},
	}

	secret := corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "prom-pass",
			Namespace: "default",
		},
		StringData: map[string]string{
			"pass": "pass",
		},
	}

	err = c.Create(ctx, &secret)
	require.NoError(t, err)

	err = c.Create(ctx, &cluster)
	require.NoError(t, err)

	cluster.Status = v1alpha1.ClusterStatus{
		Conditions: []v1alpha1.ClusterCondition{
			{
				Type:               v1alpha1.ClusterConfiguredConditionType,
				Status:             corev1.ConditionTrue,
				LastTransitionTime: metav1.Now(),
			},
		},
	}
	err = c.Status().Update(ctx, &cluster)
	require.NoError(t, err)

	err = c.Create(ctx, &console)
	require.NoError(t, err)

	err = c.Get(ctx, client.ObjectKey{
		Namespace: "default",
		Name:      "cluster",
	}, &cluster)
	require.NoError(t, err)

	err = c.Get(ctx, client.ObjectKey{
		Namespace: "default",
		Name:      "console",
	}, &console)
	require.NoError(t, err)

	for len(console.Finalizers) == 0 {
		time.Sleep(1 * time.Second)
		err = c.Get(ctx, client.ObjectKey{
			Namespace: "default",
			Name:      "console",
		}, &console)
		require.NoError(t, err)
	}

	err = c.Delete(ctx, &secret)
	require.NoError(t, err)

	err = c.Delete(ctx, &console)
	require.NoError(t, err)

	err = c.Get(ctx, client.ObjectKey{
		Namespace: "default",
		Name:      "console",
	}, &console)

	for !apierrors.IsNotFound(err) {
		time.Sleep(1 * time.Second)
		err = c.Get(ctx, client.ObjectKey{
			Namespace: "default",
			Name:      "console",
		}, &console)
	}
}

//nolint:funlen // this is table driven test
func TestValidatePrometheus(t *testing.T) {
	ctl := fake.NewClientBuilder().Build()
	consoleNamespace := "console"
	secretName := "secret"
	passwordKey := "password"
	secret := corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: consoleNamespace,
		},
		Data: map[string][]byte{
			passwordKey: []byte("password"),
		},
	}
	require.NoError(t, ctl.Create(context.TODO(), &secret))

	tests := []struct {
		name                    string
		cloudConfig             v1alpha1.CloudConfig
		expectedValidationError bool
	}{
		{"valid", v1alpha1.CloudConfig{
			PrometheusEndpoint: &v1alpha1.PrometheusEndpointConfig{
				Enabled: true,
				BasicAuth: v1alpha1.BasicAuthConfig{
					Username: "username",
					PasswordRef: v1alpha1.SecretKeyRef{
						Name:      secretName,
						Namespace: consoleNamespace,
						Key:       passwordKey,
					},
				},
				Prometheus: &v1alpha1.PrometheusConfig{
					Address: "address",
					Jobs: []v1alpha1.PrometheusScraperJobConfig{
						{
							JobName:    "job",
							KeepLabels: []string{"label"},
						},
					},
				},
			},
		}, false},
		{"missing job config", v1alpha1.CloudConfig{
			PrometheusEndpoint: &v1alpha1.PrometheusEndpointConfig{
				Enabled: true,
				BasicAuth: v1alpha1.BasicAuthConfig{
					Username: "username",
					PasswordRef: v1alpha1.SecretKeyRef{
						Name:      secretName,
						Namespace: consoleNamespace,
						Key:       passwordKey,
					},
				},
				Prometheus: &v1alpha1.PrometheusConfig{
					Address: "address",
				},
			},
		}, true},
		{"missing basic auth password", v1alpha1.CloudConfig{
			PrometheusEndpoint: &v1alpha1.PrometheusEndpointConfig{
				Enabled: true,
				BasicAuth: v1alpha1.BasicAuthConfig{
					Username: "username",
				},
				Prometheus: &v1alpha1.PrometheusConfig{
					Address: "address",
					Jobs: []v1alpha1.PrometheusScraperJobConfig{
						{
							JobName:    "job",
							KeepLabels: []string{"label"},
						},
					},
				},
			},
		}, true},
		{"nonexistent secret", v1alpha1.CloudConfig{
			PrometheusEndpoint: &v1alpha1.PrometheusEndpointConfig{
				Enabled: true,
				BasicAuth: v1alpha1.BasicAuthConfig{
					Username: "username",
					PasswordRef: v1alpha1.SecretKeyRef{
						Name:      "nonexisting",
						Namespace: consoleNamespace,
						Key:       passwordKey,
					},
				},
				Prometheus: &v1alpha1.PrometheusConfig{
					Address: "address",
					Jobs: []v1alpha1.PrometheusScraperJobConfig{
						{
							JobName:    "job",
							KeepLabels: []string{"label"},
						},
					},
				},
			},
		}, true},
		{"wrong basic auth secret key", v1alpha1.CloudConfig{
			PrometheusEndpoint: &v1alpha1.PrometheusEndpointConfig{
				Enabled: true,
				BasicAuth: v1alpha1.BasicAuthConfig{
					Username: "username",
					PasswordRef: v1alpha1.SecretKeyRef{
						Name:      secretName,
						Namespace: consoleNamespace,
						Key:       "nonexistingkey",
					},
				},
				Prometheus: &v1alpha1.PrometheusConfig{
					Address: "address",
					Jobs: []v1alpha1.PrometheusScraperJobConfig{
						{
							JobName:    "job",
							KeepLabels: []string{"label"},
						},
					},
				},
			},
		}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errs, err := redpanda.ValidatePrometheus(context.TODO(), ctl, &v1alpha1.Console{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "console",
					Namespace: consoleNamespace,
				},
				Spec: v1alpha1.ConsoleSpec{
					Cloud: &tt.cloudConfig,
				},
			})
			require.NoError(t, err)
			if tt.expectedValidationError {
				require.NotEmpty(t, errs)
			} else {
				require.Empty(t, errs)
			}
		})
	}
}
