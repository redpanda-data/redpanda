package redpanda_test

import (
	"context"
	"testing"

	"github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	"github.com/redpanda-data/redpanda/src/go/k8s/webhooks/redpanda"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

//nolint:funlen // this is table driven test
func TestValidatePrometheus(t *testing.T) {
	client := fake.NewClientBuilder().Build()
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
	require.NoError(t, client.Create(context.TODO(), &secret))

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
			errs, err := redpanda.ValidatePrometheus(context.TODO(), client, &v1alpha1.Console{
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
