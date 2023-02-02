package console

import (
	"context"
	"testing"

	"github.com/go-logr/logr"
	redpandav1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestGenEnvVars(t *testing.T) { //nolint:funlen // test table is long
	kakfaEnableAuth := true
	table := []struct {
		consoleSpec    redpandav1alpha1.ConsoleSpec
		clusterSpec    redpandav1alpha1.ClusterSpec
		expectedEnvars []string
	}{
		{
			consoleSpec: redpandav1alpha1.ConsoleSpec{
				Cloud: &redpandav1alpha1.CloudConfig{
					PrometheusEndpoint: &redpandav1alpha1.PrometheusEndpointConfig{
						Enabled: true,
						BasicAuth: redpandav1alpha1.BasicAuthConfig{
							PasswordRef: redpandav1alpha1.SecretKeyRef{
								Name: "any",
								Key:  "any",
							},
						},
					},
				},
			},
			clusterSpec: redpandav1alpha1.ClusterSpec{
				KafkaEnableAuthorization: &kakfaEnableAuth,
				Configuration: redpandav1alpha1.RedpandaConfig{
					SchemaRegistry: &redpandav1alpha1.SchemaRegistryAPI{
						AuthenticationMethod: "http_basic",
					},
				},
			},
			expectedEnvars: []string{
				kafkaSASLBasicAuthPasswordEnvVar,
				schemaRegistryBasicAuthPasswordEnvVar,
				prometheusBasicAuthPasswordEnvVar,
			},
		},
		{
			consoleSpec: redpandav1alpha1.ConsoleSpec{},
			clusterSpec: redpandav1alpha1.ClusterSpec{
				KafkaEnableAuthorization: &kakfaEnableAuth,
			},
			expectedEnvars: []string{
				kafkaSASLBasicAuthPasswordEnvVar,
			},
		},
		{
			consoleSpec: redpandav1alpha1.ConsoleSpec{},
			clusterSpec: redpandav1alpha1.ClusterSpec{
				Configuration: redpandav1alpha1.RedpandaConfig{
					SchemaRegistry: &redpandav1alpha1.SchemaRegistryAPI{
						AuthenticationMethod: "http_basic",
					},
				},
			},
			expectedEnvars: []string{
				schemaRegistryBasicAuthPasswordEnvVar,
			},
		},
		{
			consoleSpec: redpandav1alpha1.ConsoleSpec{
				Cloud: &redpandav1alpha1.CloudConfig{
					PrometheusEndpoint: &redpandav1alpha1.PrometheusEndpointConfig{
						Enabled: true,
						BasicAuth: redpandav1alpha1.BasicAuthConfig{
							PasswordRef: redpandav1alpha1.SecretKeyRef{
								Name: "any",
								Key:  "any",
							},
						},
					},
				},
			},
			clusterSpec: redpandav1alpha1.ClusterSpec{},
			expectedEnvars: []string{
				prometheusBasicAuthPasswordEnvVar,
			},
		},
	}

	nsn := metav1.ObjectMeta{
		Name:      "any",
		Namespace: "default",
	}
	for _, tt := range table {
		console := &redpandav1alpha1.Console{
			ObjectMeta: nsn,
			Spec:       tt.consoleSpec,
		}
		cluster := &redpandav1alpha1.Cluster{
			ObjectMeta: nsn,
			Spec:       tt.clusterSpec,
		}
		deployment := NewDeployment(nil, nil, console, cluster, nil, logr.Discard())
		genEnvVars, err := deployment.genEnvVars(context.Background())
		assert.NoError(t, err)

		for _, e := range tt.expectedEnvars {
			var found bool
			for _, envar := range genEnvVars {
				if envar.Name == e {
					found = true
				}
			}
			require.True(t, found, "expected envar %s, but not found", e)
		}
		require.Equal(t, len(tt.expectedEnvars), len(genEnvVars), "expected envars generated is %d, found %d", len(tt.expectedEnvars), len(genEnvVars))
	}
}
