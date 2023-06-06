package console

import (
	"context"
	"testing"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	vectorizedv1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/vectorized/v1alpha1"
)

func TestGenEnvVars(t *testing.T) { //nolint:funlen // test table is long
	kakfaEnableAuth := true
	table := []struct {
		consoleSpec    vectorizedv1alpha1.ConsoleSpec
		clusterSpec    vectorizedv1alpha1.ClusterSpec
		expectedEnvars []string
	}{
		{
			consoleSpec: vectorizedv1alpha1.ConsoleSpec{
				Cloud: &vectorizedv1alpha1.CloudConfig{
					PrometheusEndpoint: &vectorizedv1alpha1.PrometheusEndpointConfig{
						Enabled: true,
						BasicAuth: vectorizedv1alpha1.BasicAuthConfig{
							PasswordRef: vectorizedv1alpha1.SecretKeyRef{
								Name: "any",
								Key:  "any",
							},
						},
					},
				},
			},
			clusterSpec: vectorizedv1alpha1.ClusterSpec{
				KafkaEnableAuthorization: &kakfaEnableAuth,
				Configuration: vectorizedv1alpha1.RedpandaConfig{
					SchemaRegistry: &vectorizedv1alpha1.SchemaRegistryAPI{
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
			consoleSpec: vectorizedv1alpha1.ConsoleSpec{},
			clusterSpec: vectorizedv1alpha1.ClusterSpec{
				KafkaEnableAuthorization: &kakfaEnableAuth,
			},
			expectedEnvars: []string{
				kafkaSASLBasicAuthPasswordEnvVar,
			},
		},
		{
			consoleSpec: vectorizedv1alpha1.ConsoleSpec{},
			clusterSpec: vectorizedv1alpha1.ClusterSpec{
				Configuration: vectorizedv1alpha1.RedpandaConfig{
					SchemaRegistry: &vectorizedv1alpha1.SchemaRegistryAPI{
						AuthenticationMethod: "http_basic",
					},
				},
			},
			expectedEnvars: []string{
				schemaRegistryBasicAuthPasswordEnvVar,
			},
		},
		{
			consoleSpec: vectorizedv1alpha1.ConsoleSpec{
				Cloud: &vectorizedv1alpha1.CloudConfig{
					PrometheusEndpoint: &vectorizedv1alpha1.PrometheusEndpointConfig{
						Enabled: true,
						BasicAuth: vectorizedv1alpha1.BasicAuthConfig{
							PasswordRef: vectorizedv1alpha1.SecretKeyRef{
								Name: "any",
								Key:  "any",
							},
						},
					},
				},
			},
			clusterSpec: vectorizedv1alpha1.ClusterSpec{},
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
		console := &vectorizedv1alpha1.Console{
			ObjectMeta: nsn,
			Spec:       tt.consoleSpec,
		}
		cluster := &vectorizedv1alpha1.Cluster{
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
