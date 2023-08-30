package console

import (
	"context"
	"reflect"
	"testing"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	vectorizedv1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/vectorized/v1alpha1"
	"k8s.io/apimachinery/pkg/types"
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

func TestDeployment_getServiceAccountKey(t *testing.T) {
	const (
		testNamespace   = "betelgeuse"
		testConsoleName = "deepthought"
	)
	testSAName := "slartibartfast"
	testSAEmpty := ""
	type fields struct {
		consoleobj *vectorizedv1alpha1.Console
	}
	tests := []struct {
		name   string
		fields fields
		want   types.NamespacedName
	}{
		{
			name: "Spec.ServiceAccount is set",
			fields: fields{
				consoleobj: &vectorizedv1alpha1.Console{
					ObjectMeta: metav1.ObjectMeta{
						Name:      testConsoleName,
						Namespace: testNamespace,
					},
					Spec: vectorizedv1alpha1.ConsoleSpec{
						ServiceAccount: &testSAName,
					},
				},
			},
			want: types.NamespacedName{
				Name:      testSAName,
				Namespace: testNamespace,
			},
		},
		{
			name: "Spec.ServiceAccount is set but empty",
			fields: fields{
				consoleobj: &vectorizedv1alpha1.Console{
					ObjectMeta: metav1.ObjectMeta{
						Name:      testConsoleName,
						Namespace: testNamespace,
					},
					Spec: vectorizedv1alpha1.ConsoleSpec{
						ServiceAccount: &testSAEmpty,
					},
				},
			},
			want: types.NamespacedName{
				Name:      testConsoleName,
				Namespace: testNamespace,
			},
		},
		{
			name: "Spec.ServiceAccount is nil",
			fields: fields{
				consoleobj: &vectorizedv1alpha1.Console{
					ObjectMeta: metav1.ObjectMeta{
						Name:      testConsoleName,
						Namespace: testNamespace,
					},
				},
			},
			want: types.NamespacedName{
				Name:      testConsoleName,
				Namespace: testNamespace,
			},
		},

		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &Deployment{
				consoleobj: tt.fields.consoleobj,
			}
			if got := d.getServiceAccountKey(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Deployment.getServiceAccountKey() = %v, want %v", got, tt.want)
			}
		})
	}
}
