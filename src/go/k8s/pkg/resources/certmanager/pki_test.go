package certmanager_test

import (
	"testing"

	"github.com/go-logr/logr"
	cmmeta "github.com/jetstack/cert-manager/pkg/apis/meta/v1"
	"github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources/certmanager"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestSchemaRegistryCerts(t *testing.T) {
	testcases := []struct {
		name                   string
		cluster                v1alpha1.Cluster
		expectedClientCertName string
		expectedNodeCertName   string
	}{
		{
			name: "SchemaRegistry: When NodeSecretRef is used",
			cluster: v1alpha1.Cluster{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "redpanda",
					Name:      "fake-cluster",
				},
				Spec: v1alpha1.ClusterSpec{
					Configuration: v1alpha1.RedpandaConfig{
						SchemaRegistry: &v1alpha1.SchemaRegistryAPI{
							TLS: &v1alpha1.SchemaRegistryAPITLS{
								Enabled: true,
								NodeSecretRef: &v1.ObjectReference{
									Namespace: "other-namespace",
									Name:      "custom-secret-ref",
								},
							},
						},
					},
				},
			},
			expectedClientCertName: "fake-cluster-schema-registry-client",
			expectedNodeCertName:   "custom-secret-ref",
		},
		{
			name: "SchemaRegistry: When IssuerRef is used",
			cluster: v1alpha1.Cluster{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "redpanda",
					Name:      "fake-cluster",
				},
				Spec: v1alpha1.ClusterSpec{
					Configuration: v1alpha1.RedpandaConfig{
						SchemaRegistry: &v1alpha1.SchemaRegistryAPI{
							TLS: &v1alpha1.SchemaRegistryAPITLS{
								Enabled: true,
								IssuerRef: &cmmeta.ObjectReference{
									Group: "certmanager",
									Kind:  "ClusterIssuer",
									Name:  "cluster-issuer",
								},
							},
						},
					},
				},
			},
			expectedClientCertName: "fake-cluster-schema-registry-client",
			expectedNodeCertName:   "fake-cluster-schema-registry-node",
		},
		{
			// TODO(#3550): refactor how methods used to get APIs node and client certificates behiave when
			// TLS ot mTLS are disabled. Currently they always return a NamespacedName pointing to the a
			// default certificate but that certificate doesn't exist.
			// A better approach might be to return a certificate or secret object which could be nil in case
			// TLS or mTLS are disabled.
			name: "SchemaRegistry: When TLS and mTLS are disabled",
			cluster: v1alpha1.Cluster{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "redpanda",
					Name:      "fake-cluster-with-no-mtls",
				},
				Spec: v1alpha1.ClusterSpec{
					Configuration: v1alpha1.RedpandaConfig{
						SchemaRegistry: &v1alpha1.SchemaRegistryAPI{
							TLS: &v1alpha1.SchemaRegistryAPITLS{
								RequireClientAuth: false,
								Enabled:           false,
							},
						},
					},
				},
			},
			expectedClientCertName: "fake-cluster-with-no-mtls-schema-registry-client",
			expectedNodeCertName:   "fake-cluster-with-no-mtls-schema-registry-node",
		},
	}

	for _, tt := range testcases {
		t.Run(tt.name, func(t *testing.T) {
			pki := certmanager.NewPki(nil, &tt.cluster, "FQDN", "clusterFQDN", nil, logr.Discard())
			// Client cert
			clientCert := pki.SchemaRegistryAPIClientCert()
			require.NotNil(t, clientCert)
			require.Equal(t, tt.expectedClientCertName, clientCert.Name)
			require.Equal(t, tt.cluster.ObjectMeta.Namespace, clientCert.Namespace)

			// Node cert
			nodeCert := pki.SchemaRegistryAPINodeCert()
			require.NotNil(t, nodeCert)
			require.Equal(t, tt.expectedNodeCertName, nodeCert.Name)
			require.Equal(t, tt.cluster.ObjectMeta.Namespace, nodeCert.Namespace)
		})
	}
}
