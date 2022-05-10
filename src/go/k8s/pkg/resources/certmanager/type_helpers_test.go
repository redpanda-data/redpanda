// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package certmanager_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/go-logr/logr"
	cmmetav1 "github.com/jetstack/cert-manager/pkg/apis/meta/v1"
	"github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources/certmanager"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// nolint:funlen // the subtests might causes linter to complain
func TestClusterCertificates(t *testing.T) {
	secret := corev1.Secret{
		ObjectMeta: v1.ObjectMeta{
			Name:      "cluster-tls-secret-node-certificate",
			Namespace: "cert-manager",
		},
		Data: map[string][]byte{
			"tls.crt": []byte("XXX"),
			"tls.key": []byte("XXX"),
			"ca.crt":  []byte("XXX"),
		},
	}
	tests := []struct {
		name          string
		pandaCluster  *v1alpha1.Cluster
		expectedNames []string
		volumesCount  int
	}{
		{"kafka tls disabled", &v1alpha1.Cluster{
			ObjectMeta: v1.ObjectMeta{Name: "test", Namespace: "test"},
			Spec: v1alpha1.ClusterSpec{
				Configuration: v1alpha1.RedpandaConfig{
					KafkaAPI: []v1alpha1.KafkaAPI{
						{
							TLS: v1alpha1.KafkaAPITLS{
								Enabled: false,
							},
						},
					},
				},
			},
		}, []string{}, 0},
		{"kafka tls", &v1alpha1.Cluster{
			ObjectMeta: v1.ObjectMeta{Name: "test", Namespace: "test"},
			Spec: v1alpha1.ClusterSpec{
				Configuration: v1alpha1.RedpandaConfig{
					KafkaAPI: []v1alpha1.KafkaAPI{
						{
							TLS: v1alpha1.KafkaAPITLS{
								Enabled: true,
							},
						},
					},
				},
			},
		}, []string{"test-kafka-selfsigned-issuer", "test-kafka-root-certificate", "test-kafka-root-issuer", "test-redpanda"}, 1},
		{"kafka tls with external node issuer", &v1alpha1.Cluster{
			ObjectMeta: v1.ObjectMeta{Name: "test", Namespace: "test"},
			Spec: v1alpha1.ClusterSpec{
				Configuration: v1alpha1.RedpandaConfig{
					KafkaAPI: []v1alpha1.KafkaAPI{
						{
							TLS: v1alpha1.KafkaAPITLS{
								Enabled: true,
								IssuerRef: &cmmetav1.ObjectReference{
									Name: "issuer",
								},
							},
						},
					},
				},
			},
		}, []string{"test-kafka-selfsigned-issuer", "test-kafka-root-certificate", "test-kafka-root-issuer", "test-redpanda"}, 1},
		{"kafka mutual tls", &v1alpha1.Cluster{
			ObjectMeta: v1.ObjectMeta{Name: "test", Namespace: "test"},
			Spec: v1alpha1.ClusterSpec{
				Configuration: v1alpha1.RedpandaConfig{
					KafkaAPI: []v1alpha1.KafkaAPI{
						{
							TLS: v1alpha1.KafkaAPITLS{
								Enabled:           true,
								RequireClientAuth: true,
							},
						},
					},
				},
			},
		}, []string{"test-kafka-selfsigned-issuer", "test-kafka-root-certificate", "test-kafka-root-issuer", "test-redpanda", "test-operator-client", "test-user-client", "test-admin-client"}, 2},
		{"admin api tls disabled", &v1alpha1.Cluster{
			ObjectMeta: v1.ObjectMeta{Name: "test", Namespace: "test"},
			Spec: v1alpha1.ClusterSpec{
				Configuration: v1alpha1.RedpandaConfig{
					AdminAPI: []v1alpha1.AdminAPI{
						{
							TLS: v1alpha1.AdminAPITLS{
								Enabled: false,
							},
						},
					},
				},
			},
		}, []string{}, 0},
		{"admin api tls", &v1alpha1.Cluster{
			ObjectMeta: v1.ObjectMeta{Name: "test", Namespace: "test"},
			Spec: v1alpha1.ClusterSpec{
				Configuration: v1alpha1.RedpandaConfig{
					AdminAPI: []v1alpha1.AdminAPI{
						{
							TLS: v1alpha1.AdminAPITLS{
								Enabled: true,
							},
						},
					},
				},
			},
		}, []string{"test-admin-selfsigned-issuer", "test-admin-root-certificate", "test-admin-root-issuer", "test-admin-api-node"}, 1},
		{"admin api mutual tls", &v1alpha1.Cluster{
			ObjectMeta: v1.ObjectMeta{Name: "test", Namespace: "test"},
			Spec: v1alpha1.ClusterSpec{
				Configuration: v1alpha1.RedpandaConfig{
					AdminAPI: []v1alpha1.AdminAPI{
						{
							TLS: v1alpha1.AdminAPITLS{
								Enabled:           true,
								RequireClientAuth: true,
							},
						},
					},
				},
			},
		}, []string{"test-admin-selfsigned-issuer", "test-admin-root-certificate", "test-admin-root-issuer", "test-admin-api-node", "test-admin-api-client"}, 2},
		{"pandaproxy api tls disabled", &v1alpha1.Cluster{
			ObjectMeta: v1.ObjectMeta{Name: "test", Namespace: "test"},
			Spec: v1alpha1.ClusterSpec{
				Configuration: v1alpha1.RedpandaConfig{
					PandaproxyAPI: []v1alpha1.PandaproxyAPI{
						{
							TLS: v1alpha1.PandaproxyAPITLS{
								Enabled: false,
							},
						},
					},
				},
			},
		}, []string{}, 0},
		{"pandaproxy api tls", &v1alpha1.Cluster{
			ObjectMeta: v1.ObjectMeta{Name: "test", Namespace: "test"},
			Spec: v1alpha1.ClusterSpec{
				Configuration: v1alpha1.RedpandaConfig{
					PandaproxyAPI: []v1alpha1.PandaproxyAPI{
						{
							TLS: v1alpha1.PandaproxyAPITLS{
								Enabled: true,
							},
						},
					},
				},
			},
		}, []string{"test-proxy-selfsigned-issuer", "test-proxy-root-certificate", "test-proxy-root-issuer", "test-proxy-api-node"}, 1},
		{"pandaproxy api mutual tls", &v1alpha1.Cluster{
			ObjectMeta: v1.ObjectMeta{Name: "test", Namespace: "test"},
			Spec: v1alpha1.ClusterSpec{
				Configuration: v1alpha1.RedpandaConfig{
					PandaproxyAPI: []v1alpha1.PandaproxyAPI{
						{
							TLS: v1alpha1.PandaproxyAPITLS{
								Enabled:           true,
								RequireClientAuth: true,
							},
						},
					},
				},
			},
		}, []string{"test-proxy-selfsigned-issuer", "test-proxy-root-certificate", "test-proxy-root-issuer", "test-proxy-api-node", "test-proxy-api-client"}, 2},
		{"schematregistry api tls disabled", &v1alpha1.Cluster{
			ObjectMeta: v1.ObjectMeta{Name: "test", Namespace: "test"},
			Spec: v1alpha1.ClusterSpec{
				Configuration: v1alpha1.RedpandaConfig{
					SchemaRegistry: &v1alpha1.SchemaRegistryAPI{
						TLS: &v1alpha1.SchemaRegistryAPITLS{
							Enabled: false,
						},
					},
				},
			},
		}, []string{}, 0},
		{
			"schematregistry api tls", &v1alpha1.Cluster{
				ObjectMeta: v1.ObjectMeta{Name: "test", Namespace: "test"},
				Spec: v1alpha1.ClusterSpec{
					Configuration: v1alpha1.RedpandaConfig{
						SchemaRegistry: &v1alpha1.SchemaRegistryAPI{
							TLS: &v1alpha1.SchemaRegistryAPITLS{
								Enabled: true,
							},
						},
					},
				},
			},
			[]string{"test-schema-registry-selfsigned-issuer", "test-schema-registry-root-certificate", "test-schema-registry-root-issuer", "test-schema-registry-node"},
			1,
		},
		{
			"schematregistry api mutual tls", &v1alpha1.Cluster{
				ObjectMeta: v1.ObjectMeta{Name: "test", Namespace: "test"},
				Spec: v1alpha1.ClusterSpec{
					Configuration: v1alpha1.RedpandaConfig{
						SchemaRegistry: &v1alpha1.SchemaRegistryAPI{
							TLS: &v1alpha1.SchemaRegistryAPITLS{
								Enabled:           true,
								RequireClientAuth: true,
							},
						},
					},
				},
			},
			[]string{"test-schema-registry-selfsigned-issuer", "test-schema-registry-root-certificate", "test-schema-registry-root-issuer", "test-schema-registry-node", "test-schema-registry-client"},
			2,
		},
		{
			"kafka and schematregistry with nodesecretref", &v1alpha1.Cluster{
				ObjectMeta: v1.ObjectMeta{Name: "test", Namespace: "test"},
				Spec: v1alpha1.ClusterSpec{
					Configuration: v1alpha1.RedpandaConfig{
						SchemaRegistry: &v1alpha1.SchemaRegistryAPI{
							TLS: &v1alpha1.SchemaRegistryAPITLS{
								Enabled:           true,
								RequireClientAuth: true,
								NodeSecretRef: &corev1.ObjectReference{
									Name:      secret.Name,
									Namespace: secret.Namespace,
								},
							},
						},
						KafkaAPI: []v1alpha1.KafkaAPI{
							{
								TLS: v1alpha1.KafkaAPITLS{
									Enabled:           true,
									RequireClientAuth: true,
									NodeSecretRef: &corev1.ObjectReference{
										Name:      secret.Name,
										Namespace: secret.Namespace,
									},
								},
							},
						},
					},
				},
			},
			[]string{"test-kafka-selfsigned-issuer", "test-kafka-root-certificate", "test-kafka-root-issuer", "test-operator-client", "test-user-client", "test-admin-client", "test-schema-registry-selfsigned-issuer", "test-schema-registry-root-certificate", "test-schema-registry-root-issuer", "test-schema-registry-client"},
			4,
		},
	}
	for _, tt := range tests {
		cc := certmanager.NewClusterCertificates(tt.pandaCluster,
			types.NamespacedName{
				Name:      "test",
				Namespace: "test",
			}, fake.NewClientBuilder().WithRuntimeObjects(&secret).Build(), "cluster.local", "cluster2.local", scheme.Scheme, logr.DiscardLogger{})
		resources, err := cc.Resources(context.TODO())
		require.NoError(t, err)
		for _, r := range resources {
			fmt.Println(r.Key().Name)
		}
		require.Equal(t, len(tt.expectedNames), len(resources))
		for _, n := range tt.expectedNames {
			found := false
			for _, r := range resources {
				if r.Key().Name == n {
					found = true
					break
				}
			}
			require.True(t, found, fmt.Sprintf("name %s not found in resources", n))
		}
		v, vm := cc.Volumes()
		require.Equal(t, tt.volumesCount, len(v), fmt.Sprintf("%s: volumes count don't match", tt.name))
		require.Equal(t, tt.volumesCount, len(vm), fmt.Sprintf("%s: volume mounts count don't match", tt.name))
	}
}
