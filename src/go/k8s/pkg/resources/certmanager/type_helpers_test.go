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

	cmapiv1 "github.com/cert-manager/cert-manager/pkg/apis/certmanager/v1"
	cmmetav1 "github.com/cert-manager/cert-manager/pkg/apis/meta/v1"
	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources/certmanager"
	resourcetypes "github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources/types"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
)

//nolint:funlen // the subtests might causes linter to complain
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
	issuer := cmapiv1.Issuer{
		ObjectMeta: v1.ObjectMeta{
			Name:      "issuer",
			Namespace: "test",
		},
		Spec: cmapiv1.IssuerSpec{
			IssuerConfig: cmapiv1.IssuerConfig{
				SelfSigned: nil,
			},
		},
	}
	tests := []struct {
		name              string
		pandaCluster      *v1alpha1.Cluster
		expectedNames     []string
		volumesCount      int
		verifyVolumes     func(vols []corev1.Volume) bool
		expectedBrokerTLS *config.ServerTLS
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
		}, []string{}, 0, nil, nil},
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
		}, []string{"test-kafka-selfsigned-issuer", "test-kafka-root-certificate", "test-kafka-root-issuer", "test-redpanda"}, 1, func(vols []corev1.Volume) bool {
			// verify the volume also contains CA in the node tls folder
			for _, i := range vols[0].Secret.Items {
				if i.Key == cmmetav1.TLSCAKey {
					return true
				}
			}
			return false
		}, &config.ServerTLS{
			Enabled:        true,
			TruststoreFile: "/etc/tls/certs/ca.crt",
		}},
		{"kafka tls on external only", &v1alpha1.Cluster{
			ObjectMeta: v1.ObjectMeta{Name: "test", Namespace: "test"},
			Spec: v1alpha1.ClusterSpec{
				Configuration: v1alpha1.RedpandaConfig{
					KafkaAPI: []v1alpha1.KafkaAPI{
						{
							External: v1alpha1.ExternalConnectivityConfig{
								Enabled: true,
							},
							TLS: v1alpha1.KafkaAPITLS{
								Enabled: true,
							},
						},
					},
				},
			},
		}, []string{"test-kafka-selfsigned-issuer", "test-kafka-root-certificate", "test-kafka-root-issuer", "test-redpanda"}, 1, func(vols []corev1.Volume) bool {
			return true
		}, nil},
		{"kafka tls with two tls listeners", &v1alpha1.Cluster{
			ObjectMeta: v1.ObjectMeta{Name: "test", Namespace: "test"},
			Spec: v1alpha1.ClusterSpec{
				Configuration: v1alpha1.RedpandaConfig{
					KafkaAPI: []v1alpha1.KafkaAPI{
						{
							TLS: v1alpha1.KafkaAPITLS{
								Enabled: true,
							},
						},
						{
							External: v1alpha1.ExternalConnectivityConfig{
								Enabled: true,
							},
							TLS: v1alpha1.KafkaAPITLS{
								Enabled: true,
							},
						},
					},
				},
			},
		}, []string{"test-kafka-selfsigned-issuer", "test-kafka-root-certificate", "test-kafka-root-issuer", "test-redpanda"}, 1, nil, &config.ServerTLS{
			Enabled:        true,
			TruststoreFile: "/etc/tls/certs/ca.crt",
		}},
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
									Kind: "Issuer",
								},
							},
						},
					},
				},
			},
		}, []string{"test-kafka-selfsigned-issuer", "test-kafka-root-certificate", "test-kafka-root-issuer", "test-redpanda"}, 1, func(vols []corev1.Volume) bool {
			// verify the volume does not contain CA in the node tls folder when node cert is injected
			for i, v := range vols {
				if v.Name == "tlscert" {
					for _, i := range vols[i].Secret.Items {
						if i.Key == cmmetav1.TLSCAKey {
							return false
						}
					}
				}
			}
			return true
		}, &config.ServerTLS{
			Enabled: true,
		}},
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
						{
							External: v1alpha1.ExternalConnectivityConfig{
								Enabled: true,
							},
							TLS: v1alpha1.KafkaAPITLS{
								Enabled: true,
							},
						},
					},
				},
			},
		}, []string{"test-kafka-selfsigned-issuer", "test-kafka-root-certificate", "test-kafka-root-issuer", "test-redpanda", "test-operator-client", "test-user-client", "test-admin-client"}, 2, func(vols []corev1.Volume) bool {
			// verify the ca volume contains also client cert
			foundKey := false
			foundCrt := false
			for i, v := range vols {
				if v.Name == "tlsca" {
					for _, i := range vols[i].Secret.Items {
						if i.Key == corev1.TLSCertKey {
							foundCrt = true
						}
						if i.Key == corev1.TLSPrivateKeyKey {
							foundKey = true
						}
						if foundKey && foundCrt {
							break
						}
					}
					break
				}
			}
			return foundCrt && foundKey
		}, &config.ServerTLS{
			Enabled:        true,
			KeyFile:        "/etc/tls/certs/ca/tls.key",
			CertFile:       "/etc/tls/certs/ca/tls.crt",
			TruststoreFile: "/etc/tls/certs/ca.crt",
		}},
		{"kafka mutual tls with two tls listeners", &v1alpha1.Cluster{
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
		}, []string{"test-kafka-selfsigned-issuer", "test-kafka-root-certificate", "test-kafka-root-issuer", "test-redpanda", "test-operator-client", "test-user-client", "test-admin-client"}, 2, nil, &config.ServerTLS{
			Enabled:        true,
			KeyFile:        "/etc/tls/certs/ca/tls.key",
			CertFile:       "/etc/tls/certs/ca/tls.crt",
			TruststoreFile: "/etc/tls/certs/ca.crt",
		}},
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
		}, []string{}, 0, nil, nil},
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
		}, []string{"test-admin-selfsigned-issuer", "test-admin-root-certificate", "test-admin-root-issuer", "test-admin-api-node"}, 1, nil, nil},
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
		}, []string{"test-admin-selfsigned-issuer", "test-admin-root-certificate", "test-admin-root-issuer", "test-admin-api-node", "test-admin-api-client"}, 2, nil, nil},
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
		}, []string{}, 0, nil, nil},
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
		}, []string{"test-proxy-selfsigned-issuer", "test-proxy-root-certificate", "test-proxy-root-issuer", "test-proxy-api-node"}, 1, nil, nil},
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
		}, []string{"test-proxy-selfsigned-issuer", "test-proxy-root-certificate", "test-proxy-root-issuer", "test-proxy-api-node", "test-proxy-api-client"}, 2, nil, nil},
		{
			"pandaproxy api mutual tls with external ca provided by customer",
			&v1alpha1.Cluster{
				ObjectMeta: v1.ObjectMeta{Name: "test", Namespace: "test"},
				Spec: v1alpha1.ClusterSpec{
					Configuration: v1alpha1.RedpandaConfig{
						PandaproxyAPI: []v1alpha1.PandaproxyAPI{
							{
								TLS: v1alpha1.PandaproxyAPITLS{
									Enabled:           true,
									RequireClientAuth: true,
									ClientCACertRef: &corev1.TypedLocalObjectReference{
										Name: "client-ca-secret",
									},
								},
							},
						},
					},
				},
			},
			[]string{"test-proxy-api-trusted-client-ca", "test-proxy-selfsigned-issuer", "test-proxy-root-certificate", "test-proxy-root-issuer", "test-proxy-api-node", "test-proxy-api-client"},
			2, nil, nil,
		},
		{
			"pandaproxy api mutual tls with external ca provided by customer and external node issuer",
			&v1alpha1.Cluster{
				ObjectMeta: v1.ObjectMeta{Name: "test", Namespace: "test"},
				Spec: v1alpha1.ClusterSpec{
					Configuration: v1alpha1.RedpandaConfig{
						PandaproxyAPI: []v1alpha1.PandaproxyAPI{
							{
								TLS: v1alpha1.PandaproxyAPITLS{
									Enabled:           true,
									RequireClientAuth: true,
									ClientCACertRef: &corev1.TypedLocalObjectReference{
										Name: "client-ca-secret",
									},
									IssuerRef: &cmmetav1.ObjectReference{
										Name: "issuer",
										Kind: "Issuer",
									},
								},
							},
						},
					},
				},
			},
			[]string{"test-proxy-api-trusted-client-ca", "test-proxy-selfsigned-issuer", "test-proxy-root-certificate", "test-proxy-root-issuer", "test-proxy-api-node", "test-proxy-api-client"},
			2, nil, nil,
		},
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
		}, []string{}, 0, nil, nil},
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
			1, nil, nil,
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
			2, nil, nil,
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
			4, nil, &config.ServerTLS{
				Enabled:        true,
				KeyFile:        "/etc/tls/certs/ca/tls.key",
				CertFile:       "/etc/tls/certs/ca/tls.crt",
				TruststoreFile: "/etc/tls/certs/ca.crt",
			},
		},
		{
			"schematregistry api mutual tls with external ca provided by customer", &v1alpha1.Cluster{
				ObjectMeta: v1.ObjectMeta{Name: "test", Namespace: "test"},
				Spec: v1alpha1.ClusterSpec{
					Configuration: v1alpha1.RedpandaConfig{
						SchemaRegistry: &v1alpha1.SchemaRegistryAPI{
							TLS: &v1alpha1.SchemaRegistryAPITLS{
								Enabled:           true,
								RequireClientAuth: true,
								ClientCACertRef: &corev1.TypedLocalObjectReference{
									Name: "client-ca-secret",
								},
							},
						},
					},
				},
			},
			[]string{"test-schema-registry-trusted-client-ca", "test-schema-registry-selfsigned-issuer", "test-schema-registry-root-certificate", "test-schema-registry-root-issuer", "test-schema-registry-node", "test-schema-registry-client"},
			2, nil, nil,
		},
		{
			"schematregistry api mutual tls with external ca provided by customer and external node issuer", &v1alpha1.Cluster{
				ObjectMeta: v1.ObjectMeta{Name: "test", Namespace: "test"},
				Spec: v1alpha1.ClusterSpec{
					Configuration: v1alpha1.RedpandaConfig{
						SchemaRegistry: &v1alpha1.SchemaRegistryAPI{
							TLS: &v1alpha1.SchemaRegistryAPITLS{
								Enabled:           true,
								RequireClientAuth: true,
								ClientCACertRef: &corev1.TypedLocalObjectReference{
									Name: "client-ca-secret",
								},
								IssuerRef: &cmmetav1.ObjectReference{
									Name: "issuer",
									Kind: "Issuer",
								},
							},
						},
					},
				},
			},
			[]string{"test-schema-registry-trusted-client-ca", "test-schema-registry-selfsigned-issuer", "test-schema-registry-root-certificate", "test-schema-registry-root-issuer", "test-schema-registry-node", "test-schema-registry-client"},
			2, nil, nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cc, err := certmanager.NewClusterCertificates(context.TODO(), tt.pandaCluster,
				types.NamespacedName{
					Name:      "test",
					Namespace: "test",
				}, fake.NewClientBuilder().WithRuntimeObjects(&secret, &issuer).Build(), "cluster.local", "cluster2.local", scheme.Scheme, logr.Discard())
			require.NoError(t, err)
			resources, err := cc.Resources(context.TODO())
			require.NoError(t, err)
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
			if tt.verifyVolumes != nil {
				require.True(t, tt.verifyVolumes(v), "failed during volumes verification")
			}
			brokerTLS := cc.KafkaClientBrokerTLS(resourcetypes.GetTLSMountPoints())
			require.Equal(t, tt.expectedBrokerTLS == nil, brokerTLS == nil)
			if tt.expectedBrokerTLS != nil && brokerTLS != nil {
				require.Equal(t, *tt.expectedBrokerTLS, *brokerTLS)
			}
		})
	}
}
