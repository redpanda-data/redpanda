// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package v1alpha1_test

import (
	"testing"

	cmmeta "github.com/jetstack/cert-manager/pkg/apis/meta/v1"
	"github.com/stretchr/testify/assert"
	"github.com/vectorizedio/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/utils/pointer"
)

// nolint:funlen // this is ok for a test
func TestDefault(t *testing.T) {
	type test struct {
		name                                string
		replicas                            int32
		additionalConfigurationSetByWebhook bool
		configAlreadyPresent                bool
	}
	tests := []test{
		{
			name:                                "do not set default topic replication when there is less than 3 replicas",
			replicas:                            2,
			additionalConfigurationSetByWebhook: false,
		},
		{
			name:                                "sets default topic replication",
			replicas:                            3,
			additionalConfigurationSetByWebhook: true,
		},
		{
			name:                                "does not set default topic replication when it already exists in CRD",
			replicas:                            3,
			additionalConfigurationSetByWebhook: false,
			configAlreadyPresent:                true,
		},
	}
	fields := []string{"redpanda.default_topic_replications", "redpanda.transaction_coordinator_replication", "redpanda.id_allocator_replication"}
	for _, tt := range tests {
		for _, field := range fields {
			t.Run(tt.name, func(t *testing.T) {
				redpandaCluster := &v1alpha1.Cluster{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test",
						Namespace: "",
					},
					Spec: v1alpha1.ClusterSpec{
						Replicas:      pointer.Int32Ptr(tt.replicas),
						Configuration: v1alpha1.RedpandaConfig{},
					},
				}

				if tt.configAlreadyPresent {
					redpandaCluster.Spec.AdditionalConfiguration = make(map[string]string)
					redpandaCluster.Spec.AdditionalConfiguration[field] = "111"
				}

				redpandaCluster.Default()
				val, exist := redpandaCluster.Spec.AdditionalConfiguration[field]
				if (exist && val == "3") != tt.additionalConfigurationSetByWebhook {
					t.Fail()
				}
			})
		}
	}

	t.Run("missing schema registry does not set default port", func(t *testing.T) {
		redpandaCluster := &v1alpha1.Cluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test",
				Namespace: "",
			},
			Spec: v1alpha1.ClusterSpec{
				Replicas:      pointer.Int32Ptr(1),
				Configuration: v1alpha1.RedpandaConfig{},
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceMemory: resource.MustParse("1Gi"),
					},
				},
			},
		}

		redpandaCluster.Default()
		assert.Nil(t, redpandaCluster.Spec.Configuration.SchemaRegistry)
	})
	t.Run("if schema registry exist, but the port is 0 the default is set", func(t *testing.T) {
		redpandaCluster := &v1alpha1.Cluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test",
				Namespace: "",
			},
			Spec: v1alpha1.ClusterSpec{
				Replicas: pointer.Int32Ptr(1),
				Configuration: v1alpha1.RedpandaConfig{
					SchemaRegistry: &v1alpha1.SchemaRegistryAPI{},
				},
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceMemory: resource.MustParse("1Gi"),
					},
				},
			},
		}

		redpandaCluster.Default()
		assert.Equal(t, 8081, redpandaCluster.Spec.Configuration.SchemaRegistry.Port)
	})
	t.Run("if schema registry exist and port have not zero value the default will not be used", func(t *testing.T) {
		redpandaCluster := &v1alpha1.Cluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test",
				Namespace: "",
			},
			Spec: v1alpha1.ClusterSpec{
				Replicas: pointer.Int32Ptr(1),
				Configuration: v1alpha1.RedpandaConfig{
					SchemaRegistry: &v1alpha1.SchemaRegistryAPI{
						Port: 999,
					},
				},
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceMemory: resource.MustParse("1Gi"),
					},
				},
			},
		}

		redpandaCluster.Default()
		assert.Equal(t, 999, redpandaCluster.Spec.Configuration.SchemaRegistry.Port)
	})
	t.Run("if schema registry is defined as rest of external listeners the default port is used", func(t *testing.T) {
		redpandaCluster := &v1alpha1.Cluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test",
				Namespace: "",
			},
			Spec: v1alpha1.ClusterSpec{
				Replicas: pointer.Int32Ptr(1),
				Configuration: v1alpha1.RedpandaConfig{
					SchemaRegistry: &v1alpha1.SchemaRegistryAPI{
						External: &v1alpha1.ExternalConnectivityConfig{Enabled: true},
					},
				},
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceMemory: resource.MustParse("1Gi"),
					},
				},
			},
		}

		redpandaCluster.Default()
		assert.Equal(t, 8081, redpandaCluster.Spec.Configuration.SchemaRegistry.Port)
	})
}

func TestValidateUpdate(t *testing.T) {
	var replicas1 int32 = 1
	var replicas2 int32 = 2

	redpandaCluster := &v1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test",
			Namespace: "",
		},
		Spec: v1alpha1.ClusterSpec{
			Replicas:      pointer.Int32Ptr(replicas2),
			Configuration: v1alpha1.RedpandaConfig{},
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("1"),
					corev1.ResourceMemory: resource.MustParse("0.9Gi"),
				},
			},
		},
	}

	updatedCluster := redpandaCluster.DeepCopy()
	updatedCluster.Spec.Replicas = &replicas1
	updatedCluster.Spec.Configuration = v1alpha1.RedpandaConfig{
		KafkaAPI: []v1alpha1.KafkaAPI{
			{Port: 123,
				TLS: v1alpha1.KafkaAPITLS{
					RequireClientAuth: true,
					IssuerRef: &cmmeta.ObjectReference{
						Name: "test",
					},
					NodeSecretRef: &corev1.ObjectReference{
						Name:      "name",
						Namespace: "default",
					},
					Enabled: false,
				},
			},
		},
	}

	err := updatedCluster.ValidateUpdate(redpandaCluster)
	if err == nil {
		t.Fatalf("expecting validation error but got none")
	}

	// verify the error causes contain all expected fields
	statusError := err.(*apierrors.StatusError)
	expectedFields := []string{
		field.NewPath("spec").Child("replicas").String(),
		field.NewPath("spec").Child("resources").Child("requests").Child("memory").String(),
		field.NewPath("spec").Child("configuration").Child("kafkaApi").Index(0).Child("tls").Child("requireclientauth").String(),
		field.NewPath("spec").Child("configuration").Child("kafkaApi").Index(0).Child("tls").Child("nodeSecretRef").String(),
	}

	for _, ef := range expectedFields {
		found := false
		for _, c := range statusError.Status().Details.Causes {
			if ef == c.Field {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("expecting failure on field %s but have %v", ef, statusError.Status().Details.Causes)
		}
	}
}

//nolint:funlen // this is ok for a test
func TestValidateUpdate_NoError(t *testing.T) {
	var replicas2 int32 = 2

	redpandaCluster := &v1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test",
			Namespace: "",
		},
		Spec: v1alpha1.ClusterSpec{
			Replicas: pointer.Int32Ptr(replicas2),
			Configuration: v1alpha1.RedpandaConfig{
				KafkaAPI:       []v1alpha1.KafkaAPI{{Port: 124}},
				AdminAPI:       []v1alpha1.AdminAPI{{Port: 125}},
				RPCServer:      v1alpha1.SocketAddress{Port: 126},
				SchemaRegistry: &v1alpha1.SchemaRegistryAPI{Port: 127},
				PandaproxyAPI:  []v1alpha1.PandaproxyAPI{{Port: 128}},
			},
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceMemory: resource.MustParse("2Gi"),
					corev1.ResourceCPU:    resource.MustParse("1"),
				},
				Limits: corev1.ResourceList{
					corev1.ResourceMemory: resource.MustParse("2Gi"),
					corev1.ResourceCPU:    resource.MustParse("1"),
				},
			},
		},
	}

	t.Run("same object updated", func(t *testing.T) {
		err := redpandaCluster.ValidateUpdate(redpandaCluster)
		assert.NoError(t, err)
	})

	t.Run("scale up", func(t *testing.T) {
		var scaleUp int32 = *redpandaCluster.Spec.Replicas + 1
		updatedScaleUp := redpandaCluster.DeepCopy()
		updatedScaleUp.Spec.Replicas = &scaleUp
		err := updatedScaleUp.ValidateUpdate(redpandaCluster)
		assert.NoError(t, err)
	})

	t.Run("change image and tag", func(t *testing.T) {
		updatedImage := redpandaCluster.DeepCopy()
		updatedImage.Spec.Image = "differentimage"
		updatedImage.Spec.Version = "111"
		err := updatedImage.ValidateUpdate(redpandaCluster)
		assert.NoError(t, err)
	})

	t.Run("collision in the port", func(t *testing.T) {
		updatePort := redpandaCluster.DeepCopy()
		updatePort.Spec.Configuration.KafkaAPI[0].Port = 200
		updatePort.Spec.Configuration.AdminAPI[0].Port = 200
		updatePort.Spec.Configuration.RPCServer.Port = 200
		updatePort.Spec.Configuration.SchemaRegistry.Port = 200

		err := updatePort.ValidateUpdate(redpandaCluster)
		assert.Error(t, err)
	})

	t.Run("collision in the port when external connectivity is enabled", func(t *testing.T) {
		updatePort := redpandaCluster.DeepCopy()
		updatePort.Spec.Configuration.KafkaAPI = append(updatePort.Spec.Configuration.KafkaAPI,
			v1alpha1.KafkaAPI{External: v1alpha1.ExternalConnectivityConfig{Enabled: true}})
		updatePort.Spec.Configuration.AdminAPI = append(updatePort.Spec.Configuration.AdminAPI,
			v1alpha1.AdminAPI{External: v1alpha1.ExternalConnectivityConfig{Enabled: true}})
		updatePort.Spec.Configuration.PandaproxyAPI = append(updatePort.Spec.Configuration.PandaproxyAPI,
			v1alpha1.PandaproxyAPI{External: v1alpha1.ExternalConnectivityConfig{Enabled: true}})

		err := updatePort.ValidateUpdate(redpandaCluster)
		assert.Error(t, err)
	})

	t.Run("no collision when schema registry has the next port to panda proxy", func(t *testing.T) {
		updatePort := redpandaCluster.DeepCopy()
		updatePort.Spec.Configuration.KafkaAPI[0].Port = 200
		updatePort.Spec.Configuration.KafkaAPI = append(updatePort.Spec.Configuration.KafkaAPI,
			v1alpha1.KafkaAPI{External: v1alpha1.ExternalConnectivityConfig{Enabled: true}})
		updatePort.Spec.Configuration.PandaproxyAPI = append(updatePort.Spec.Configuration.PandaproxyAPI,
			v1alpha1.PandaproxyAPI{External: v1alpha1.ExternalConnectivityConfig{Enabled: true}})
		updatePort.Spec.Configuration.SchemaRegistry.External = &v1alpha1.ExternalConnectivityConfig{
			Enabled: true,
		}

		err := updatePort.ValidateUpdate(redpandaCluster)
		assert.NoError(t, err)
	})

	t.Run("collision in the port when external connectivity is enabled", func(t *testing.T) {
		updatePort := redpandaCluster.DeepCopy()
		updatePort.Spec.Configuration.KafkaAPI = append(updatePort.Spec.Configuration.KafkaAPI,
			v1alpha1.KafkaAPI{External: v1alpha1.ExternalConnectivityConfig{Enabled: true}})
		updatePort.Spec.Configuration.KafkaAPI[0].Port = 200
		updatePort.Spec.Configuration.AdminAPI[0].Port = 300
		updatePort.Spec.Configuration.RPCServer.Port = 201

		err := updatePort.ValidateUpdate(redpandaCluster)
		assert.Error(t, err)
	})

	t.Run("collision in the port when external connectivity is enabled", func(t *testing.T) {
		updatePort := redpandaCluster.DeepCopy()
		updatePort.Spec.Configuration.KafkaAPI = append(updatePort.Spec.Configuration.KafkaAPI,
			v1alpha1.KafkaAPI{External: v1alpha1.ExternalConnectivityConfig{Enabled: true}})
		updatePort.Spec.Configuration.KafkaAPI[0].Port = 200
		updatePort.Spec.Configuration.AdminAPI[0].Port = 300
		updatePort.Spec.Configuration.AdminAPI[0].External.Enabled = true
		updatePort.Spec.Configuration.RPCServer.Port = 301

		err := updatePort.ValidateUpdate(redpandaCluster)
		assert.Error(t, err)
	})

	t.Run("port collision with proxy and schema registry", func(t *testing.T) {
		updatePort := redpandaCluster.DeepCopy()
		updatePort.Spec.Configuration.SchemaRegistry.Port = updatePort.Spec.Configuration.PandaproxyAPI[0].Port

		err := updatePort.ValidateUpdate(redpandaCluster)
		assert.Error(t, err)
	})

	t.Run("collision in admin port when external connectivity is enabled", func(t *testing.T) {
		updatePort := redpandaCluster.DeepCopy()
		updatePort.Spec.Configuration.AdminAPI[0].External.Enabled = true
		updatePort.Spec.Configuration.KafkaAPI = append(updatePort.Spec.Configuration.KafkaAPI,
			v1alpha1.KafkaAPI{External: v1alpha1.ExternalConnectivityConfig{Enabled: true}})
		updatePort.Spec.Configuration.KafkaAPI[0].Port = 201
		updatePort.Spec.Configuration.AdminAPI[0].Port = 200
		updatePort.Spec.Configuration.RPCServer.Port = 300

		err := updatePort.ValidateUpdate(redpandaCluster)
		assert.Error(t, err)
	})

	t.Run("requireclientauth true and tls enabled", func(t *testing.T) {
		tls := redpandaCluster.DeepCopy()
		tls.Spec.Configuration.KafkaAPI[0].TLS.RequireClientAuth = true
		tls.Spec.Configuration.KafkaAPI[0].TLS.Enabled = true

		err := tls.ValidateUpdate(redpandaCluster)
		assert.NoError(t, err)
	})

	t.Run("multiple external listeners", func(t *testing.T) {
		exPort := redpandaCluster.DeepCopy()
		exPort.Spec.Configuration.KafkaAPI[0].External.Enabled = true
		exPort.Spec.Configuration.KafkaAPI = append(exPort.Spec.Configuration.KafkaAPI,
			v1alpha1.KafkaAPI{Port: 123, External: v1alpha1.ExternalConnectivityConfig{Enabled: true}})
		err := exPort.ValidateUpdate(redpandaCluster)

		assert.Error(t, err)
	})

	t.Run("multiple internal listeners", func(t *testing.T) {
		multiPort := redpandaCluster.DeepCopy()
		multiPort.Spec.Configuration.KafkaAPI = append(multiPort.Spec.Configuration.KafkaAPI,
			v1alpha1.KafkaAPI{Port: 123})
		err := multiPort.ValidateUpdate(redpandaCluster)

		assert.Error(t, err)
	})

	t.Run("external listener cannot have port specified", func(t *testing.T) {
		exPort := redpandaCluster.DeepCopy()
		exPort.Spec.Configuration.KafkaAPI[0].External.Enabled = true
		err := exPort.ValidateUpdate(redpandaCluster)

		assert.Error(t, err)
	})

	t.Run("no admin port", func(t *testing.T) {
		noPort := redpandaCluster.DeepCopy()
		noPort.Spec.Configuration.AdminAPI = []v1alpha1.AdminAPI{}

		err := noPort.ValidateUpdate(redpandaCluster)
		assert.Error(t, err)
	})

	t.Run("multiple internal admin listeners", func(t *testing.T) {
		multiPort := redpandaCluster.DeepCopy()
		multiPort.Spec.Configuration.AdminAPI = append(multiPort.Spec.Configuration.AdminAPI,
			v1alpha1.AdminAPI{Port: 123})
		err := multiPort.ValidateUpdate(redpandaCluster)

		assert.Error(t, err)
	})

	t.Run("external admin listener cannot have port specified", func(t *testing.T) {
		exPort := redpandaCluster.DeepCopy()
		exPort.Spec.Configuration.AdminAPI[0].External.Enabled = true
		err := exPort.ValidateUpdate(redpandaCluster)

		assert.Error(t, err)
	})

	t.Run("multiple admin listeners with tls", func(t *testing.T) {
		multiPort := redpandaCluster.DeepCopy()
		multiPort.Spec.Configuration.AdminAPI[0].TLS.Enabled = true
		multiPort.Spec.Configuration.AdminAPI = append(multiPort.Spec.Configuration.AdminAPI,
			v1alpha1.AdminAPI{
				Port:     123,
				External: v1alpha1.ExternalConnectivityConfig{Enabled: true},
				TLS:      v1alpha1.AdminAPITLS{Enabled: true},
			})
		err := multiPort.ValidateUpdate(redpandaCluster)

		assert.Error(t, err)
	})

	t.Run("tls admin listener without enabled true", func(t *testing.T) {
		multiPort := redpandaCluster.DeepCopy()
		multiPort.Spec.Configuration.AdminAPI[0].TLS.RequireClientAuth = true
		multiPort.Spec.Configuration.AdminAPI[0].TLS.Enabled = false
		err := multiPort.ValidateUpdate(redpandaCluster)

		assert.Error(t, err)
	})

	t.Run("proxy subdomain must be the same as kafka subdomain", func(t *testing.T) {
		withSub := redpandaCluster.DeepCopy()
		withSub.Spec.Configuration.PandaproxyAPI = []v1alpha1.PandaproxyAPI{
			{
				Port:     145,
				External: v1alpha1.ExternalConnectivityConfig{Enabled: true, Subdomain: "subdomain"},
			},
		}
		err := withSub.ValidateUpdate(redpandaCluster)

		assert.Error(t, err)
	})

	t.Run("cannot have multiple internal proxy listeners", func(t *testing.T) {
		multiPort := redpandaCluster.DeepCopy()
		multiPort.Spec.Configuration.PandaproxyAPI = append(multiPort.Spec.Configuration.PandaproxyAPI,
			v1alpha1.PandaproxyAPI{Port: 123}, v1alpha1.PandaproxyAPI{Port: 321})
		err := multiPort.ValidateUpdate(redpandaCluster)

		assert.Error(t, err)
	})

	t.Run("cannot have external proxy listener without an internal one", func(t *testing.T) {
		noInternal := redpandaCluster.DeepCopy()
		noInternal.Spec.Configuration.PandaproxyAPI = append(noInternal.Spec.Configuration.PandaproxyAPI,
			v1alpha1.PandaproxyAPI{External: v1alpha1.ExternalConnectivityConfig{Enabled: true}, Port: 123})
		err := noInternal.ValidateUpdate(redpandaCluster)

		assert.Error(t, err)
	})

	t.Run("external proxy listener cannot have port specified", func(t *testing.T) {
		multiPort := redpandaCluster.DeepCopy()
		multiPort.Spec.Configuration.PandaproxyAPI = append(multiPort.Spec.Configuration.PandaproxyAPI,
			v1alpha1.PandaproxyAPI{External: v1alpha1.ExternalConnectivityConfig{Enabled: true}, Port: 123},
			v1alpha1.PandaproxyAPI{Port: 321})
		err := multiPort.ValidateUpdate(redpandaCluster)

		assert.Error(t, err)
	})

	t.Run("pandaproxy tls disabled with client auth enabled", func(t *testing.T) {
		tls := redpandaCluster.DeepCopy()
		tls.Spec.Configuration.PandaproxyAPI = append(tls.Spec.Configuration.PandaproxyAPI,
			v1alpha1.PandaproxyAPI{TLS: v1alpha1.PandaproxyAPITLS{Enabled: false, RequireClientAuth: true}})

		err := tls.ValidateUpdate(redpandaCluster)
		assert.Error(t, err)
	})

	t.Run("resource limits/requests on redpanda resources", func(t *testing.T) {
		c := redpandaCluster.DeepCopy()
		c.Spec.Resources.Limits = corev1.ResourceList{
			corev1.ResourceMemory: resource.MustParse("1Gi"),
			corev1.ResourceCPU:    resource.MustParse("1"),
		}
		c.Spec.Resources.Requests = corev1.ResourceList{
			corev1.ResourceMemory: resource.MustParse("2Gi"),
			corev1.ResourceCPU:    resource.MustParse("1"),
		}

		err := c.ValidateUpdate(redpandaCluster)
		assert.Error(t, err)
	})

	t.Run("resource limits/requests on rpk status resources", func(t *testing.T) {
		c := redpandaCluster.DeepCopy()
		c.Spec.Sidecars = v1alpha1.Sidecars{
			RpkStatus: &v1alpha1.Sidecar{
				Enabled: true,
				Resources: &corev1.ResourceRequirements{
					Limits: corev1.ResourceList{
						corev1.ResourceMemory: resource.MustParse("0.1Gi"),
						corev1.ResourceCPU:    resource.MustParse("0.1"),
					},
					Requests: corev1.ResourceList{
						corev1.ResourceMemory: resource.MustParse("0.2Gi"),
						corev1.ResourceCPU:    resource.MustParse("0.1"),
					},
				},
			},
		}

		err := c.ValidateUpdate(redpandaCluster)
		assert.Error(t, err)
	})
}

//nolint:funlen // this is ok for a test
func TestCreation(t *testing.T) {
	redpandaCluster := &v1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test",
			Namespace: "",
		},
		Spec: v1alpha1.ClusterSpec{
			Configuration: v1alpha1.RedpandaConfig{
				KafkaAPI:       []v1alpha1.KafkaAPI{{Port: 124}},
				AdminAPI:       []v1alpha1.AdminAPI{{Port: 125}},
				RPCServer:      v1alpha1.SocketAddress{Port: 126},
				SchemaRegistry: &v1alpha1.SchemaRegistryAPI{Port: 127},
				PandaproxyAPI:  []v1alpha1.PandaproxyAPI{{Port: 128}},
			},
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceMemory: resource.MustParse("2Gi"),
					corev1.ResourceCPU:    resource.MustParse("1"),
				},
			},
		},
	}

	t.Run("no collision in the port", func(t *testing.T) {
		newPort := redpandaCluster.DeepCopy()
		newPort.Spec.Configuration.KafkaAPI[0].Port = 200

		err := newPort.ValidateCreate()
		assert.NoError(t, err)
	})

	t.Run("collision in the port", func(t *testing.T) {
		newPort := redpandaCluster.DeepCopy()
		newPort.Spec.Configuration.KafkaAPI[0].Port = 200
		newPort.Spec.Configuration.AdminAPI[0].Port = 200
		newPort.Spec.Configuration.RPCServer.Port = 200
		newPort.Spec.Configuration.SchemaRegistry.Port = 200

		err := newPort.ValidateCreate()
		assert.Error(t, err)
	})

	t.Run("collision in the port when external connectivity is enabled", func(t *testing.T) {
		newPort := redpandaCluster.DeepCopy()
		newPort.Spec.Configuration.KafkaAPI = append(newPort.Spec.Configuration.KafkaAPI,
			v1alpha1.KafkaAPI{External: v1alpha1.ExternalConnectivityConfig{Enabled: true}})
		newPort.Spec.Configuration.AdminAPI = append(newPort.Spec.Configuration.AdminAPI,
			v1alpha1.AdminAPI{External: v1alpha1.ExternalConnectivityConfig{Enabled: true}})
		newPort.Spec.Configuration.PandaproxyAPI = append(newPort.Spec.Configuration.PandaproxyAPI,
			v1alpha1.PandaproxyAPI{External: v1alpha1.ExternalConnectivityConfig{Enabled: true}})

		err := newPort.ValidateCreate()
		assert.Error(t, err)
	})

	t.Run("no collision when schema registry has the next port to panda proxy", func(t *testing.T) {
		newPort := redpandaCluster.DeepCopy()
		newPort.Spec.Configuration.KafkaAPI[0].Port = 200
		newPort.Spec.Configuration.KafkaAPI = append(newPort.Spec.Configuration.KafkaAPI,
			v1alpha1.KafkaAPI{External: v1alpha1.ExternalConnectivityConfig{Enabled: true}})
		newPort.Spec.Configuration.PandaproxyAPI = append(newPort.Spec.Configuration.PandaproxyAPI,
			v1alpha1.PandaproxyAPI{External: v1alpha1.ExternalConnectivityConfig{Enabled: true}})
		newPort.Spec.Configuration.SchemaRegistry.External = &v1alpha1.ExternalConnectivityConfig{
			Enabled: true,
		}

		err := newPort.ValidateCreate()
		assert.NoError(t, err)
	})

	t.Run("port collision with proxy and schema registry", func(t *testing.T) {
		newPort := redpandaCluster.DeepCopy()
		newPort.Spec.Configuration.SchemaRegistry.Port = newPort.Spec.Configuration.PandaproxyAPI[0].Port

		err := newPort.ValidateCreate()
		assert.Error(t, err)
	})

	t.Run("no kafka port", func(t *testing.T) {
		noPort := redpandaCluster.DeepCopy()
		noPort.Spec.Configuration.KafkaAPI = []v1alpha1.KafkaAPI{}

		err := noPort.ValidateCreate()
		assert.Error(t, err)
	})

	t.Run("no admin port", func(t *testing.T) {
		noPort := redpandaCluster.DeepCopy()
		noPort.Spec.Configuration.AdminAPI = []v1alpha1.AdminAPI{}

		err := noPort.ValidateCreate()
		assert.Error(t, err)
	})

	t.Run("multiple internal admin listeners", func(t *testing.T) {
		multiPort := redpandaCluster.DeepCopy()
		multiPort.Spec.Configuration.AdminAPI = append(multiPort.Spec.Configuration.AdminAPI,
			v1alpha1.AdminAPI{Port: 123})
		err := multiPort.ValidateCreate()

		assert.Error(t, err)
	})

	t.Run("external admin listener cannot have port specified", func(t *testing.T) {
		exPort := redpandaCluster.DeepCopy()
		exPort.Spec.Configuration.AdminAPI[0].External.Enabled = true
		err := exPort.ValidateCreate()

		assert.Error(t, err)
	})

	t.Run("incorrect memory (need 2GB per core)", func(t *testing.T) {
		memory := redpandaCluster.DeepCopy()
		memory.Spec.Resources = corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceMemory: resource.MustParse("1Gi"),
				corev1.ResourceCPU:    resource.MustParse("2"),
			},
		}

		err := memory.ValidateCreate()
		assert.Error(t, err)
	})

	t.Run("no 2GB per core required when in developer mode", func(t *testing.T) {
		memory := redpandaCluster.DeepCopy()
		memory.Spec.Resources = corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceMemory: resource.MustParse("1Gi"),
				corev1.ResourceCPU:    resource.MustParse("2"),
			},
		}
		memory.Spec.Configuration.DeveloperMode = true

		err := memory.ValidateCreate()
		assert.NoError(t, err)
	})

	t.Run("tls properly configured", func(t *testing.T) {
		tls := redpandaCluster.DeepCopy()
		tls.Spec.Configuration.KafkaAPI[0].TLS.Enabled = true
		tls.Spec.Configuration.KafkaAPI[0].TLS.RequireClientAuth = true

		err := tls.ValidateCreate()
		assert.NoError(t, err)
	})

	t.Run("require client auth without tls enabled", func(t *testing.T) {
		tls := redpandaCluster.DeepCopy()
		tls.Spec.Configuration.KafkaAPI[0].TLS.Enabled = false
		tls.Spec.Configuration.KafkaAPI[0].TLS.RequireClientAuth = true

		err := tls.ValidateCreate()
		assert.Error(t, err)
	})

	t.Run("multiple external listeners", func(t *testing.T) {
		exPort := redpandaCluster.DeepCopy()
		exPort.Spec.Configuration.KafkaAPI[0].External.Enabled = true
		exPort.Spec.Configuration.KafkaAPI = append(exPort.Spec.Configuration.KafkaAPI,
			v1alpha1.KafkaAPI{Port: 123, External: v1alpha1.ExternalConnectivityConfig{Enabled: true}})
		err := exPort.ValidateCreate()

		assert.Error(t, err)
	})

	t.Run("multiple internal listeners", func(t *testing.T) {
		multiPort := redpandaCluster.DeepCopy()
		multiPort.Spec.Configuration.KafkaAPI = append(multiPort.Spec.Configuration.KafkaAPI,
			v1alpha1.KafkaAPI{Port: 123})
		err := multiPort.ValidateCreate()

		assert.Error(t, err)
	})

	t.Run("external listener with port", func(t *testing.T) {
		exPort := redpandaCluster.DeepCopy()
		exPort.Spec.Configuration.KafkaAPI[0].External.Enabled = true
		err := exPort.ValidateCreate()

		assert.Error(t, err)
	})

	t.Run("multiple admin listeners with tls", func(t *testing.T) {
		multiPort := redpandaCluster.DeepCopy()
		multiPort.Spec.Configuration.AdminAPI[0].TLS.Enabled = true
		multiPort.Spec.Configuration.AdminAPI = append(multiPort.Spec.Configuration.AdminAPI,
			v1alpha1.AdminAPI{
				Port:     123,
				External: v1alpha1.ExternalConnectivityConfig{Enabled: true},
				TLS:      v1alpha1.AdminAPITLS{Enabled: true},
			})
		err := multiPort.ValidateCreate()

		assert.Error(t, err)
	})

	t.Run("tls admin listener without enabled true", func(t *testing.T) {
		multiPort := redpandaCluster.DeepCopy()
		multiPort.Spec.Configuration.AdminAPI[0].TLS.RequireClientAuth = true
		multiPort.Spec.Configuration.AdminAPI[0].TLS.Enabled = false
		err := multiPort.ValidateCreate()

		assert.Error(t, err)
	})

	t.Run("proxy subdomain must be the same as kafka subdomain", func(t *testing.T) {
		withSub := redpandaCluster.DeepCopy()
		withSub.Spec.Configuration.PandaproxyAPI = []v1alpha1.PandaproxyAPI{
			{
				Port:     145,
				External: v1alpha1.ExternalConnectivityConfig{Enabled: true, Subdomain: "subdomain"},
			},
		}
		err := withSub.ValidateCreate()

		assert.Error(t, err)
	})

	t.Run("cannot have multiple internal proxy listeners", func(t *testing.T) {
		multiPort := redpandaCluster.DeepCopy()
		multiPort.Spec.Configuration.PandaproxyAPI = append(multiPort.Spec.Configuration.PandaproxyAPI,
			v1alpha1.PandaproxyAPI{Port: 123}, v1alpha1.PandaproxyAPI{Port: 321})
		err := multiPort.ValidateCreate()

		assert.Error(t, err)
	})

	t.Run("cannot have external proxy listener without an internal one", func(t *testing.T) {
		noInternal := redpandaCluster.DeepCopy()
		noInternal.Spec.Configuration.PandaproxyAPI = append(noInternal.Spec.Configuration.PandaproxyAPI,
			v1alpha1.PandaproxyAPI{External: v1alpha1.ExternalConnectivityConfig{Enabled: true}, Port: 123})
		err := noInternal.ValidateCreate()

		assert.Error(t, err)
	})

	t.Run("external proxy listener cannot have port specified", func(t *testing.T) {
		multiPort := redpandaCluster.DeepCopy()
		multiPort.Spec.Configuration.PandaproxyAPI = append(multiPort.Spec.Configuration.PandaproxyAPI,
			v1alpha1.PandaproxyAPI{External: v1alpha1.ExternalConnectivityConfig{Enabled: true}, Port: 123},
			v1alpha1.PandaproxyAPI{Port: 321})
		err := multiPort.ValidateCreate()

		assert.Error(t, err)
	})

	t.Run("pandaproxy tls disabled but client auth enabled", func(t *testing.T) {
		tls := redpandaCluster.DeepCopy()
		tls.Spec.Configuration.PandaproxyAPI = append(tls.Spec.Configuration.PandaproxyAPI,
			v1alpha1.PandaproxyAPI{TLS: v1alpha1.PandaproxyAPITLS{Enabled: false, RequireClientAuth: true}})

		err := tls.ValidateCreate()
		assert.Error(t, err)
	})
}
