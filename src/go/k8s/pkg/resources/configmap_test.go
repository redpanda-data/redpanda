// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package resources_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	redpandav1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestEnsureConfigMap(t *testing.T) {
	require.NoError(t, redpandav1alpha1.AddToScheme(scheme.Scheme))
	clusterWithExternal := pandaCluster().DeepCopy()
	clusterWithExternal.Spec.Configuration.KafkaAPI = append(clusterWithExternal.Spec.Configuration.KafkaAPI, redpandav1alpha1.KafkaAPI{Port: 30001, External: redpandav1alpha1.ExternalConnectivityConfig{Enabled: true}})
	clusterWithMultipleKafkaTLS := pandaCluster().DeepCopy()
	clusterWithMultipleKafkaTLS.Spec.Configuration.KafkaAPI[0].TLS = redpandav1alpha1.KafkaAPITLS{Enabled: true}
	clusterWithMultipleKafkaTLS.Spec.Configuration.KafkaAPI = append(clusterWithMultipleKafkaTLS.Spec.Configuration.KafkaAPI, redpandav1alpha1.KafkaAPI{Port: 30001, TLS: redpandav1alpha1.KafkaAPITLS{Enabled: true}, External: redpandav1alpha1.ExternalConnectivityConfig{Enabled: true}})

	testcases := []struct {
		name           string
		cluster        redpandav1alpha1.Cluster
		expectedString string
	}{
		{
			name:    "External port specified",
			cluster: *clusterWithExternal,
			expectedString: `- address: 0.0.0.0
          port: 30001
          name: kafka-external`,
		},
		{
			name:    "Multiple Kafka TLS",
			cluster: *clusterWithMultipleKafkaTLS,
			expectedString: `- name: kafka
          key_file: /etc/tls/certs/tls.key
          cert_file: /etc/tls/certs/tls.crt
          enabled: true
        - name: kafka-external
          key_file: /etc/tls/certs/tls.key
          cert_file: /etc/tls/certs/tls.crt
          enabled: true`,
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			c := fake.NewClientBuilder().Build()
			secret := v1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "archival",
					Namespace: "default",
				},
				Data: map[string][]byte{
					"archival": []byte("XXX"),
				},
			}
			require.NoError(t, c.Create(context.TODO(), &secret))
			cfgRes := resources.NewConfigMap(
				c,
				&tc.cluster,
				scheme.Scheme,
				"cluster.local",
				types.NamespacedName{Name: "test", Namespace: "test"},
				types.NamespacedName{Name: "test", Namespace: "test"},
				ctrl.Log.WithName("test"))
			require.NoError(t, cfgRes.Ensure(context.TODO()))

			actual := &v1.ConfigMap{}
			err := c.Get(context.Background(), cfgRes.Key(), actual)
			require.NoError(t, err)
			data := actual.Data["redpanda.yaml"]
			require.True(t, strings.Contains(data, tc.expectedString), fmt.Sprintf("expecting %s but got %v", tc.expectedString, data))
		})
	}
}

func TestEnsureConfigMap_AdditionalConfig(t *testing.T) {
	require.NoError(t, redpandav1alpha1.AddToScheme(scheme.Scheme))

	testcases := []struct {
		name                    string
		additionalConfiguration map[string]string
		expectedStrings         []string
		expectedHash            string
	}{
		{
			name:                    "Primitive object in additional configuration",
			additionalConfiguration: map[string]string{"redpanda.transactional_id_expiration_ms": "25920000000"},
			expectedStrings:         []string{"transactional_id_expiration_ms: 25920000000"},
			expectedHash:            "13b7ea49c811062afe2a6d275604929e",
		},
		{
			name:                    "Complex struct in additional configuration",
			additionalConfiguration: map[string]string{"schema_registry.schema_registry_api": "[{'name':'external','address':'0.0.0.0','port':8081}]}"},
			expectedStrings: []string{`schema_registry:
    schema_registry_api:
        - address: 0.0.0.0
          port: 8081
          name: external`},
			expectedHash: "4b241955a317f47a6a06960a333ed661",
		},
		{
			name: "shadow index cache directory",
			expectedStrings: []string{
				`cloud_storage_cache_directory: /var/lib/shadow-index-cache`,
				`cloud_storage_cache_size: "10737418240"`,
			},
			expectedHash: "533227069ff8ee6791b874db60480b40",
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			panda := pandaCluster().DeepCopy()
			panda.Spec.AdditionalConfiguration = tc.additionalConfiguration
			c := fake.NewClientBuilder().Build()
			secret := v1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "archival",
					Namespace: "default",
				},
				Data: map[string][]byte{
					"archival": []byte("XXX"),
				},
			}
			require.NoError(t, c.Create(context.TODO(), &secret))
			cfgRes := resources.NewConfigMap(
				c,
				panda,
				scheme.Scheme,
				"cluster.local",
				types.NamespacedName{Name: "test", Namespace: "test"},
				types.NamespacedName{Name: "test", Namespace: "test"},
				ctrl.Log.WithName("test"))
			require.NoError(t, cfgRes.Ensure(context.TODO()))

			actual := &v1.ConfigMap{}
			err := c.Get(context.Background(), cfgRes.Key(), actual)
			require.NoError(t, err)
			data := actual.Data["redpanda.yaml"]
			for _, es := range tc.expectedStrings {
				require.True(t, strings.Contains(data, es), fmt.Sprintf("expecting %s but got %v", es, data))
			}
			hash, err := cfgRes.GetNodeConfigHash(context.TODO())
			require.NoError(t, err)
			require.Equal(t, tc.expectedHash, hash)
		})
	}
}
