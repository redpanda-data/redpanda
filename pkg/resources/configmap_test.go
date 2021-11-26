package resources_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	redpandav1alpha1 "github.com/vectorizedio/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	"github.com/vectorizedio/redpanda/src/go/k8s/pkg/resources"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestEnsureConfigMap(t *testing.T) {
	require.NoError(t, redpandav1alpha1.AddToScheme(scheme.Scheme))

	testcases := []struct {
		name                    string
		additionalConfiguration map[string]string
		expectedString          string
	}{
		{
			name:                    "Primitive object in additional configuration",
			additionalConfiguration: map[string]string{"redpanda.transactional_id_expiration_ms": "25920000000"},
			expectedString:          "transactional_id_expiration_ms: 25920000000",
		},
		{
			name:                    "Complex struct in additional configuration",
			additionalConfiguration: map[string]string{"schema_registry.schema_registry_api": "[{'name':'external','address':'0.0.0.0','port':8081}]}"},
			expectedString: `schema_registry:
    schema_registry_api:
        - address: 0.0.0.0
          port: 8081
          name: external`,
		},
		{
			name: "shadow index cache directory",
			expectedString: `cloud_storage_cache_directory: /var/lib/shadow-index-cache
    cloud_storage_cache_size: "10737418240"`,
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
			require.True(t, strings.Contains(data, tc.expectedString), fmt.Sprintf("expecting %s but got %v", tc.expectedString, data))
		})
	}
}
