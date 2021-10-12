package resources_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vectorizedio/redpanda/src/go/k8s/pkg/resources"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/deprecated/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestEnsureConfigMap(t *testing.T) {
	panda := pandaCluster().DeepCopy()
	panda.Spec.AdditionalConfiguration = map[string]string{"redpanda.transactional_id_expiration_ms": "25920000000"}
	c := fake.NewClientBuilder().Build()
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
	require.True(t, strings.Contains(data, "transactional_id_expiration_ms: 25920000000"), fmt.Sprintf("expecting transactional_id_expiration_ms: 25920000000 but got %v", data))
}
