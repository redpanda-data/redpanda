package resources_test

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	redpandav1alpha1 "github.com/vectorizedio/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	res "github.com/vectorizedio/redpanda/src/go/k8s/pkg/resources"
	v1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/deprecated/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

var c client.Client

func TestMain(m *testing.M) {
	var err error

	testEnv := &envtest.Environment{
		CRDDirectoryPaths: []string{filepath.Join("..", "..", "config", "crd", "bases")},
	}

	cfg, err := testEnv.Start()
	if err != nil {
		log.Fatal(err)
	}

	err = scheme.AddToScheme(scheme.Scheme)
	if err != nil {
		log.Fatal(err)
	}
	err = redpandav1alpha1.AddToScheme(scheme.Scheme)
	if err != nil {
		log.Fatal(err)
	}

	clientOptions := client.Options{Scheme: scheme.Scheme}

	c, err = client.New(cfg, clientOptions)
	if err != nil {
		log.Fatal(err)
	}

	exitCode := m.Run()
	err = testEnv.Stop()
	if err != nil {
		log.Fatal(err)
	}

	os.Exit(exitCode)
}

func TestEnsure_StatefulSet(t *testing.T) {
	cluster := pandaCluster()
	cluster = cluster.DeepCopy()
	cluster.Name = "ensure-integration-cluster"

	sts := res.NewStatefulSet(
		c,
		cluster,
		scheme.Scheme,
		"cluster.local",
		"servicename",
		types.NamespacedName{Name: "test", Namespace: "test"},
		types.NamespacedName{},
		types.NamespacedName{},
		types.NamespacedName{},
		types.NamespacedName{},
		"",
		"latest",
		ctrl.Log.WithName("test"))

	err := sts.Ensure(context.Background())
	assert.NoError(t, err)

	actual := &v1.StatefulSet{}
	err = c.Get(context.Background(), sts.Key(), actual)
	assert.NoError(t, err)
	originalResourceVersion := actual.ResourceVersion

	// calling ensure for second time to see the resource does not get updated
	err = sts.Ensure(context.Background())
	assert.NoError(t, err)

	err = c.Get(context.Background(), sts.Key(), actual)
	assert.NoError(t, err)
	if actual.ResourceVersion != originalResourceVersion {
		t.Fatalf("second ensure: expecting version %s but got %s", originalResourceVersion, actual.GetResourceVersion())
	}
}

func TestEnsure_ConfigMap(t *testing.T) {
	cluster := pandaCluster()
	cluster = cluster.DeepCopy()
	cluster.Name = "ensure-integration-cm-cluster"

	cm := res.NewConfigMap(
		c,
		cluster,
		scheme.Scheme,
		"cluster.local",
		ctrl.Log.WithName("test"))

	err := cm.Ensure(context.Background())
	assert.NoError(t, err)

	actual := &corev1.ConfigMap{}
	err = c.Get(context.Background(), cm.Key(), actual)
	assert.NoError(t, err)
	originalResourceVersion := actual.ResourceVersion

	// calling ensure for second time to see the resource does not get updated
	err = cm.Ensure(context.Background())
	assert.NoError(t, err)

	err = c.Get(context.Background(), cm.Key(), actual)
	assert.NoError(t, err)
	if actual.ResourceVersion != originalResourceVersion {
		t.Fatalf("second ensure: expecting version %s but got %s", originalResourceVersion, actual.GetResourceVersion())
	}

	// verify the update patches the config
	cluster.Spec.Configuration.KafkaAPI[0].Port = 1111
	cluster.Spec.Configuration.KafkaAPI[0].TLS.Enabled = true

	err = cm.Ensure(context.Background())
	assert.NoError(t, err)

	err = c.Get(context.Background(), cm.Key(), actual)
	assert.NoError(t, err)
	if actual.ResourceVersion == originalResourceVersion {
		t.Fatalf("expecting version to get updated after resource update but is %s", originalResourceVersion)
	}
	data := actual.Data["redpanda.yaml"]
	if !strings.Contains(data, "cert_file") || !strings.Contains(data, "port: 1111") {
		t.Fatalf("expecting configmap updated but got %v", data)
	}
}
