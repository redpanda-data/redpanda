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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

var c client.Client

const hash = "hash"

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
		types.NamespacedName{},
		types.NamespacedName{},
		types.NamespacedName{},
		types.NamespacedName{},
		types.NamespacedName{},
		"",
		res.ConfiguratorSettings{
			ConfiguratorBaseImage: "vectorized/configurator",
			ConfiguratorTag:       "latest",
			ImagePullPolicy:       "Always",
		},
		func(ctx context.Context) (string, error) { return hash, nil },
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

	secret := corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "archival",
			Namespace: "default",
		},
		Data: map[string][]byte{
			"archival": []byte("XXX"),
		},
	}
	assert.NoError(t, c.Create(context.Background(), &secret))

	cm := res.NewConfigMap(
		c,
		cluster,
		scheme.Scheme,
		"cluster.local",
		types.NamespacedName{},
		types.NamespacedName{},
		ctrl.Log.WithName("test"))

	err := cm.Ensure(context.Background())
	assert.NoError(t, err)

	actual := &corev1.ConfigMap{}
	err = c.Get(context.Background(), cm.Key(), actual)
	assert.NoError(t, err)
	originalResourceVersion := actual.ResourceVersion

	data := actual.Data["redpanda.yaml"]
	if !strings.Contains(data, "auto_create_topics_enabled: false") {
		t.Fatalf("expecting configmap containing 'auto_create_topics_enabled: false' but got %v", data)
	}
	if !strings.Contains(data, "enable_idempotence: false") {
		t.Fatalf("expecting configmap containing 'enable_idempotence: true' but got %v", data)
	}
	if !strings.Contains(data, "enable_transactions: false") {
		t.Fatalf("expecting configmap containing 'enable_transactions: true' but got %v", data)
	}

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
	data = actual.Data["redpanda.yaml"]
	if !strings.Contains(data, "cert_file") || !strings.Contains(data, "port: 1111") {
		t.Fatalf("expecting configmap updated but got %v", data)
	}
}

// nolint:funlen // the subtests might causes linter to complain
func TestEnsure_HeadlessService(t *testing.T) {
	t.Run("create-headless-service", func(t *testing.T) {
		cluster := pandaCluster()
		cluster.Name = "create-headles-service"

		hsvc := res.NewHeadlessService(
			c,
			cluster,
			scheme.Scheme,
			[]res.NamedServicePort{
				{Port: 123},
			},
			ctrl.Log.WithName("test"))

		err := hsvc.Ensure(context.Background())
		assert.NoError(t, err)

		actual := &corev1.Service{}
		err = c.Get(context.Background(), hsvc.Key(), actual)
		assert.NoError(t, err)
		assert.Equal(t, int32(123), actual.Spec.Ports[0].Port)
	})

	t.Run("create headless service idempotency", func(t *testing.T) {
		cluster := pandaCluster()
		cluster.Name = "create-headles-service-idempotency"

		hsvc := res.NewHeadlessService(
			c,
			cluster,
			scheme.Scheme,
			[]res.NamedServicePort{
				{Port: 123},
			},
			ctrl.Log.WithName("test"))

		err := hsvc.Ensure(context.Background())
		assert.NoError(t, err)

		actual := &corev1.Service{}
		err = c.Get(context.Background(), hsvc.Key(), actual)
		assert.NoError(t, err)
		originalResourceVersion := actual.ResourceVersion

		err = hsvc.Ensure(context.Background())
		assert.NoError(t, err)

		err = c.Get(context.Background(), hsvc.Key(), actual)
		assert.NoError(t, err)
		assert.Equal(t, originalResourceVersion, actual.ResourceVersion)
	})

	t.Run("updating headless service", func(t *testing.T) {
		cluster := pandaCluster()
		cluster.Name = "update-headles-service"

		hsvc := res.NewHeadlessService(
			c,
			cluster,
			scheme.Scheme,
			[]res.NamedServicePort{
				{Port: 123},
			},
			ctrl.Log.WithName("test"))

		err := hsvc.Ensure(context.Background())
		assert.NoError(t, err)

		actual := &corev1.Service{}
		err = c.Get(context.Background(), hsvc.Key(), actual)
		assert.NoError(t, err)
		originalResourceVersion := actual.ResourceVersion

		hsvc = res.NewHeadlessService(
			c,
			cluster,
			scheme.Scheme,
			[]res.NamedServicePort{
				{Port: 1111},
			},
			ctrl.Log.WithName("test"))

		err = hsvc.Ensure(context.Background())
		assert.NoError(t, err)

		err = c.Get(context.Background(), hsvc.Key(), actual)
		assert.NoError(t, err)
		assert.NotEqual(t, originalResourceVersion, actual.ResourceVersion)
		assert.Equal(t, int32(1111), actual.Spec.Ports[0].Port)
	})

	t.Run("HeadlessServiceFQDN with trailing dot", func(t *testing.T) {
		cluster := pandaCluster()
		cluster.Name = "trailing-dot-headles-service"
		cluster.Namespace = "some-namespace"

		hsvc := res.NewHeadlessService(
			c,
			cluster,
			scheme.Scheme,
			[]res.NamedServicePort{
				{Port: 123},
			},
			ctrl.Log.WithName("test"))

		fqdn := hsvc.HeadlessServiceFQDN("some.domain")
		assert.Equal(t, "trailing-dot-headles-service.some-namespace.svc.some.domain.", fqdn)
	})

	t.Run("HeadlessServiceFQDN without trailing dot", func(t *testing.T) {
		cluster := pandaCluster()
		cluster.Name = "without-trailing-dot-headles-service"
		cluster.Namespace = "different-namespace"
		cluster.Spec.DNSTrailingDotDisabled = true

		hsvc := res.NewHeadlessService(
			c,
			cluster,
			scheme.Scheme,
			[]res.NamedServicePort{
				{Port: 123},
			},
			ctrl.Log.WithName("test"))

		fqdn := hsvc.HeadlessServiceFQDN("some.domain")
		assert.Equal(t, "without-trailing-dot-headles-service.different-namespace.svc.some.domain", fqdn)
	})
}

func TestEnsure_NodePortService(t *testing.T) {
	cluster := pandaCluster()
	cluster = cluster.DeepCopy()
	cluster.Spec.Configuration.KafkaAPI = append(cluster.Spec.Configuration.KafkaAPI,
		redpandav1alpha1.KafkaAPI{External: redpandav1alpha1.ExternalConnectivityConfig{Enabled: true}})
	cluster.Name = "ensure-integration-np-cluster"

	npsvc := res.NewNodePortService(
		c,
		cluster,
		scheme.Scheme,
		[]res.NamedServiceNodePort{
			{NamedServicePort: res.NamedServicePort{Port: 123}, GenerateNodePort: true},
		},
		ctrl.Log.WithName("test"))

	err := npsvc.Ensure(context.Background())
	assert.NoError(t, err)

	actual := &corev1.Service{}
	err = c.Get(context.Background(), npsvc.Key(), actual)
	assert.NoError(t, err)
	originalResourceVersion := actual.ResourceVersion

	// calling ensure for second time to see the resource does not get updated
	err = npsvc.Ensure(context.Background())
	assert.NoError(t, err)

	err = c.Get(context.Background(), npsvc.Key(), actual)
	assert.NoError(t, err)
	if actual.ResourceVersion != originalResourceVersion {
		t.Fatalf("second ensure: expecting version %s but got %s", originalResourceVersion, actual.GetResourceVersion())
	}

	// verify the update patches the config

	// TODO this has to recreate the resource because the ports are passed from
	// outside. Once we refactor it to a point where ports are derived from CR
	// as it should, this test should be adjusted
	npsvc = res.NewNodePortService(
		c,
		cluster,
		scheme.Scheme,
		[]res.NamedServiceNodePort{
			{NamedServicePort: res.NamedServicePort{Port: 1111}, GenerateNodePort: true},
		},
		ctrl.Log.WithName("test"))

	err = npsvc.Ensure(context.Background())
	assert.NoError(t, err)

	err = c.Get(context.Background(), npsvc.Key(), actual)
	assert.NoError(t, err)
	if actual.ResourceVersion == originalResourceVersion {
		t.Fatalf("expecting version to get updated after resource update but is %s", originalResourceVersion)
	}
	port := actual.Spec.Ports[0].Port
	if port != 1111 {
		t.Fatalf("expecting configmap updated but got %d", port)
	}
}

func TestEnsure_LoadbalancerService(t *testing.T) {
	t.Run("create-loadbalancer-service", func(t *testing.T) {
		cluster := pandaCluster()
		cluster = cluster.DeepCopy()
		cluster.Spec.Configuration.KafkaAPI = append(cluster.Spec.Configuration.KafkaAPI,
			[]redpandav1alpha1.KafkaAPI{
				{
					Port: 1111,
				},
				{
					External: redpandav1alpha1.ExternalConnectivityConfig{
						Enabled: true,
						Bootstrap: &redpandav1alpha1.LoadBalancerConfig{
							Annotations: map[string]string{"key1": "val1"},
							Port:        2222,
						},
					},
				},
			}...)
		cluster.Name = "ensure-integration-lb-cluster"

		lb := res.NewLoadBalancerService(
			c,
			cluster,
			scheme.Scheme,
			[]res.NamedServicePort{
				{Name: "kafka-external-bootstrap", Port: 2222, TargetPort: 1112},
			},
			true,
			ctrl.Log.WithName("test"))

		err := lb.Ensure(context.Background())
		assert.NoError(t, err)

		actual := &corev1.Service{}
		err = c.Get(context.Background(), lb.Key(), actual)
		assert.NoError(t, err)

		assert.Equal(t, "ensure-integration-lb-cluster-lb-bootstrap", actual.Name)

		_, annotationExists := actual.Annotations["key1"]
		assert.True(t, annotationExists)
		assert.Equal(t, "val1", actual.Annotations["key1"])

		assert.True(t, len(actual.Spec.Ports) == 1)
		assert.Equal(t, int32(2222), actual.Spec.Ports[0].Port)
		assert.Equal(t, 1112, actual.Spec.Ports[0].TargetPort.IntValue())
		assert.Equal(t, corev1.ProtocolTCP, actual.Spec.Ports[0].Protocol)
		assert.Equal(t, "kafka-external-bootstrap", actual.Spec.Ports[0].Name)
	})
}
