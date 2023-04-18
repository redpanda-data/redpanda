package certmanager_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/go-logr/logr"
	"github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources/certmanager"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/pointer"

	"k8s.io/client-go/kubernetes/scheme"

	fake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

var errNotSecret = errors.New("not secret")

const (
	bundleCACertSecretSuffix = "ca-bundle"
	clusterName              = "test-cluster-create"
	ns                       = "default"
)

func init() {
	fakeK8sClient := fake.NewClientBuilder().Build()
	_ = v1alpha1.AddToScheme(fakeK8sClient.Scheme())
}

func TestCreateCACertBundle(t *testing.T) {
	fakeK8sClient := fake.NewClientBuilder().Build()
	nsPrefix := "test-ns"

	pandaCluster := &v1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{
			Name: clusterName,
		},
		Spec: v1alpha1.ClusterSpec{
			Replicas:      pointer.Int32(1),
			Configuration: v1alpha1.RedpandaConfig{},
		},
	}

	caCert, err := getCertSecretFromFile("./testdata/ca-cert-secret.yaml")
	assert.NoError(t, err)
	caCert.SetNamespace(nsPrefix)
	err = fakeK8sClient.Create(context.TODO(), caCert)
	assert.NoError(t, err)

	tlsCert, err := getCertSecretFromFile("./testdata/tls-cert-secret.yaml")
	assert.NoError(t, err)
	tlsCert.SetNamespace(nsPrefix)
	err = fakeK8sClient.Create(context.TODO(), tlsCert)
	assert.NoError(t, err)

	expectedOneCACertData := getExpectedCACert("./testdata/expected-one-ca-crt.pem")
	expectedTwoCACertData := getExpectedCACert("./testdata/expected-two-ca-crt.pem")
	expectedThreeCACertData := getExpectedCACert("./testdata/expected-three-ca-crt.pem")

	table := []struct {
		scenario         string
		certs            []*types.NamespacedName
		expectedBundleCA []byte
	}{
		{
			scenario:         "Get CA cert from secret having ca.crt",
			certs:            []*types.NamespacedName{{Name: caCert.Name, Namespace: caCert.Namespace}},
			expectedBundleCA: expectedOneCACertData,
		},
		{
			scenario:         "Get CA certs from secret having tls.crt",
			certs:            []*types.NamespacedName{{Name: tlsCert.Name, Namespace: tlsCert.Namespace}},
			expectedBundleCA: expectedTwoCACertData,
		},
		{
			scenario: "Get CA certs from secret having ca.crt and secret having tls.crt",
			certs: []*types.NamespacedName{
				{Name: caCert.Name, Namespace: caCert.Namespace},
				{Name: tlsCert.Name, Namespace: tlsCert.Namespace},
			},
			expectedBundleCA: expectedThreeCACertData,
		},
	}
	for i, tt := range table {
		t.Run(fmt.Sprintf("case-%d: %s", i, tt.scenario), func(t *testing.T) {
			ns := fmt.Sprintf("%s-%d", nsPrefix, i)
			pandaCluster.SetNamespace(ns)

			caCertBundle := certmanager.NewCACertificateBundle(fakeK8sClient, fakeK8sClient.Scheme(), pandaCluster,
				tt.certs, bundleCACertSecretSuffix, logr.Discard())

			err = caCertBundle.Ensure(context.TODO())
			assert.NoError(t, err)

			var bundledCASecret corev1.Secret
			err = fakeK8sClient.Get(context.TODO(), types.NamespacedName{Name: fmt.Sprintf("%s-%s", clusterName, bundleCACertSecretSuffix), Namespace: ns}, &bundledCASecret)
			assert.NoError(t, err)
			assert.Equal(t, tt.expectedBundleCA, bundledCASecret.Data["ca.crt"])
		})
	}
}

func TestUpdateCACertBundle(t *testing.T) {
	fakeK8sClient := fake.NewClientBuilder().Build()

	pandaCluster := &v1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      clusterName,
			Namespace: ns,
			UID:       "ff2770aa-c919-43f0-8b4a-30cb7cfdaf79",
		},
		Spec: v1alpha1.ClusterSpec{
			Replicas:      pointer.Int32(1),
			Configuration: v1alpha1.RedpandaConfig{},
		},
	}

	caCert, err := getCertSecretFromFile("./testdata/ca-cert-secret.yaml")
	assert.NoError(t, err)
	caCert.SetNamespace(ns)
	err = fakeK8sClient.Create(context.TODO(), caCert)
	assert.NoError(t, err)

	tlsCert, err := getCertSecretFromFile("./testdata/tls-cert-secret.yaml")
	assert.NoError(t, err)
	tlsCert.SetNamespace(ns)
	err = fakeK8sClient.Create(context.TODO(), tlsCert)
	assert.NoError(t, err)

	expectedThreeCACertData := getExpectedCACert("./testdata/expected-three-ca-crt.pem")

	caCertBundle := certmanager.NewCACertificateBundle(
		fakeK8sClient, fakeK8sClient.Scheme(), pandaCluster,
		[]*types.NamespacedName{
			{Name: caCert.Name, Namespace: caCert.Namespace},
			{Name: tlsCert.Name, Namespace: tlsCert.Namespace},
		},
		bundleCACertSecretSuffix, logr.Discard())

	err = caCertBundle.Ensure(context.TODO())
	assert.NoError(t, err)

	var bundledCASecret corev1.Secret
	err = fakeK8sClient.Get(context.TODO(), types.NamespacedName{Name: fmt.Sprintf("%s-%s", clusterName, bundleCACertSecretSuffix), Namespace: ns}, &bundledCASecret)
	assert.NoError(t, err)
	assert.Equal(t, expectedThreeCACertData, bundledCASecret.Data["ca.crt"])

	caCert, err = getCertSecretFromFile("./testdata/ca-cert-secret-update.yaml")
	assert.NoError(t, err)
	caCert.SetNamespace(ns)
	err = fakeK8sClient.Update(context.TODO(), caCert)
	assert.NoError(t, err)

	err = caCertBundle.Ensure(context.TODO())
	assert.NoError(t, err)

	expectedThreeCACertData = getExpectedCACert("./testdata/expected-three-ca-crt-update.pem")
	err = fakeK8sClient.Get(context.TODO(), types.NamespacedName{Name: fmt.Sprintf("%s-%s", clusterName, bundleCACertSecretSuffix), Namespace: ns}, &bundledCASecret)
	assert.NoError(t, err)
	assert.Equal(t, expectedThreeCACertData, bundledCASecret.Data["ca.crt"])
}

func TestCACertBundleFailures(t *testing.T) {
	fakeK8sClient := fake.NewClientBuilder().Build()

	pandaCluster := &v1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      clusterName,
			Namespace: ns,
			UID:       "ff2770aa-c919-43f0-8b4a-30cb7cfdaf79",
		},
		Spec: v1alpha1.ClusterSpec{
			Replicas:      pointer.Int32(1),
			Configuration: v1alpha1.RedpandaConfig{},
		},
	}

	caCert, err := getCertSecretFromFile("./testdata/ca-cert-secret.yaml")
	assert.NoError(t, err)
	caCert.SetNamespace(ns)

	tlsCert, err := getCertSecretFromFile("./testdata/tls-cert-secret.yaml")
	assert.NoError(t, err)
	tlsCert.SetNamespace(ns)

	caCertBundle := certmanager.NewCACertificateBundle(
		fakeK8sClient, fakeK8sClient.Scheme(), pandaCluster,
		[]*types.NamespacedName{
			{Name: tlsCert.Name, Namespace: tlsCert.Namespace},
			{Name: caCert.Name, Namespace: caCert.Namespace},
		},
		bundleCACertSecretSuffix, logr.Discard())

	err = caCertBundle.Ensure(context.TODO())
	// Expect failure since the secret does not exist.
	assert.Error(t, err)

	err = fakeK8sClient.Create(context.TODO(), tlsCert)
	assert.NoError(t, err)
	err = caCertBundle.Ensure(context.TODO())
	// Expect failure since the secret does not exist.
	assert.Error(t, err)

	err = fakeK8sClient.Create(context.TODO(), caCert)
	assert.NoError(t, err)
	err = caCertBundle.Ensure(context.TODO())
	assert.NoError(t, err)
}

func getCertSecretFromFile(filePath string) (*corev1.Secret, error) {
	s, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	obj, err := k8sObject(s)
	if err != nil {
		return nil, err
	}

	secret, ok := obj.(*corev1.Secret)
	if !ok {
		return nil, errNotSecret
	}

	return secret, nil
}

func k8sObject(s []byte) (runtime.Object, error) {
	// Decode text (yaml/json) to kube api object
	deserializer := serializer.NewCodecFactory(scheme.Scheme).UniversalDeserializer()
	obj, group, err := deserializer.Decode(s, nil, nil)
	if err != nil {
		return nil, err
	}
	obj.GetObjectKind().SetGroupVersionKind(*group)
	return obj, nil
}

func getExpectedCACert(expectedFile string) []byte {
	s, err := os.ReadFile(expectedFile)
	if err != nil {
		return nil
	}
	return s
}
