package certmanager

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	redpandav1alpha1 "github.com/vectorizedio/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	"github.com/vectorizedio/redpanda/src/go/k8s/pkg/labels"
	"github.com/vectorizedio/redpanda/src/go/k8s/pkg/resources"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	k8sclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const (
	passwordKey = "password"

	keystoreSuffix = "keystore"
	separator      = "-"
)

// KeystoreSecretResource is part of the reconciliation of redpanda.vectorized.io CRD
// creating secrets for pkcs#12 and jks password
type KeystoreSecretResource struct {
	k8sclient.Client
	scheme       *runtime.Scheme
	pandaCluster *redpandav1alpha1.Cluster
	key          types.NamespacedName
	logger       logr.Logger
}

// NewKeystoreSecretResource creates KeystoreSecretResource instance
func NewKeystoreSecretResource(
	client k8sclient.Client,
	scheme *runtime.Scheme,
	pandaCluster *redpandav1alpha1.Cluster,
	key types.NamespacedName,
	logger logr.Logger,
) *KeystoreSecretResource {
	return &KeystoreSecretResource{
		client,
		scheme,
		pandaCluster,
		key,
		logger,
	}
}

// Ensure will manage pkcs#12 and jks password for KafkaAPI certificate
func (r *KeystoreSecretResource) Ensure(ctx context.Context) (bool, error) {
	obj, err := r.obj()
	if err != nil {
		return false, fmt.Errorf("unable to construct object: %w", err)
	}

	_, err = resources.CreateIfNotExists(ctx, r, obj, r.logger)
	return err == nil, err
}

// obj returns secret that consist password for jks and pkcs#12 keystores
func (r *KeystoreSecretResource) obj() (k8sclient.Object, error) {
	objLabels := labels.ForCluster(r.pandaCluster)
	secret := &v1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      r.Key().Name,
			Namespace: r.Key().Namespace,
			Labels:    objLabels,
		},
		StringData: map[string]string{
			passwordKey: r.pandaCluster.Name,
		},
	}
	err := controllerutil.SetControllerReference(r.pandaCluster, secret, r.scheme)
	if err != nil {
		return nil, err
	}

	return secret, nil
}

// Key returns namespace/name object that is used to identify object.
// For reference please visit types.NamespacedName docs in k8s.io/apimachinery
func (r *KeystoreSecretResource) Key() types.NamespacedName {
	return r.key
}

func keystoreName(clusterName string) string {
	suffixLength := len(keystoreSuffix)
	maxClusterNameLength := validation.DNS1123SubdomainMaxLength - suffixLength - len(separator)
	if len(clusterName) > maxClusterNameLength {
		clusterName = clusterName[:maxClusterNameLength]
	}
	return fmt.Sprintf("%s%s%s", clusterName, separator, keystoreSuffix)
}
