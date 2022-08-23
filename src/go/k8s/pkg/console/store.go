package console

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	redpandav1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
)

// By default, Console can only be created with the same namespace as the referenced Cluster.
// This is because Secrets, which are namespaced resource, cannot be mounted in Deployment (e.g. Schema Registry TLS).
// Store enables creating Console in different namespace from the Cluster by saving a copy of the Secret in cache.

// WANT: Make this file generic through "registering" watched resources and using dynamic client to get unstructured resources.

// Store is a controller cache store for resources required across namespaces
type Store struct {
	cache.ThreadSafeStore

	context context.Context
	client  client.Client
}

// NewStore creates a new store
func NewStore(cl client.Client) *Store {
	return &Store{
		ThreadSafeStore: cache.NewThreadSafeStore(cache.Indexers{}, cache.Indices{}),
		context:         context.Background(),
		client:          cl,
	}
}

// Sync synchronizes watched resources to the store
func (s *Store) Sync(cluster *redpandav1alpha1.Cluster) error {
	if cluster.IsSchemaRegistryTLSEnabled() {
		if cluster.IsSchemaRegistryMutualTLSEnabled() {
			schemaRegistryClientCert, err := syncSchemaRegistryCert(
				s.context,
				s.client,
				client.ObjectKeyFromObject(cluster),
				fmt.Sprintf("%s-%s", cluster.GetName(), schemaRegistryClientCertSuffix),
			)
			if err != nil {
				return fmt.Errorf("sync schema registry client certificate: %w", err)
			}
			// same as Update()
			s.Add(s.getSchemaRegistryClientCertKey(cluster), schemaRegistryClientCert)
		}

		// Only sync CA cert if not using DefaultCaFilePath
		ca := &SchemaRegistryTLSCa{cluster.SchemaRegistryAPITLS().TLS.NodeSecretRef}
		if ca.useCaCert() {
			nodeSecretRef := cluster.SchemaRegistryAPITLS().TLS.NodeSecretRef
			schemaRegistryNodeCert, err := syncSchemaRegistryCert(
				s.context,
				s.client,
				types.NamespacedName{Namespace: nodeSecretRef.Namespace, Name: nodeSecretRef.Name},
				nodeSecretRef.Name,
			)
			if err != nil {
				return fmt.Errorf("sync schema registry node certificate: %w", err)
			}
			s.Add(s.getSchemaRegistryNodeCertKey(cluster), schemaRegistryNodeCert)
		}
	}

	return nil
}

func syncSchemaRegistryCert(ctx context.Context, cl client.Client, nsn client.ObjectKey, name string) (client.Object, error) {
	secret := corev1.Secret{}
	secretNsn := types.NamespacedName{
		Namespace: nsn.Namespace,
		Name:      name,
	}
	if err := cl.Get(ctx, secretNsn, &secret); err != nil {
		return &secret, err
	}
	return &secret, nil
}

func (s *Store) getSchemaRegistryClientCertKey(cluster *redpandav1alpha1.Cluster) string {
	return fmt.Sprintf("%s-%s-%s", cluster.GetNamespace(), cluster.GetName(), schemaRegistryClientCertSuffix)
}

func (s *Store) getSchemaRegistryNodeCertKey(cluster *redpandav1alpha1.Cluster) string {
	return fmt.Sprintf("%s-%s-%s", cluster.GetNamespace(), cluster.GetName(), "schema-registry-node")
}

// GetSchemaRegistryClientCert gets the Schema Registry client cert and returns Secret object
func (s *Store) GetSchemaRegistryClientCert(cluster *redpandav1alpha1.Cluster) (*corev1.Secret, bool) {
	if secret, exists := s.Get(s.getSchemaRegistryClientCertKey(cluster)); exists {
		return secret.(*corev1.Secret), true
	}
	return nil, false
}

// GetSchemaRegistryNodeCert gets the Schema Registry node cert and returns Secret object
func (s *Store) GetSchemaRegistryNodeCert(cluster *redpandav1alpha1.Cluster) (*corev1.Secret, bool) {
	if secret, exists := s.Get(s.getSchemaRegistryNodeCertKey(cluster)); exists {
		return secret.(*corev1.Secret), true
	}
	return nil, false
}
