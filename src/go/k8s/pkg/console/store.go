package console

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	redpandav1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	labels "github.com/redpanda-data/redpanda/src/go/k8s/pkg/labels"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
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
	scheme  *runtime.Scheme
}

var (
	schemaRegistrySyncedSecretKey = "schema-registry"
	kafkaSyncedSecretKey          = "kafka"
)

// NewStore creates a new store
func NewStore(cl client.Client, scheme *runtime.Scheme) *Store {
	return &Store{
		ThreadSafeStore: cache.NewThreadSafeStore(cache.Indexers{}, cache.Indices{}),
		context:         context.Background(),
		client:          cl,
		scheme:          scheme,
	}
}

// Sync synchronizes watched resources to the store
func (s *Store) Sync(cluster *redpandav1alpha1.Cluster) error {
	if cluster.IsSchemaRegistryTLSEnabled() { //nolint:nestif // sync is complex
		if cluster.IsSchemaRegistryMutualTLSEnabled() {
			schemaRegistryClientCert, err := syncCert(
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
			schemaRegistryNodeCert, err := syncCert(
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

	if cluster.KafkaListener().IsMutualTLSEnabled() {
		kafkaClientCert, err := syncCert(
			s.context,
			s.client,
			client.ObjectKeyFromObject(cluster),
			fmt.Sprintf("%s-%s", cluster.GetName(), kafkaClientCertSuffix),
		)
		if err != nil {
			return fmt.Errorf("sync kafka client certificate: %w", err)
		}
		// same as Update()
		s.Add(s.getKafkaClientCertKey(cluster), kafkaClientCert)
	}

	return nil
}

func syncCert(
	ctx context.Context, cl client.Client, nsn client.ObjectKey, name string,
) (client.Object, error) {
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

func (s *Store) getSchemaRegistryClientCertKey(
	cluster *redpandav1alpha1.Cluster,
) string {
	return fmt.Sprintf("%s-%s-%s", cluster.GetNamespace(), cluster.GetName(), schemaRegistryClientCertSuffix)
}

func (s *Store) getKafkaClientCertKey(
	cluster *redpandav1alpha1.Cluster,
) string {
	return fmt.Sprintf("%s-%s-%s", cluster.GetNamespace(), cluster.GetName(), kafkaClientCertSuffix)
}

func (s *Store) getSchemaRegistryNodeCertKey(
	cluster *redpandav1alpha1.Cluster,
) string {
	return fmt.Sprintf("%s-%s-%s", cluster.GetNamespace(), cluster.GetName(), "schema-registry-node")
}

// GetSchemaRegistryClientCert gets the Schema Registry client cert and returns Secret object
func (s *Store) GetSchemaRegistryClientCert(
	cluster *redpandav1alpha1.Cluster,
) (*corev1.Secret, bool) {
	if secret, exists := s.Get(s.getSchemaRegistryClientCertKey(cluster)); exists {
		return secret.(*corev1.Secret), true
	}
	return nil, false
}

// GetSchemaRegistryNodeCert gets the Schema Registry node cert and returns Secret object
func (s *Store) GetSchemaRegistryNodeCert(
	cluster *redpandav1alpha1.Cluster,
) (*corev1.Secret, bool) {
	if secret, exists := s.Get(s.getSchemaRegistryNodeCertKey(cluster)); exists {
		return secret.(*corev1.Secret), true
	}
	return nil, false
}

// GetKafkaClientCert gets the Kafka client cert and returns Secret object
func (s *Store) GetKafkaClientCert(
	cluster *redpandav1alpha1.Cluster,
) (*corev1.Secret, bool) {
	if secret, exists := s.Get(s.getKafkaClientCertKey(cluster)); exists {
		return secret.(*corev1.Secret), true
	}
	return nil, false
}

// CreateSyncedSecret creates the synced Secret in Console namespace
func (s *Store) CreateSyncedSecret(
	console *redpandav1alpha1.Console,
	data map[string][]byte,
	secretNameSuffix string,
	log logr.Logger,
) (string, error) {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-%s", console.GetName(), secretNameSuffix),
			Namespace: console.GetNamespace(),
			Labels:    labels.ForConsole(console),
		},
		TypeMeta: metav1.TypeMeta{
			Kind:       "Secret",
			APIVersion: "v1",
		},
		Data: data,
	}

	err := controllerutil.SetControllerReference(console, secret, s.scheme)
	if err != nil {
		return "", err
	}

	created, err := resources.CreateIfNotExists(s.context, s.client, secret, log)
	if err != nil {
		return "", fmt.Errorf("creating Console synced secret %s: %w", secret, err)
	}

	if !created {
		var current corev1.Secret
		err = s.client.Get(s.context, types.NamespacedName{Name: secret.GetName(), Namespace: secret.GetNamespace()}, &current)
		if err != nil {
			return "", fmt.Errorf("fetching Console synced secret %s: %w", secret, err)
		}
		_, err = resources.Update(s.context, &current, secret, s.client, log)
		if err != nil {
			return "", fmt.Errorf("updating Console synced secret %s: %w", secret, err)
		}
	}

	return secret.GetName(), nil
}
