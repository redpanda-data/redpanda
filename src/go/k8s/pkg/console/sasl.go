package console

import (
	"context"
	"fmt"
	"strings"

	"github.com/go-logr/logr"
	redpandav1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	adminutils "github.com/redpanda-data/redpanda/src/go/k8s/pkg/admin"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/api/admin"
	"github.com/twmb/franz-go/pkg/kadm"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	kerrors "k8s.io/apimachinery/pkg/util/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// KafkaSA is a Console resource
type KafkaSA struct {
	client.Client
	scheme             *runtime.Scheme
	consoleobj         *redpandav1alpha1.Console
	clusterobj         *redpandav1alpha1.Cluster
	clusterDomain      string
	adminAPI           adminutils.AdminAPIClientFactory
	superUsersResource *resources.SuperUsersResource
	log                logr.Logger
}

// NewKafkaSA instantiates a new KafkaSA
func NewKafkaSA(
	cl client.Client,
	scheme *runtime.Scheme,
	consoleobj *redpandav1alpha1.Console,
	clusterobj *redpandav1alpha1.Cluster,
	clusterDomain string,
	adminAPI adminutils.AdminAPIClientFactory,
	log logr.Logger,
) *KafkaSA {
	su := resources.NewSuperUsers(cl, consoleobj, scheme, GenerateSASLUsername(consoleobj.GetName()), resources.ConsoleSuffix, log)
	return &KafkaSA{
		Client:             cl,
		scheme:             scheme,
		consoleobj:         consoleobj,
		clusterobj:         clusterobj,
		clusterDomain:      clusterDomain,
		adminAPI:           adminAPI,
		log:                log,
		superUsersResource: su,
	}
}

var (
	// ConsoleSAFinalizer is the finalizer for deleting Service Account
	ConsoleSAFinalizer = "consoles.redpanda.vectorized.io/service-account"

	// ConsoleACLFinalizer is the finalizer for deleting ACLs
	ConsoleACLFinalizer = "consoles.redpanda.vectorized.io/acl"
)

type (
	// KafkaAdminClient contains functions from kadm.Client functions used by KafkaSA
	KafkaAdminClient interface {
		CreateACLs(context.Context, *kadm.ACLBuilder) (kadm.CreateACLsResults, error)
		DeleteACLs(context.Context, *kadm.ACLBuilder) (kadm.DeleteACLsResults, error)
	}

	// KafkaAdminClientFactory returns a KafkaAdminClient
	KafkaAdminClientFactory func(context.Context, client.Client, *redpandav1alpha1.Cluster, *Store) (KafkaAdminClient, error)
)

// GenerateSASLUsername returns username used for Kafka SASL config
func GenerateSASLUsername(name string) string {
	return fmt.Sprintf("%s_%s", name, resources.ScramConsoleUsername)
}

// KafkaSASecretKey returns the NamespacedName of Kafka SA Secret
func KafkaSASecretKey(console *redpandav1alpha1.Console) types.NamespacedName {
	return types.NamespacedName{Namespace: console.GetNamespace(), Name: fmt.Sprintf("%s-%s", console.GetName(), resources.ConsoleSuffix)}
}

// Ensure implements Resource interface
func (k *KafkaSA) Ensure(ctx context.Context) error {
	su := k.superUsersResource
	if err := su.Ensure(ctx); err != nil {
		return fmt.Errorf("ensuring sasl user secret: %w", err)
	}

	// SuperUsers resource generates a username/password credentials
	var secret corev1.Secret
	if err := k.Get(ctx, su.Key(), &secret); err != nil {
		e := fmt.Errorf("fetching Secret (%s) from namespace (%s): %w", su.Key().Name, su.Key().Namespace, err)
		// If created, it may not be available immediately; don't log the error
		// WANT: su.Ensure() should return the object so we don't have to query again
		if apierrors.IsNotFound(err) {
			return &resources.RequeueError{Msg: e.Error()}
		}
		return e
	}
	username := string(secret.Data[corev1.BasicAuthUsernameKey])
	password := string(secret.Data[corev1.BasicAuthPasswordKey])

	adminAPI, err := NewAdminAPI(ctx, k.Client, k.scheme, k.clusterobj, k.clusterDomain, k.adminAPI, k.log)
	if err != nil {
		return err
	}

	if err := adminAPI.CreateUser(ctx, username, password, admin.ScramSha256); err != nil && !strings.Contains(err.Error(), "already exists") {
		// Don't overwhelm Admin API
		return &resources.RequeueAfterError{
			RequeueAfter: resources.RequeueDuration,
			Msg:          fmt.Sprintf("could not create user: %v", err),
		}
	}

	if !controllerutil.ContainsFinalizer(k.consoleobj, ConsoleSAFinalizer) {
		controllerutil.AddFinalizer(k.consoleobj, ConsoleSAFinalizer)
		if err := k.Update(ctx, k.consoleobj); err != nil {
			return err
		}
	}

	return nil
}

// Key implements Resource interface
// But this is not a K8s resource, not implemented
// In the future we might track Kafka SASL users via CR
func (k *KafkaSA) Key() (nsn types.NamespacedName) {
	return nsn
}

// Cleanup implements ManagedResource interface
func (k *KafkaSA) Cleanup(ctx context.Context) error {
	if !controllerutil.ContainsFinalizer(k.consoleobj, ConsoleSAFinalizer) {
		return nil
	}

	adminAPI, err := NewAdminAPI(ctx, k.Client, k.scheme, k.clusterobj, k.clusterDomain, k.adminAPI, k.log)
	if err != nil {
		return err
	}

	if err := adminAPI.DeleteUser(ctx, k.superUsersResource.GetUsername()); err != nil {
		return err
	}
	controllerutil.RemoveFinalizer(k.consoleobj, ConsoleSAFinalizer)
	return k.Update(ctx, k.consoleobj)
}

// KafkaACL is a Console resource
type KafkaACL struct {
	client.Client
	scheme             *runtime.Scheme
	consoleobj         *redpandav1alpha1.Console
	clusterobj         *redpandav1alpha1.Cluster
	kafkaAdmin         KafkaAdminClientFactory
	store              *Store
	superUsersResource *resources.SuperUsersResource
	log                logr.Logger
}

// NewKafkaACL instantiates a new KafkaACL
func NewKafkaACL(
	cl client.Client,
	scheme *runtime.Scheme,
	consoleobj *redpandav1alpha1.Console,
	clusterobj *redpandav1alpha1.Cluster,
	kafkaAdmin KafkaAdminClientFactory,
	store *Store,
	log logr.Logger,
) *KafkaACL {
	su := resources.NewSuperUsers(cl, consoleobj, scheme, GenerateSASLUsername(consoleobj.GetName()), resources.ConsoleSuffix, log)
	return &KafkaACL{
		Client:             cl,
		scheme:             scheme,
		consoleobj:         consoleobj,
		clusterobj:         clusterobj,
		kafkaAdmin:         kafkaAdmin,
		store:              store,
		superUsersResource: su,
		log:                log,
	}
}

// Ensure implements Resource interface
func (k *KafkaACL) Ensure(ctx context.Context) error {
	kadmclient, b, err := k.createAdminClient(ctx)
	if err != nil {
		return fmt.Errorf("creating console sasl user: %w", err)
	}

	results, err := kadmclient.CreateACLs(ctx, b)
	var errList []error
	if err != nil {
		errList = append(errList, err)
	}
	for _, r := range results {
		if r.Err != nil {
			errList = append(errList, r.Err)
		}
	}
	if len(errList) > 0 {
		return fmt.Errorf("creating kafka ACLs: %w", kerrors.NewAggregate(errList))
	}

	if !controllerutil.ContainsFinalizer(k.consoleobj, ConsoleACLFinalizer) {
		controllerutil.AddFinalizer(k.consoleobj, ConsoleACLFinalizer)
		if err := k.Update(ctx, k.consoleobj); err != nil {
			return err
		}
	}

	return nil
}

// Key implements Resource interface
// But this is not a K8s resource, not implemented
// In the future we might track Kafka ACLs via CR
func (k *KafkaACL) Key() (nsn types.NamespacedName) {
	return nsn
}

// Cleanup implements ManagedResource interface
func (k *KafkaACL) Cleanup(ctx context.Context) error {
	if !controllerutil.ContainsFinalizer(k.consoleobj, ConsoleACLFinalizer) {
		return nil
	}

	kadmclient, b, err := k.createAdminClient(ctx)
	if err != nil {
		return fmt.Errorf("cleaning console sasl user: %w", err)
	}

	results, err := kadmclient.DeleteACLs(ctx, b)
	var errList []error
	if err != nil {
		errList = append(errList, err)
	}
	for _, r := range results {
		if r.Err != nil {
			errList = append(errList, r.Err)
		}
	}
	if len(errList) > 0 {
		return fmt.Errorf("deleting kafka ACLs: %w", kerrors.NewAggregate(errList))
	}

	controllerutil.RemoveFinalizer(k.consoleobj, ConsoleACLFinalizer)
	return k.Update(ctx, k.consoleobj)
}

func (k *KafkaACL) createAdminClient(ctx context.Context) (KafkaAdminClient, *kadm.ACLBuilder, error) {
	// Build ACL for console SASL user to access everything
	b := kadm.NewACLs().
		Allow(k.superUsersResource.GetUsername()).AllowHosts().
		Topics("*").Groups("*").Clusters().Operations(kadm.OpAll).
		ResourcePatternType(kadm.ACLPatternLiteral)
	if err := b.ValidateCreate(); err != nil {
		return nil, nil, fmt.Errorf("validating ACLs: %w", err)
	}
	b.PrefixUserExcept()

	kadmclient, err := k.kafkaAdmin(ctx, k.Client, k.clusterobj, k.store)
	if err != nil {
		return nil, nil, fmt.Errorf("creating kafka admin client: %w", err)
	}
	return kadmclient, b, nil
}
