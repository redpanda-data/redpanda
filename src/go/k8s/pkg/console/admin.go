package console

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"

	"github.com/go-logr/logr"
	redpandav1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	adminutils "github.com/redpanda-data/redpanda/src/go/k8s/pkg/admin"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources/certmanager"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// NewAdminAPI create an Admin API client
func NewAdminAPI(
	ctx context.Context,
	cl client.Client,
	scheme *runtime.Scheme,
	cluster *redpandav1alpha1.Cluster,
	clusterDomain string,
	adminAPI adminutils.AdminAPIClientFactory,
	log logr.Logger,
) (adminutils.AdminAPIClient, error) {
	headlessSvc := resources.NewHeadlessService(cl, cluster, scheme, nil, log)
	clusterSvc := resources.NewClusterService(cl, cluster, scheme, nil, log)
	pki, err := certmanager.NewPki(
		ctx,
		cl,
		cluster,
		headlessSvc.HeadlessServiceFQDN(clusterDomain),
		clusterSvc.ServiceFQDN(clusterDomain),
		scheme,
		log,
	)
	if err != nil {
		return nil, fmt.Errorf("creating PKI: %w", err)
	}
	adminTLSConfigProvider := pki.AdminAPIConfigProvider()
	adminAPIClient, err := adminAPI(
		ctx,
		cl,
		cluster,
		headlessSvc.HeadlessServiceFQDN(clusterDomain),
		adminTLSConfigProvider,
	)
	if err != nil {
		return nil, fmt.Errorf("creating AdminAPIClient: %w", err)
	}
	return adminAPIClient, nil
}

// NewKafkaAdmin create a franz-go admin client
func NewKafkaAdmin(
	ctx context.Context,
	cl client.Client,
	cluster *redpandav1alpha1.Cluster,
	store *Store,
) (KafkaAdminClient, error) {
	opts := []kgo.Opt{kgo.SeedBrokers(getBrokers(cluster)...)}
	if cluster.IsSASLOnInternalEnabled() {
		sasl, err := getSASLOpt(ctx, cl, cluster)
		if err != nil {
			return nil, err
		}
		opts = append(opts, sasl)
	}
	if cluster.KafkaListener().IsMutualTLSEnabled() {
		tlsconfig, err := getTLSConfigOpt(cluster, store)
		if err != nil {
			return nil, err
		}
		opts = append(opts, tlsconfig)
	}

	kclient, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("creating kafka client: %w", err)
	}
	return kadm.NewClient(kclient), nil
}

func getSASLOpt(
	ctx context.Context, cl client.Client, cluster *redpandav1alpha1.Cluster,
) (kgo.Opt, error) {
	// Use Cluster superuser to manage Kafka
	// Console Kafka Service Account can't add ACLs to itself
	su := redpandav1alpha1.SecretKeyRef{
		Namespace: cluster.GetNamespace(),
		Name:      fmt.Sprintf("%s-superuser", cluster.GetName()),
	}

	secret, err := su.GetSecret(ctx, cl)
	if err != nil {
		return nil, err
	}
	user, err := su.GetValue(secret, corev1.BasicAuthUsernameKey)
	if err != nil {
		return nil, err
	}
	pass, err := su.GetValue(secret, corev1.BasicAuthPasswordKey)
	if err != nil {
		return nil, err
	}

	mech := scram.Auth{User: string(user), Pass: string(pass)}
	return kgo.SASL(mech.AsSha256Mechanism()), nil
}

func getTLSConfigOpt(
	cluster *redpandav1alpha1.Cluster, store *Store,
) (kgo.Opt, error) {
	keypair, err := clientCert(cluster, store)
	if err != nil {
		return nil, fmt.Errorf("getting client certificate: %w", err)
	}

	config := &tls.Config{
		Certificates: keypair,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS12,
	}

	ca := &SecretTLSCa{NodeSecretRef: cluster.KafkaListener().TLS.NodeSecretRef}
	if ca.useCaCert() {
		rootca, err := caCert(cluster, store)
		if err != nil {
			return nil, fmt.Errorf("getting root certificate: %w", err)
		}
		config.RootCAs = rootca
	}

	return kgo.DialTLSConfig(config), nil
}

func clientCert(
	cluster *redpandav1alpha1.Cluster, store *Store,
) ([]tls.Certificate, error) {
	clientcert := redpandav1alpha1.SecretKeyRef{}
	secret, exists := store.GetKafkaClientCert(cluster)
	if !exists {
		return nil, fmt.Errorf("not in store") //nolint:goerr113 // no need to declare new error type
	}

	cert, err := clientcert.GetValue(secret, corev1.TLSCertKey)
	if err != nil {
		return nil, err
	}
	key, err := clientcert.GetValue(secret, corev1.TLSPrivateKeyKey)
	if err != nil {
		return nil, err
	}

	keypair, err := tls.X509KeyPair(cert, key)
	if err != nil {
		return nil, fmt.Errorf("parsing certificate: %w", err)
	}
	return []tls.Certificate{keypair}, nil
}

func caCert(
	cluster *redpandav1alpha1.Cluster, store *Store,
) (*x509.CertPool, error) {
	rootca := redpandav1alpha1.SecretKeyRef{}
	secret, exists := store.GetKafkaNodeCert(cluster)
	if !exists {
		return nil, fmt.Errorf("not in store") //nolint:goerr113 // no need to declare new error type
	}

	caRaw, err := rootca.GetValue(secret, corev1.ServiceAccountRootCAKey)
	if err != nil {
		return nil, err
	}
	caBlock, _ := pem.Decode(caRaw)
	ca, err := x509.ParseCertificate(caBlock.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parsing certificate: %w", err)
	}

	pool := x509.NewCertPool()
	pool.AddCert(ca)
	return pool, nil
}
