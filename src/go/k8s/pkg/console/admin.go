package console

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources/certmanager"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	redpandav1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	adminutils "github.com/redpanda-data/redpanda/src/go/k8s/pkg/admin"
)

// NewAdminAPI create an Admin API client
func NewAdminAPI(ctx context.Context, cl client.Client, scheme *runtime.Scheme, cluster *redpandav1alpha1.Cluster, clusterDomain string, adminAPI adminutils.AdminAPIClientFactory, log logr.Logger) (adminutils.AdminAPIClient, error) {
	headlessSvc := resources.NewHeadlessService(cl, cluster, scheme, nil, log)
	clusterSvc := resources.NewClusterService(cl, cluster, scheme, nil, log)
	pki := certmanager.NewPki(
		cl,
		cluster,
		headlessSvc.HeadlessServiceFQDN(clusterDomain),
		clusterSvc.ServiceFQDN(clusterDomain),
		scheme,
		log,
	)
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

// NewKafkaAdminClient create a franz-go admin client
func NewKafkaAdmin(ctx context.Context, cl client.Client, cluster *redpandav1alpha1.Cluster) (KafkaAdminClient, error) {
	opts := []kgo.Opt{kgo.SeedBrokers(getBrokers(cluster)...)}
	if cluster.Spec.EnableSASL {
		// Use Cluster superuser to manage Kafka
		// Console Kafka Service Account can't add ACLs to itself
		clusterSu := types.NamespacedName{
			Namespace: cluster.GetNamespace(),
			Name:      fmt.Sprintf("%s-superuser", cluster.GetName()),
		}
		secret := v1.Secret{}
		if err := cl.Get(ctx, clusterSu, &secret); err != nil {
			return nil, fmt.Errorf("getting Cluster superuser Secret: %w", err)
		}
		mech := scram.Auth{
			User: string(secret.Data[corev1.BasicAuthUsernameKey]),
			Pass: string(secret.Data[corev1.BasicAuthPasswordKey]),
		}
		opts = append(opts, kgo.SASL(mech.AsSha256Mechanism()))
	}

	kclient, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("creating kafka client: %w", err)
	}

	return kadm.NewClient(kclient), nil
}
