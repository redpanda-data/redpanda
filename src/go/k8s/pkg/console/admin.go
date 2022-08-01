package console

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources/certmanager"
	"k8s.io/apimachinery/pkg/runtime"
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
