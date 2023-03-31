// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// Package admin contains tools for the operator to connect to the admin API
package admin

import (
	"context"
	"crypto/tls"
	"fmt"

	"sigs.k8s.io/controller-runtime/pkg/client"

	redpandav1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources/types"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/api/admin"
)

// NoInternalAdminAPI signal absence of the internal admin API endpoint
type NoInternalAdminAPI struct{}

func (n *NoInternalAdminAPI) Error() string {
	return "no internal admin API defined for cluster"
}

// NewInternalAdminAPI is used to construct an admin API client that talks to the cluster via
// the internal interface.
func NewInternalAdminAPI(
	ctx context.Context,
	k8sClient client.Reader,
	redpandaCluster *redpandav1alpha1.Cluster,
	fqdn string,
	adminTLSProvider types.AdminTLSConfigProvider,
	ordinals ...int32,
) (AdminAPIClient, error) {
	adminInternal := redpandaCluster.AdminAPIInternal()
	if adminInternal == nil {
		return nil, &NoInternalAdminAPI{}
	}

	var tlsConfig *tls.Config
	if adminInternal.TLS.Enabled {
		var err error
		tlsConfig, err = adminTLSProvider.GetTLSConfig(ctx, k8sClient)
		if err != nil {
			return nil, fmt.Errorf("could not create tls configuration for internal admin API: %w", err)
		}
	}

	adminInternalPort := adminInternal.Port

	if len(ordinals) == 0 {
		// Not a specific node, just go through all them
		replicas := redpandaCluster.GetCurrentReplicas()

		for i := int32(0); i < replicas; i++ {
			ordinals = append(ordinals, i)
		}
	}
	urls := make([]string, 0, len(ordinals))
	for _, on := range ordinals {
		urls = append(urls, fmt.Sprintf("%s-%d.%s:%d", redpandaCluster.Name, on, fqdn, adminInternalPort))
	}

	adminAPI, err := admin.NewAdminAPI(urls, admin.BasicCredentials{}, tlsConfig)
	if err != nil {
		return nil, fmt.Errorf("error creating admin api for cluster %s/%s using urls %v (tls=%v): %w", redpandaCluster.Namespace, redpandaCluster.Name, urls, tlsConfig != nil, err)
	}

	return adminAPI, nil
}

// AdminAPIClient is a sub interface of the admin API containing what we need in the operator
//

type AdminAPIClient interface {
	Config(ctx context.Context, includeDefaults bool) (admin.Config, error)
	ClusterConfigStatus(ctx context.Context, sendToLeader bool) (admin.ConfigStatusResponse, error)
	ClusterConfigSchema(ctx context.Context) (admin.ConfigSchema, error)
	PatchClusterConfig(ctx context.Context, upsert map[string]interface{}, remove []string) (admin.ClusterConfigWriteResult, error)
	GetNodeConfig(ctx context.Context) (admin.NodeConfig, error)

	CreateUser(ctx context.Context, username, password, mechanism string) error
	ListUsers(ctx context.Context) ([]string, error)
	DeleteUser(ctx context.Context, username string) error
	UpdateUser(ctx context.Context, username, password, mechanism string) error

	GetFeatures(ctx context.Context) (admin.FeaturesResponse, error)
	SetLicense(ctx context.Context, license interface{}) error
	GetLicenseInfo(ctx context.Context) (admin.License, error)

	Brokers(ctx context.Context) ([]admin.Broker, error)
	Broker(ctx context.Context, nodeID int) (admin.Broker, error)
	DecommissionBroker(ctx context.Context, node int) error
	RecommissionBroker(ctx context.Context, node int) error

	EnableMaintenanceMode(ctx context.Context, node int) error
	DisableMaintenanceMode(ctx context.Context, node int) error

	GetHealthOverview(ctx context.Context) (admin.ClusterHealthOverview, error)
}

var _ AdminAPIClient = &admin.AdminAPI{}

// AdminAPIClientFactory is an abstract constructor of admin API clients
//

type AdminAPIClientFactory func(
	ctx context.Context,
	k8sClient client.Reader,
	redpandaCluster *redpandav1alpha1.Cluster,
	fqdn string,
	adminTLSProvider types.AdminTLSConfigProvider,
	ordinals ...int32,
) (AdminAPIClient, error)

var _ AdminAPIClientFactory = NewInternalAdminAPI
