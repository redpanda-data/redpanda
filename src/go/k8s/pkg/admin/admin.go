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

	redpandav1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/api/admin"
	"sigs.k8s.io/controller-runtime/pkg/client"
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
	adminTLSProvider resources.AdminTLSConfigProvider,
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

	var urls []string
	replicas := *redpandaCluster.Spec.Replicas

	for i := int32(0); i < replicas; i++ {
		urls = append(urls, fmt.Sprintf("%s-%d.%s:%d", redpandaCluster.Name, i, fqdn, adminInternalPort))
	}

	adminAPI, err := admin.NewAdminAPI(urls, admin.BasicCredentials{}, tlsConfig)
	if err != nil {
		return nil, fmt.Errorf("error creating admin api for cluster %s/%s using urls %v (tls=%v): %w", redpandaCluster.Namespace, redpandaCluster.Name, urls, tlsConfig != nil, err)
	}
	return adminAPI, nil
}

// AdminAPIClient is a sub interface of the admin API containing what we need in the operator
// nolint:revive // usually package is called adminutils
type AdminAPIClient interface {
	Config(bool) (admin.Config, error)
	ClusterConfigStatus() (admin.ConfigStatusResponse, error)
	ClusterConfigSchema() (admin.ConfigSchema, error)
	PatchClusterConfig(upsert map[string]interface{}, remove []string) (admin.ClusterConfigWriteResult, error)

	CreateUser(username, password, mechanism string) error

	GetFeatures() (admin.FeaturesResponse, error)
}

var _ AdminAPIClient = &admin.AdminAPI{}

// AdminAPIClientFactory is an abstract constructor of admin API clients
// nolint:revive // usually package is called adminutils
type AdminAPIClientFactory func(
	ctx context.Context,
	k8sClient client.Reader,
	redpandaCluster *redpandav1alpha1.Cluster,
	fqdn string,
	adminTLSProvider resources.AdminTLSConfigProvider,
) (AdminAPIClient, error)

var _ AdminAPIClientFactory = NewInternalAdminAPI
