// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// Package certmanager contains resources for TLS certificate handling using cert-manager
package certmanager

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	k8sclient "sigs.k8s.io/controller-runtime/pkg/client"

	redpandav1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources"
	resourcetypes "github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources/types"
)

var _ resources.Reconciler = &PkiReconciler{}

// RootCert cert name
const RootCert = "rootcert"

// PkiReconciler is part of the reconciliation of redpanda.vectorized.io CRD.
// It creates certificates for Redpanda and its clients when TLS is enabled.
type PkiReconciler struct {
	k8sclient.Client
	scheme       *runtime.Scheme
	pandaCluster *redpandav1alpha1.Cluster
	internalFQDN string
	clusterFQDN  string
	logger       logr.Logger

	clusterCertificates *ClusterCertificates
}

// NewPki creates PkiReconciler
func NewPki(
	ctx context.Context,
	client k8sclient.Client,
	pandaCluster *redpandav1alpha1.Cluster,
	fqdn string,
	clusterFQDN string,
	scheme *runtime.Scheme,
	logger logr.Logger,
) (*PkiReconciler, error) {
	cc, err := NewClusterCertificates(ctx, pandaCluster, keyStoreKey(pandaCluster), client, fqdn, clusterFQDN, scheme, logger)
	if err != nil {
		return nil, err
	}
	return &PkiReconciler{
		client, scheme, pandaCluster, fqdn, clusterFQDN, logger.WithValues("Reconciler", "pki"),
		cc,
	}, nil
}

func keyStoreKey(pandaCluster *redpandav1alpha1.Cluster) types.NamespacedName {
	return types.NamespacedName{Name: keystoreName(pandaCluster.Name), Namespace: pandaCluster.Namespace}
}

// Ensure will manage PKI for redpanda.vectorized.io custom resource
func (r *PkiReconciler) Ensure(ctx context.Context) error {
	toApply := []resources.Resource{}

	keystoreSecret := NewKeystoreSecretResource(r.Client, r.scheme, r.pandaCluster, keyStoreKey(r.pandaCluster), r.logger)

	toApply = append(toApply, keystoreSecret)
	res, err := r.clusterCertificates.Resources(ctx)
	if err != nil {
		return fmt.Errorf("creating resources %w", err)
	}
	toApply = append(toApply, res...)

	for _, res := range toApply {
		err := res.Ensure(ctx)
		if err != nil {
			r.logger.Error(err, "Failed to reconcile pki")
		}
	}

	return nil
}

// StatefulSetVolumeProvider returns volume provider for all TLS certificates
func (r *PkiReconciler) StatefulSetVolumeProvider() resourcetypes.StatefulsetTLSVolumeProvider {
	return r.clusterCertificates
}

// AdminAPIConfigProvider returns provider of admin TLS configuration
func (r *PkiReconciler) AdminAPIConfigProvider() resourcetypes.AdminTLSConfigProvider {
	return r.clusterCertificates
}

// BrokerTLSConfigProvider returns provider of broker TLS
func (r *PkiReconciler) BrokerTLSConfigProvider() resourcetypes.BrokerTLSConfigProvider {
	return r.clusterCertificates
}
