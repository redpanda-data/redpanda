// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// Package certmanager contains resources for TLS certificate handling using cert-manager
// nolint:dupl // this is similar to the proxy pki
package certmanager

import (
	cmmeta "github.com/jetstack/cert-manager/pkg/apis/meta/v1"
	"github.com/vectorizedio/redpanda/src/go/k8s/pkg/resources"
	"k8s.io/apimachinery/pkg/types"
)

const (
	adminAPI = "admin"
	// AdminAPIClientCert cert name - client certificate for Admin API
	AdminAPIClientCert = "admin-api-client"
	// AdminAPINodeCert cert name - node certificate for Admin API
	AdminAPINodeCert = "admin-api-node"
)

// AdminAPINodeCert returns the namespaced name for the Admin API certificate used by nodes
func (r *PkiReconciler) AdminAPINodeCert() types.NamespacedName {
	return types.NamespacedName{Name: r.pandaCluster.Name + "-" + AdminAPINodeCert, Namespace: r.pandaCluster.Namespace}
}

// AdminAPIClientCert returns the namespaced name for the Admin API certificate used by clients
func (r *PkiReconciler) AdminAPIClientCert() types.NamespacedName {
	return types.NamespacedName{Name: r.pandaCluster.Name + "-" + AdminAPIClientCert, Namespace: r.pandaCluster.Namespace}
}

func (r *PkiReconciler) prepareAdminAPI(
	issuerRef *cmmeta.ObjectReference, keystoreSecret *types.NamespacedName,
) []resources.Resource {
	toApply := []resources.Resource{}

	// Redpanda cluster certificate for Admin API - to be provided to each broker
	cn := NewCommonName(r.pandaCluster.Name, AdminAPINodeCert)
	certsKey := types.NamespacedName{Name: string(cn), Namespace: r.pandaCluster.Namespace}

	dnsName := r.internalFQDN
	externalListener := r.pandaCluster.ExternalListener()
	if externalListener != nil && externalListener.External.Subdomain != "" {
		dnsName = externalListener.External.Subdomain
	}

	nodeCert := NewNodeCertificate(r.Client, r.scheme, r.pandaCluster, certsKey, issuerRef, dnsName, cn, false, keystoreSecret, r.logger)
	toApply = append(toApply, nodeCert)

	if r.pandaCluster.AdminAPITLS() != nil && r.pandaCluster.AdminAPITLS().TLS.RequireClientAuth {
		// Certificate for calling the Admin API on any broker
		cn := NewCommonName(r.pandaCluster.Name, AdminAPIClientCert)
		clientCertsKey := types.NamespacedName{Name: string(cn), Namespace: r.pandaCluster.Namespace}
		adminClientCert := NewCertificate(r.Client, r.scheme, r.pandaCluster, clientCertsKey, issuerRef, cn, false, keystoreSecret, r.logger)

		toApply = append(toApply, adminClientCert)
	}

	return toApply
}
