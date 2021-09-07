// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// Package certmanager contains resources for TLS certificate handling using cert-manager
// nolint:dupl // this is similar to the admin pki
package certmanager

import (
	cmmeta "github.com/jetstack/cert-manager/pkg/apis/meta/v1"
	"github.com/vectorizedio/redpanda/src/go/k8s/pkg/resources"
	"k8s.io/apimachinery/pkg/types"
)

const (
	pandaproxyAPI           = "proxy"
	pandaproxyAPIClientCert = "proxy-api-client"
	pandaproxyAPINodeCert   = "proxy-api-node"
)

// PandaproxyAPINodeCert returns the namespaced name for the Pandaproxy API certificate used by nodes
func (r *PkiReconciler) PandaproxyAPINodeCert() types.NamespacedName {
	return types.NamespacedName{Name: r.pandaCluster.Name + "-" + pandaproxyAPINodeCert, Namespace: r.pandaCluster.Namespace}
}

// PandaproxyAPIClientCert returns the namespaced name for the Pandaproxy API certificate used by clients
func (r *PkiReconciler) PandaproxyAPIClientCert() types.NamespacedName {
	return types.NamespacedName{Name: r.pandaCluster.Name + "-" + pandaproxyAPIClientCert, Namespace: r.pandaCluster.Namespace}
}

func (r *PkiReconciler) preparePandaproxyAPI(
	issuerRef *cmmeta.ObjectReference, keystoreSecret *types.NamespacedName,
) []resources.Resource {
	toApply := []resources.Resource{}

	// Redpanda cluster certificate for Pandaproxy API - to be provided to each broker
	cn := NewCommonName(r.pandaCluster.Name, pandaproxyAPINodeCert)
	certsKey := types.NamespacedName{Name: string(cn), Namespace: r.pandaCluster.Namespace}

	dnsName := []string{r.internalFQDN}
	externalListener := r.pandaCluster.PandaproxyAPIExternal()
	if externalListener != nil && externalListener.External.Subdomain != "" {
		dnsName = append(dnsName, externalListener.External.Subdomain)
	}

	nodeCert := NewNodeCertificate(r.Client, r.scheme, r.pandaCluster, certsKey, issuerRef, dnsName, cn, keystoreSecret, r.logger)
	toApply = append(toApply, nodeCert)

	if r.pandaCluster.PandaproxyAPITLS() != nil && r.pandaCluster.PandaproxyAPITLS().TLS.RequireClientAuth {
		// Certificate for calling the Pandaproxy API on any broker
		cn := NewCommonName(r.pandaCluster.Name, pandaproxyAPIClientCert)
		clientCertsKey := types.NamespacedName{Name: string(cn), Namespace: r.pandaCluster.Namespace}
		proxyClientCert := NewCertificate(r.Client, r.scheme, r.pandaCluster, clientCertsKey, issuerRef, cn, false, keystoreSecret, r.logger)

		toApply = append(toApply, proxyClientCert)
	}

	return toApply
}
