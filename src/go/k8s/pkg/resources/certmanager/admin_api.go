// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package certmanager

import (
	cmmeta "github.com/jetstack/cert-manager/pkg/apis/meta/v1"
	"github.com/vectorizedio/redpanda/src/go/k8s/pkg/resources"
)

const (
	adminAPI = "admin"
)

func (r *PkiReconciler) prepareAdminAPI(
	issuerRef *cmmeta.ObjectReference,
) []resources.Resource {
	toApply := []resources.Resource{}

	// Redpanda cluster certificate for Admin API - to be provided to each broker
	certNames := r.CertificateNames()
	adminAPIKey := certNames.AdminAPINodeCert

	dnsName := r.internalFQDN
	externalListener := r.pandaCluster.ExternalListener()
	if externalListener != nil && externalListener.External.Subdomain != "" {
		dnsName = externalListener.External.Subdomain
	}

	nodeCert := NewNodeCertificate(r.Client, r.scheme, r.pandaCluster, adminAPIKey, issuerRef, dnsName, adminAPIKey.Name, false, r.logger)
	toApply = append(toApply, nodeCert)

	if r.pandaCluster.AdminAPITLS() != nil && r.pandaCluster.AdminAPITLS().TLS.RequireClientAuth {
		// Certificate for calling the Admin API on any broker
		adminAPIClientKey := certNames.AdminAPIClientCert
		adminClientCert := NewCertificate(r.Client, r.scheme, r.pandaCluster, adminAPIClientKey, issuerRef, adminAPIClientKey.Name, false, r.logger)

		toApply = append(toApply, adminClientCert)
	}

	return toApply
}
