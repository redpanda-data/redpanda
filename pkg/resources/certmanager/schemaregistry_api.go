// Copyright 2021 Vectorized, Inc.
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
	cmmeta "github.com/jetstack/cert-manager/pkg/apis/meta/v1"
	"github.com/vectorizedio/redpanda/src/go/k8s/pkg/resources"
	"k8s.io/apimachinery/pkg/types"
)

const (
	schemaRegistryAPI           = "schema-registry"
	schemaRegistryAPIClientCert = "schema-registry-client"
	schemaRegistryAPINodeCert   = "schema-registry-node"
)

// SchemaRegistryAPINodeCert returns the namespaced name for the SchemaRegistry API certificate used by nodes
func (r *PkiReconciler) SchemaRegistryAPINodeCert() types.NamespacedName {
	return types.NamespacedName{Name: r.pandaCluster.Name + "-" + schemaRegistryAPINodeCert, Namespace: r.pandaCluster.Namespace}
}

// SchemaRegistryAPIClientCert returns the namespaced name for the SchemaRegistry API certificate used by clients
func (r *PkiReconciler) SchemaRegistryAPIClientCert() types.NamespacedName {
	return types.NamespacedName{Name: r.pandaCluster.Name + "-" + schemaRegistryAPIClientCert, Namespace: r.pandaCluster.Namespace}
}

func (r *PkiReconciler) prepareSchemaRegistryAPI(
	issuerRef *cmmeta.ObjectReference, keystoreSecret *types.NamespacedName,
) []resources.Resource {
	toApply := []resources.Resource{}

	// Redpanda cluster certificate for SchemaRegistry API - to be provided to each broker
	cn := NewCommonName(r.pandaCluster.Name, schemaRegistryAPINodeCert)
	certsKey := types.NamespacedName{Name: string(cn), Namespace: r.pandaCluster.Namespace}

	dnsName := []string{r.clusterFQDN}
	if r.pandaCluster.Spec.Configuration.SchemaRegistry != nil &&
		r.pandaCluster.Spec.Configuration.SchemaRegistry.External != nil &&
		r.pandaCluster.Spec.Configuration.SchemaRegistry.External.Subdomain != "" {
		dnsName = append(dnsName, r.pandaCluster.Spec.Configuration.SchemaRegistry.External.Subdomain)
	}

	nodeCert := NewNodeCertificate(r.Client, r.scheme, r.pandaCluster, certsKey, issuerRef, dnsName, cn, keystoreSecret, r.logger)
	toApply = append(toApply, nodeCert)

	if r.pandaCluster.Spec.Configuration.SchemaRegistry != nil &&
		r.pandaCluster.Spec.Configuration.SchemaRegistry.TLS != nil &&
		r.pandaCluster.Spec.Configuration.SchemaRegistry.TLS.RequireClientAuth {
		// Certificate for calling the SchemaRegistry API on any broker
		cn := NewCommonName(r.pandaCluster.Name, schemaRegistryAPIClientCert)
		clientCertsKey := types.NamespacedName{Name: string(cn), Namespace: r.pandaCluster.Namespace}
		schemaRegistryClientCert := NewCertificate(r.Client, r.scheme, r.pandaCluster, clientCertsKey, issuerRef, cn, false, keystoreSecret, r.logger)

		toApply = append(toApply, schemaRegistryClientCert)
	}

	return toApply
}
