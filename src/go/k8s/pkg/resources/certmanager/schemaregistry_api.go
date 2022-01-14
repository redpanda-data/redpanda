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

import "k8s.io/apimachinery/pkg/types"

const (
	schemaRegistryAPI           = "schema-registry"
	schemaRegistryAPIClientCert = "schema-registry-client"
	schemaRegistryAPINodeCert   = "schema-registry-node"
)

// SchemaRegistryAPINodeCert returns the namespaced name for the SchemaRegistry API certificate used by nodes
func (r *PkiReconciler) SchemaRegistryAPINodeCert() types.NamespacedName {
	schemaRegistryTLSListener := getTLSListener(schemaRegistryAPIListeners(r.pandaCluster))
	if schemaRegistryTLSListener != nil && schemaRegistryTLSListener.GetTLS() != nil && schemaRegistryTLSListener.GetTLS().NodeSecretRef != nil {
		return types.NamespacedName{
			Name:      schemaRegistryTLSListener.GetTLS().NodeSecretRef.Name,
			Namespace: r.pandaCluster.Namespace,
		}
	}
	return types.NamespacedName{Name: r.pandaCluster.Name + "-" + schemaRegistryAPINodeCert, Namespace: r.pandaCluster.Namespace}
}

// SchemaRegistryAPIClientCert returns the namespaced name for the SchemaRegistry API certificate used by clients
func (r *PkiReconciler) SchemaRegistryAPIClientCert() types.NamespacedName {
	return types.NamespacedName{Name: r.pandaCluster.Name + "-" + schemaRegistryAPIClientCert, Namespace: r.pandaCluster.Namespace}
}
