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
	adminAPI           = "admin"
	adminAPIClientCert = "admin-api-client"
	adminAPINodeCert   = "admin-api-node"
)

// AdminAPINodeCert returns the namespaced name for the Admin API certificate used by nodes
func (r *PkiReconciler) AdminAPINodeCert() types.NamespacedName {
	return types.NamespacedName{Name: r.pandaCluster.Name + "-" + adminAPINodeCert, Namespace: r.pandaCluster.Namespace}
}

// AdminAPIClientCert returns the namespaced name for the Admin API certificate used by clients
func (r *PkiReconciler) AdminAPIClientCert() types.NamespacedName {
	return types.NamespacedName{Name: r.pandaCluster.Name + "-" + adminAPIClientCert, Namespace: r.pandaCluster.Namespace}
}
