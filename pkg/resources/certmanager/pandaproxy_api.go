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
