// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package certmanager

import "k8s.io/apimachinery/pkg/types"

const (
	kafkaAPI = "kafka"
	// OperatorClientCert cert name - used by kubernetes operator to call KafkaAPI
	OperatorClientCert = "operator-client"
	// UserClientCert cert name - used by redpanda clients using KafkaAPI
	UserClientCert = "user-client"
	// AdminClientCert cert name - used by redpanda clients using KafkaAPI
	AdminClientCert = "admin-client"
	// RedpandaNodeCert cert name - node certificate
	RedpandaNodeCert = "redpanda"
)

// RedpandaOperatorClientCert returns the namespaced name for the client certificate
// used by the Kubernetes operator
func (r *PkiReconciler) RedpandaOperatorClientCert() types.NamespacedName {
	return types.NamespacedName{Name: r.pandaCluster.Name + "-" + OperatorClientCert, Namespace: r.pandaCluster.Namespace}
}

// RedpandaAdminCert returns the namespaced name for the certificate used by an administrator to query the Kafka API
func (r *PkiReconciler) RedpandaAdminCert() types.NamespacedName {
	return types.NamespacedName{Name: r.pandaCluster.Name + "-" + OperatorClientCert, Namespace: r.pandaCluster.Namespace}
}

// RedpandaNodeCert returns the namespaced name for Redpanda's node certificate
func (r *PkiReconciler) RedpandaNodeCert() types.NamespacedName {
	tlsListener := r.pandaCluster.KafkaTLSListener()
	if tlsListener != nil && tlsListener.TLS.NodeSecretRef != nil {
		return types.NamespacedName{
			Name:      tlsListener.TLS.NodeSecretRef.Name,
			Namespace: r.pandaCluster.Namespace,
		}
	}
	return types.NamespacedName{Name: r.pandaCluster.Name + "-" + RedpandaNodeCert, Namespace: r.pandaCluster.Namespace}
}
