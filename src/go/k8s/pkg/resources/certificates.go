// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package resources

import (
	"fmt"

	"github.com/vectorizedio/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	"k8s.io/apimachinery/pkg/types"
)

const (
	// operatorClientCert cert name - used by kubernetes operator to call KafkaAPI
	operatorClientCert = "operator-client"
	// userClientCert cert name - used by redpanda clients using KafkaAPI
	userClientCert = "user-client"
	// adminClientCert cert name - used by redpanda clients using KafkaAPI
	adminClientCert = "admin-client"
	// redpandaNodeCert cert name - node certificate
	redpandaNodeCert = "redpanda"

	// adminAPIClientCert is client certificate for admin api
	adminAPIClientCert = "admin-api-client"
	// adminAPINodeCert cert name - node certificate for Admin API
	adminAPINodeCert = "admin-api-node"

	// cert-manager has limit of 64 bytes on the common name of certificate
	nameLimit       = 64
	separatorLength = 1 // we use - as separator
)

// Certificates provides namespaced names of all TLS certificates used for redpanda listeners
type Certificates struct {
	NodeCert           types.NamespacedName
	OperatorClientCert types.NamespacedName
	UserClientCert     types.NamespacedName
	AdminClientCert    types.NamespacedName

	AdminAPINodeCert   types.NamespacedName
	AdminAPIClientCert types.NamespacedName
}

// NewCertificates creates new instance of Certificates
func NewCertificates(pandaCluster *v1alpha1.Cluster) *Certificates {
	return &Certificates{
		NodeCert:           nodeCert(pandaCluster),
		OperatorClientCert: CertNameWithSuffix(pandaCluster, operatorClientCert),
		UserClientCert:     CertNameWithSuffix(pandaCluster, userClientCert),
		AdminClientCert:    CertNameWithSuffix(pandaCluster, adminClientCert),

		AdminAPINodeCert:   CertNameWithSuffix(pandaCluster, adminAPINodeCert),
		AdminAPIClientCert: CertNameWithSuffix(pandaCluster, adminAPIClientCert),
	}
}

// nodeCert returns the namespaced name for Redpanda's node certificate
func nodeCert(pandaCluster *v1alpha1.Cluster) types.NamespacedName {
	tlsListener := pandaCluster.KafkaTLSListener()
	if tlsListener != nil && tlsListener.TLS.NodeSecretRef != nil {
		return types.NamespacedName{
			Name:      tlsListener.TLS.NodeSecretRef.Name,
			Namespace: pandaCluster.Namespace,
		}
	}
	return CertNameWithSuffix(pandaCluster, redpandaNodeCert)
}

// CertNameWithSuffix ensures the name does not exceed the limit of 64 bytes. It always
// shortens the cluster name and keeps the whole suffix.
// Suffix and name will be separated with -
func CertNameWithSuffix(
	pandaCluster *v1alpha1.Cluster, suffix string,
) types.NamespacedName {
	return types.NamespacedName{Name: ensureNameLength(pandaCluster.Name, suffix), Namespace: pandaCluster.Namespace}
}

func ensureNameLength(clusterName, suffix string) string {
	suffixLength := len(suffix)
	maxClusterNameLength := nameLimit - suffixLength - separatorLength
	if len(clusterName) > maxClusterNameLength {
		clusterName = clusterName[:maxClusterNameLength]
	}
	return fmt.Sprintf("%s-%s", clusterName, suffix)
}
