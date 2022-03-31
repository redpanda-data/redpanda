// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package admin

import (
	"context"
	"crypto/tls"
	"crypto/x509"

	cmetav1 "github.com/jetstack/cert-manager/pkg/apis/meta/v1"
	redpandav1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// GetTLSConfig computes crypto/TLS configuration for certificate used by the operator
// during its client authentication.
func GetTLSConfig(
	ctx context.Context,
	k8sClient client.Reader,
	pandaCluster *redpandav1alpha1.Cluster,
	adminAPINodeCertSecretKey client.ObjectKey,
	adminAPIClientCertSecretKey client.ObjectKey,
) (*tls.Config, error) {
	tlsConfig := tls.Config{MinVersion: tls.VersionTLS12} // TLS12 is min version allowed by gosec.

	var nodeCertSecret corev1.Secret
	err := k8sClient.Get(ctx, adminAPINodeCertSecretKey, &nodeCertSecret)
	if err != nil {
		return nil, err
	}

	// Add root CA
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(nodeCertSecret.Data[cmetav1.TLSCAKey])
	tlsConfig.RootCAs = caCertPool

	if pandaCluster.AdminAPITLS() != nil && pandaCluster.AdminAPITLS().TLS.RequireClientAuth {
		var clientCertSecret corev1.Secret
		err := k8sClient.Get(ctx, adminAPIClientCertSecretKey, &clientCertSecret)
		if err != nil {
			return nil, err
		}
		cert, err := tls.X509KeyPair(clientCertSecret.Data[corev1.TLSCertKey], clientCertSecret.Data[corev1.TLSPrivateKeyKey])
		if err != nil {
			return nil, err
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	return &tlsConfig, nil
}
