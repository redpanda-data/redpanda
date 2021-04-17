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
	"context"

	cmmetav1 "github.com/jetstack/cert-manager/pkg/apis/meta/v1"
	"github.com/vectorizedio/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	"github.com/vectorizedio/redpanda/src/go/k8s/pkg/resources"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

const (
	kafkaAPI = "kafka"
)

func (r *PkiReconciler) prepareKafkaAPI(
	ctx context.Context,
	issuerRef *cmmetav1.ObjectReference,
	tlsListener *v1alpha1.KafkaAPITLS,
) ([]resources.Resource, error) {
	toApply := []resources.Resource{}

	externalIssuerRef := tlsListener.IssuerRef
	nodeSecretRef := tlsListener.NodeSecretRef

	certNames := r.CertificateNames()

	if nodeSecretRef == nil {
		// Redpanda cluster certificate for Kafka API - to be provided to each broker
		nodeCertKey := certNames.NodeCert
		nodeIssuerRef := issuerRef
		if externalIssuerRef != nil {
			// if external issuer is provided, we will use it to generate node certificates
			nodeIssuerRef = externalIssuerRef
		}

		dnsName := r.internalFQDN
		externalListener := r.pandaCluster.ExternalListener()
		if externalListener != nil && externalListener.External.Subdomain != "" {
			dnsName = externalListener.External.Subdomain
		}

		redpandaCert := NewNodeCertificate(r.Client, r.scheme, r.pandaCluster, nodeCertKey, nodeIssuerRef, dnsName, nodeCertKey.Name, false, r.logger)

		toApply = append(toApply, redpandaCert)
	}

	if nodeSecretRef != nil && nodeSecretRef.Namespace != r.pandaCluster.Namespace {
		if err := r.copyNodeSecretToLocalNamespace(ctx, nodeSecretRef.Name, nodeSecretRef.Namespace); err != nil {
			return nil, err
		}
	}

	if tlsListener.RequireClientAuth {
		// Certificate for external clients to call the Kafka API on any broker in this Redpanda cluster
		userClientKey := certNames.UserClientCert
		externalClientCert := NewCertificate(r.Client, r.scheme, r.pandaCluster, userClientKey, issuerRef, userClientKey.Name, false, r.logger)

		// Certificate for operator to call the Kafka API on any broker in this Redpanda cluster
		operatorClientKey := certNames.OperatorClientCert
		internalClientCert := NewCertificate(r.Client, r.scheme, r.pandaCluster, operatorClientKey, issuerRef, operatorClientKey.Name, false, r.logger)

		// Certificate for admin to call the Kafka API on any broker in this Redpanda cluster
		adminClientKey := certNames.AdminClientCert
		adminClientCert := NewCertificate(r.Client, r.scheme, r.pandaCluster, adminClientKey, issuerRef, adminClientKey.Name, false, r.logger)

		toApply = append(toApply, externalClientCert, internalClientCert, adminClientCert)
	}

	return toApply, nil
}

// Creates copy of secret in Redpanda cluster's namespace
func (r *PkiReconciler) copyNodeSecretToLocalNamespace(
	ctx context.Context, secretName, secretNamespace string,
) error {
	var secret corev1.Secret
	err := r.Get(ctx, types.NamespacedName{Name: secretName, Namespace: secretNamespace}, &secret)
	if err != nil {
		return err
	}

	tlsKey := secret.Data[corev1.TLSPrivateKeyKey]
	tlsCrt := secret.Data[corev1.TLSCertKey]
	caCrt := secret.Data[cmmetav1.TLSCAKey]

	caSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: r.pandaCluster.Namespace,
			Labels:    secret.Labels,
		},
		Type: secret.Type,
		Data: map[string][]byte{
			cmmetav1.TLSCAKey:       caCrt,
			corev1.TLSCertKey:       tlsCrt,
			corev1.TLSPrivateKeyKey: tlsKey,
		},
	}
	_, err = resources.CreateIfNotExists(ctx, r, caSecret, r.logger)
	return err
}
