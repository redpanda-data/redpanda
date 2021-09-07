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
	// OperatorClientCert cert name - used by kubernetes operator to call KafkaAPI
	OperatorClientCert = "operator-client"
	// UserClientCert cert name - used by redpanda clients using KafkaAPI
	UserClientCert = "user-client"
	// AdminClientCert cert name - used by redpanda clients using KafkaAPI
	AdminClientCert = "admin-client"
	// RedpandaNodeCert cert name - node certificate
	RedpandaNodeCert = "redpanda"
)

// OperatorClientCert returns the namespaced name for the client certificate
// used by the Kubernetes operator
func (r *PkiReconciler) OperatorClientCert() types.NamespacedName {
	return types.NamespacedName{Name: r.pandaCluster.Name + "-" + OperatorClientCert, Namespace: r.pandaCluster.Namespace}
}

// AdminCert returns the namespaced name for the certificate used by an administrator to query the Kafka API
func (r *PkiReconciler) AdminCert() types.NamespacedName {
	return types.NamespacedName{Name: r.pandaCluster.Name + "-" + OperatorClientCert, Namespace: r.pandaCluster.Namespace}
}

// NodeCert returns the namespaced name for Redpanda's node certificate
func (r *PkiReconciler) NodeCert() types.NamespacedName {
	tlsListener := r.pandaCluster.KafkaTLSListener()
	if tlsListener != nil && tlsListener.TLS.NodeSecretRef != nil {
		return types.NamespacedName{
			Name:      tlsListener.TLS.NodeSecretRef.Name,
			Namespace: r.pandaCluster.Namespace,
		}
	}
	return types.NamespacedName{Name: r.pandaCluster.Name + "-" + RedpandaNodeCert, Namespace: r.pandaCluster.Namespace}
}

func (r *PkiReconciler) prepareKafkaAPI(
	ctx context.Context,
	issuerRef *cmmetav1.ObjectReference,
	tlsListener *v1alpha1.KafkaAPITLS,
	keystoreSecret *types.NamespacedName,
) ([]resources.Resource, error) {
	toApply := []resources.Resource{}

	externalIssuerRef := tlsListener.IssuerRef
	nodeSecretRef := tlsListener.NodeSecretRef

	if nodeSecretRef == nil {
		certName := NewCertName(r.pandaCluster.Name, RedpandaNodeCert)
		certsKey := types.NamespacedName{Name: string(certName), Namespace: r.pandaCluster.Namespace}
		nodeIssuerRef := issuerRef
		if externalIssuerRef != nil {
			// if external issuer is provided, we will use it to generate node certificates
			nodeIssuerRef = externalIssuerRef
		}

		externalListener := r.pandaCluster.ExternalListener()
		internalListener := r.pandaCluster.InternalListener()
		dnsNames := []string{}
		if internalListener != nil && internalListener.TLS.Enabled {
			dnsNames = append(dnsNames, r.internalFQDN)
		}
		if externalListener != nil && externalListener.TLS.Enabled && externalListener.External.Subdomain != "" {
			dnsNames = append(dnsNames, externalListener.External.Subdomain)
		}
		redpandaCert := NewNodeCertificate(r.Client, r.scheme, r.pandaCluster, certsKey, nodeIssuerRef, dnsNames, EmptyCommonName, keystoreSecret, r.logger)

		toApply = append(toApply, redpandaCert)
	}

	if nodeSecretRef != nil && nodeSecretRef.Namespace != r.pandaCluster.Namespace {
		if err := r.copyNodeSecretToLocalNamespace(ctx, nodeSecretRef); err != nil {
			return nil, err
		}
	}

	if tlsListener.RequireClientAuth {
		// Certificate for external clients to call the Kafka API on any broker in this Redpanda cluster
		userClientCn := NewCommonName(r.pandaCluster.Name, UserClientCert)
		userClientKey := types.NamespacedName{Name: string(userClientCn), Namespace: r.pandaCluster.Namespace}
		externalClientCert := NewCertificate(r.Client, r.scheme, r.pandaCluster, userClientKey, issuerRef, userClientCn, false, keystoreSecret, r.logger)

		// Certificate for operator to call the Kafka API on any broker in this Redpanda cluster
		operatorClientCn := NewCommonName(r.pandaCluster.Name, OperatorClientCert)
		operatorClientKey := types.NamespacedName{Name: string(operatorClientCn), Namespace: r.pandaCluster.Namespace}
		internalClientCert := NewCertificate(r.Client, r.scheme, r.pandaCluster, operatorClientKey, issuerRef, operatorClientCn, false, keystoreSecret, r.logger)

		// Certificate for admin to call the Kafka API on any broker in this Redpanda cluster
		adminClientCn := NewCommonName(r.pandaCluster.Name, AdminClientCert)
		adminClientKey := types.NamespacedName{Name: string(adminClientCn), Namespace: r.pandaCluster.Namespace}
		adminClientCert := NewCertificate(r.Client, r.scheme, r.pandaCluster, adminClientKey, issuerRef, adminClientCn, false, keystoreSecret, r.logger)

		toApply = append(toApply, externalClientCert, internalClientCert, adminClientCert)
	}

	return toApply, nil
}

// Creates copy of secret in Redpanda cluster's namespace
func (r *PkiReconciler) copyNodeSecretToLocalNamespace(
	ctx context.Context, secretRef *corev1.ObjectReference,
) error {
	var secret corev1.Secret
	err := r.Get(ctx, types.NamespacedName{Name: secretRef.Name, Namespace: secretRef.Namespace}, &secret)
	if err != nil {
		return err
	}

	tlsKey := secret.Data[corev1.TLSPrivateKeyKey]
	tlsCrt := secret.Data[corev1.TLSCertKey]
	caCrt := secret.Data[cmmetav1.TLSCAKey]

	caSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      r.NodeCert().Name,
			Namespace: r.NodeCert().Namespace,
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
