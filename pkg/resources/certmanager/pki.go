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
	"context"
	"fmt"

	"github.com/go-logr/logr"
	cmmetav1 "github.com/jetstack/cert-manager/pkg/apis/meta/v1"
	redpandav1alpha1 "github.com/vectorizedio/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	"github.com/vectorizedio/redpanda/src/go/k8s/pkg/resources"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	k8sclient "sigs.k8s.io/controller-runtime/pkg/client"
)

var _ resources.Reconciler = &PkiReconciler{}

const (
	// RootCert cert name
	RootCert = "rootcert"
	// OperatorClientCert cert name - used by kubernetes operator to call KafkaAPI
	OperatorClientCert = "operator-client"
	// UserClientCert cert name - used by redpanda clients using KafkaAPI
	UserClientCert = "user-client"
	// AdminClientCert cert name - used by redpanda clients using KafkaAPI
	AdminClientCert = "admin-client"
	// RedpandaNodeCert cert name - node certificate
	RedpandaNodeCert = "redpanda"
	// AdminAPIClientCert cert name - client certificate for Admin API
	AdminAPIClientCert = "admin-api-client"
	// AdminAPINodeCert cert name - node certificate for Admin API
	AdminAPINodeCert = "admin-api-node"
)

// PkiReconciler is part of the reconciliation of redpanda.vectorized.io CRD.
// It creates certificates for Redpanda and its clients when TLS is enabled.
type PkiReconciler struct {
	k8sclient.Client
	scheme       *runtime.Scheme
	pandaCluster *redpandav1alpha1.Cluster
	internalFQDN string
	logger       logr.Logger
}

// NewPki creates PkiReconciler
func NewPki(
	client k8sclient.Client,
	pandaCluster *redpandav1alpha1.Cluster,
	fqdn string,
	scheme *runtime.Scheme,
	logger logr.Logger,
) *PkiReconciler {
	return &PkiReconciler{
		client, scheme, pandaCluster, fqdn, logger.WithValues("Reconciler", "pki"),
	}
}

func (r *PkiReconciler) certNamespacedName(name string) types.NamespacedName {
	return types.NamespacedName{Name: r.pandaCluster.Name + "-" + name, Namespace: r.pandaCluster.Namespace}
}

// NodeCert returns the namespaced name for Redpanda's node certificate
func (r *PkiReconciler) NodeCert() types.NamespacedName {
	if r.pandaCluster.Spec.Configuration.TLS.KafkaAPI.NodeSecretRef != nil {
		return types.NamespacedName{
			Name:      r.pandaCluster.Spec.Configuration.TLS.KafkaAPI.NodeSecretRef.Name,
			Namespace: r.pandaCluster.Namespace,
		}
	}
	return types.NamespacedName{Name: r.pandaCluster.Name + "-" + RedpandaNodeCert, Namespace: r.pandaCluster.Namespace}
}

// OperatorClientCert returns the namespaced name for the client certificate
// used by the Kubernetes operator
func (r *PkiReconciler) OperatorClientCert() types.NamespacedName {
	return types.NamespacedName{Name: r.pandaCluster.Name + "-" + OperatorClientCert, Namespace: r.pandaCluster.Namespace}
}

// AdminCert returns the namespaced name for the certificate used by an administrator to query the Kafka API
func (r *PkiReconciler) AdminCert() types.NamespacedName {
	return types.NamespacedName{Name: r.pandaCluster.Name + "-" + OperatorClientCert, Namespace: r.pandaCluster.Namespace}
}

func (r *PkiReconciler) prepareKafkaAPI(
	ctx context.Context, selfSignedIssuerRef *cmmetav1.ObjectReference,
) ([]resources.Resource, error) {
	toApply := []resources.Resource{}

	externalIssuerRef := r.pandaCluster.Spec.Configuration.TLS.KafkaAPI.IssuerRef
	nodeSecretRef := r.pandaCluster.Spec.Configuration.TLS.KafkaAPI.NodeSecretRef

	if nodeSecretRef == nil {
		// Redpanda cluster certificate for Kafka API - to be provided to each broker
		certsKey := r.certNamespacedName(RedpandaNodeCert)
		nodeIssuerRef := selfSignedIssuerRef
		if externalIssuerRef != nil {
			// if external issuer is provided, we will use it to generate node certificates
			nodeIssuerRef = externalIssuerRef
		}

		dnsName := r.internalFQDN
		externConn := r.pandaCluster.Spec.ExternalConnectivity
		if externConn.Enabled && externConn.Subdomain != "" {
			dnsName = externConn.Subdomain
		}

		redpandaCert := NewNodeCertificate(r.Client, r.scheme, r.pandaCluster, certsKey, nodeIssuerRef, dnsName, false, r.logger)

		toApply = append(toApply, redpandaCert)
	}

	if nodeSecretRef != nil && nodeSecretRef.Namespace != r.pandaCluster.Namespace {
		if err := r.copyNodeSecretToLocalNamespace(ctx, nodeSecretRef); err != nil {
			return nil, err
		}
	}

	if r.pandaCluster.Spec.Configuration.TLS.KafkaAPI.RequireClientAuth {
		// Certificate for external clients to call the Kafka API on any broker in this Redpanda cluster
		certsKey := r.certNamespacedName(UserClientCert)
		externalClientCert := NewCertificate(r.Client, r.scheme, r.pandaCluster, certsKey, selfSignedIssuerRef, fmt.Sprintf("rp-%s", certsKey.Name), false, r.logger)

		// Certificate for operator to call the Kafka API on any broker in this Redpanda cluster
		certsKey = r.certNamespacedName(OperatorClientCert)
		internalClientCert := NewCertificate(r.Client, r.scheme, r.pandaCluster, certsKey, selfSignedIssuerRef, fmt.Sprintf("rp-%s", certsKey.Name), false, r.logger)

		// Certificate for admin to call the Kafka API on any broker in this Redpanda cluster
		certsKey = r.certNamespacedName(AdminClientCert)
		adminClientCert := NewCertificate(r.Client, r.scheme, r.pandaCluster, certsKey, selfSignedIssuerRef, fmt.Sprintf("rp-%s", certsKey.Name), false, r.logger)

		toApply = append(toApply, externalClientCert, internalClientCert, adminClientCert)
	}

	return toApply, nil
}

func (r *PkiReconciler) prepareRoot() (
	[]resources.Resource,
	*cmmetav1.ObjectReference,
) {
	toApply := []resources.Resource{}

	selfSignedKey := r.issuerNamespacedName("selfsigned-issuer")
	selfSignedIssuer := NewIssuer(r.Client,
		r.scheme,
		r.pandaCluster,
		selfSignedKey,
		"",
		r.logger)

	rootCertificateKey := r.certNamespacedName("root-certificate")
	rootCertificate := NewCertificate(r.Client,
		r.scheme,
		r.pandaCluster,
		rootCertificateKey,
		selfSignedIssuer.objRef(),
		rootCertificateKey.String(),
		true,
		r.logger)

	// Kubernetes cluster issuer for Redpanda Operator - key provided in RedpandaCluster CR, else created
	k8sClusterIssuerKey := r.issuerNamespacedName("root-issuer")
	k8sClusterIssuer := NewIssuer(r.Client,
		r.scheme,
		r.pandaCluster,
		k8sClusterIssuerKey,
		rootCertificate.Key().Name,
		r.logger)

	selfSignedIssuerRef := k8sClusterIssuer.objRef()

	toApply = append(toApply, selfSignedIssuer, rootCertificate, k8sClusterIssuer)
	return toApply, selfSignedIssuerRef
}

// Ensure will manage PKI for redpanda.vectorized.io custom resource
func (r *PkiReconciler) Ensure(ctx context.Context) error {
	if !r.pandaCluster.Spec.Configuration.TLS.KafkaAPI.Enabled &&
		!r.pandaCluster.Spec.Configuration.TLS.AdminAPI.Enabled {
		return nil
	}

	toApply, selfSignedIssuerRef := r.prepareRoot()

	if r.pandaCluster.Spec.Configuration.TLS.KafkaAPI.Enabled {
		applyKafka, err := r.prepareKafkaAPI(ctx, selfSignedIssuerRef)
		if err != nil {
			return err
		}
		toApply = append(toApply, applyKafka...)
	}

	if r.pandaCluster.Spec.Configuration.TLS.AdminAPI.Enabled {
		toApply = append(toApply, r.prepareAdminAPI(selfSignedIssuerRef)...)
	}

	for _, res := range toApply {
		err := res.Ensure(ctx)
		if err != nil {
			r.logger.Error(err, "Failed to reconcile pki")
		}
	}

	return nil
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

func (r *PkiReconciler) issuerNamespacedName(name string) types.NamespacedName {
	return types.NamespacedName{Name: r.pandaCluster.Name + "-" + name, Namespace: r.pandaCluster.Namespace}
}
