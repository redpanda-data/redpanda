// Copyright 2021 Redpanda Data, Inc.
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
	redpandav1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	k8sclient "sigs.k8s.io/controller-runtime/pkg/client"
)

var (
	_             resources.Reconciler = &PkiReconciler{}
	errNoDNSNames                      = fmt.Errorf("failed to generate node TLS certificate")
)

// RootCert cert name
const RootCert = "rootcert"

// PkiReconciler is part of the reconciliation of redpanda.vectorized.io CRD.
// It creates certificates for Redpanda and its clients when TLS is enabled.
type PkiReconciler struct {
	k8sclient.Client
	scheme       *runtime.Scheme
	pandaCluster *redpandav1alpha1.Cluster
	internalFQDN string
	clusterFQDN  string
	logger       logr.Logger

	clusterCertificates *ClusterCertificates
}

// NewPki creates PkiReconciler
func NewPki(
	client k8sclient.Client,
	pandaCluster *redpandav1alpha1.Cluster,
	fqdn string,
	clusterFQDN string,
	scheme *runtime.Scheme,
	logger logr.Logger,
) *PkiReconciler {
	return &PkiReconciler{
		client, scheme, pandaCluster, fqdn, clusterFQDN, logger.WithValues("Reconciler", "pki"),
		NewClusterCertificates(pandaCluster, keyStoreKey(pandaCluster), client, fqdn, clusterFQDN, scheme, logger),
	}
}

func keyStoreKey(pandaCluster *redpandav1alpha1.Cluster) types.NamespacedName {
	return types.NamespacedName{Name: keystoreName(pandaCluster.Name), Namespace: pandaCluster.Namespace}
}

// Ensure will manage PKI for redpanda.vectorized.io custom resource
func (r *PkiReconciler) Ensure(ctx context.Context) error {
	toApply := []resources.Resource{}

	keystoreSecret := NewKeystoreSecretResource(r.Client, r.scheme, r.pandaCluster, keyStoreKey(r.pandaCluster), r.logger)

	toApply = append(toApply, keystoreSecret)
	res, err := r.clusterCertificates.Resources(ctx)
	if err != nil {
		return fmt.Errorf("creating resources %w", err)
	}
	toApply = append(toApply, res...)

	for _, res := range toApply {
		err := res.Ensure(ctx)
		if err != nil {
			r.logger.Error(err, "Failed to reconcile pki")
		}
	}

	return nil
}

func (r *PkiReconciler) issuerNamespacedName(name string) types.NamespacedName {
	return types.NamespacedName{Name: r.pandaCluster.Name + "-" + name, Namespace: r.pandaCluster.Namespace}
}

func (r *PkiReconciler) prepareAPI(
	ctx context.Context,
	rootCertSuffix string,
	nodeCertSuffix string,
	clientCerts []string,
	listeners []APIListener,
	keystoreSecret *types.NamespacedName,
) ([]resources.Resource, error) {
	var (
		tlsListener         = getTLSListener(listeners)
		toApply             = []resources.Resource{}
		externalTLSListener = getExternalTLSListener(listeners)
		internalTLSListener = getInternalTLSListener(listeners)
		// Issuer for the nodes
		nodeIssuerRef *cmmetav1.ObjectReference
	)

	if tlsListener == nil || tlsListener.GetTLS() == nil || !tlsListener.GetTLS().Enabled {
		return []resources.Resource{}, nil
	}

	// TODO(#3550): Do not create rootIssuer if nodeSecretRef is passed and mTLS is disabled
	toApplyRoot, rootIssuerRef := r.prepareRoot(rootCertSuffix)
	toApply = append(toApply, toApplyRoot...)
	nodeIssuerRef = rootIssuerRef

	if tlsListener.GetTLS().IssuerRef != nil {
		// if external issuer is provided, we will use it to generate node certificates
		nodeIssuerRef = tlsListener.GetTLS().IssuerRef
	}

	nodeSecretRef := tlsListener.GetTLS().NodeSecretRef
	if nodeSecretRef == nil || nodeSecretRef.Name == "" {
		certName := NewCertName(r.pandaCluster.Name, nodeCertSuffix)
		certsKey := types.NamespacedName{Name: string(certName), Namespace: r.pandaCluster.Namespace}
		dnsNames := []string{}

		if internalTLSListener != nil {
			dnsNames = append(dnsNames, r.clusterFQDN, r.internalFQDN)
		}
		// TODO(#2256): Add support for external listener + TLS certs for IPs
		if externalTLSListener != nil && externalTLSListener.GetExternal().Subdomain != "" {
			dnsNames = append(dnsNames, externalTLSListener.GetExternal().Subdomain)
		}

		if len(dnsNames) == 0 {
			return nil, fmt.Errorf("failed to generate node TLS certificate %s. If external is enabled, please add a subdomain: %w", certName, errNoDNSNames)
		}

		nodeCert := NewNodeCertificate(r.Client, r.scheme, r.pandaCluster, certsKey, nodeIssuerRef, dnsNames, EmptyCommonName, keystoreSecret, r.logger)
		toApply = append(toApply, nodeCert)
	}

	if nodeSecretRef != nil && nodeSecretRef.Name != "" && nodeSecretRef.Namespace != r.pandaCluster.Namespace {
		if err := r.copyNodeSecretToLocalNamespace(ctx, nodeSecretRef); err != nil {
			return nil, fmt.Errorf("copy node secret for %s cert group to namespace %s: %w", rootCertSuffix, nodeSecretRef.Namespace, err)
		}
	}

	if tlsListener.GetTLS().RequireClientAuth {
		for _, clientCertName := range clientCerts {
			clientCn := NewCommonName(r.pandaCluster.Name, clientCertName)
			clientKey := types.NamespacedName{Name: string(clientCn), Namespace: r.pandaCluster.Namespace}
			clientCert := NewCertificate(r.Client, r.scheme, r.pandaCluster, clientKey, rootIssuerRef, clientCn, false, keystoreSecret, r.logger)
			toApply = append(toApply, clientCert)
		}
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
			Name:      secret.Name,
			Namespace: r.pandaCluster.Namespace,
			Labels:    secret.Labels,
		},
		TypeMeta: metav1.TypeMeta{
			Kind:       "Secret",
			APIVersion: "v1",
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
