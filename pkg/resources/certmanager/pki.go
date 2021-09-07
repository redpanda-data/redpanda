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

	"github.com/go-logr/logr"
	cmmetav1 "github.com/jetstack/cert-manager/pkg/apis/meta/v1"
	redpandav1alpha1 "github.com/vectorizedio/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	"github.com/vectorizedio/redpanda/src/go/k8s/pkg/resources"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	k8sclient "sigs.k8s.io/controller-runtime/pkg/client"
)

var _ resources.Reconciler = &PkiReconciler{}

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
	}
}

func (r *PkiReconciler) prepareRoot(
	prefix string,
) ([]resources.Resource, *cmmetav1.ObjectReference) {
	toApply := []resources.Resource{}

	selfSignedIssuer := NewIssuer(r.Client,
		r.scheme,
		r.pandaCluster,
		r.issuerNamespacedName(prefix+"-"+"selfsigned-issuer"),
		"",
		r.logger)

	rootCn := NewCommonName(r.pandaCluster.Name, prefix+"-root-certificate")
	rootKey := types.NamespacedName{Name: string(rootCn), Namespace: r.pandaCluster.Namespace}
	rootCertificate := NewCACertificate(r.Client,
		r.scheme,
		r.pandaCluster,
		rootKey,
		selfSignedIssuer.objRef(),
		rootCn,
		nil,
		r.logger)

	leafIssuer := NewIssuer(r.Client,
		r.scheme,
		r.pandaCluster,
		r.issuerNamespacedName(prefix+"-"+"root-issuer"),
		rootCertificate.Key().Name,
		r.logger)

	leafIssuerRef := leafIssuer.objRef()

	toApply = append(toApply, selfSignedIssuer, rootCertificate, leafIssuer)
	return toApply, leafIssuerRef
}

// Ensure will manage PKI for redpanda.vectorized.io custom resource
func (r *PkiReconciler) Ensure(ctx context.Context) error {
	toApply := []resources.Resource{}
	tlsListener := r.pandaCluster.KafkaTLSListener()

	keystoreKey := types.NamespacedName{Name: keystoreName(r.pandaCluster.Name), Namespace: r.pandaCluster.Namespace}
	keystoreSecret := NewKeystoreSecretResource(r.Client, r.scheme, r.pandaCluster, keystoreKey, r.logger)

	toApply = append(toApply, keystoreSecret)

	if tlsListener != nil {
		toApplyRootKafka, kafkaIssuerRef := r.prepareRoot(kafkaAPI)
		toApplyKafka, err := r.prepareKafkaAPI(ctx, kafkaIssuerRef, &tlsListener.TLS, &keystoreKey)
		if err != nil {
			return err
		}
		toApply = append(toApply, toApplyRootKafka...)
		toApply = append(toApply, toApplyKafka...)
	}

	if r.pandaCluster.AdminAPITLS() != nil {
		toApplyRootAdmin, adminIssuerRef := r.prepareRoot(adminAPI)
		toApply = append(toApply, toApplyRootAdmin...)
		toApply = append(toApply, r.prepareAdminAPI(adminIssuerRef, &keystoreKey)...)
	}

	if r.pandaCluster.PandaproxyAPITLS() != nil {
		toApplyRootPandaproxy, pandaproxyIssuerRef := r.prepareRoot(pandaproxyAPI)
		toApply = append(toApply, toApplyRootPandaproxy...)
		toApply = append(toApply, r.preparePandaproxyAPI(pandaproxyIssuerRef, &keystoreKey)...)
	}

	if r.pandaCluster.Spec.Configuration.SchemaRegistry != nil && r.pandaCluster.Spec.Configuration.SchemaRegistry.TLS != nil {
		toApplyRootPandaproxy, schemaRegistryIssuerRef := r.prepareRoot(schemaRegistryAPI)
		toApply = append(toApply, toApplyRootPandaproxy...)
		toApply = append(toApply, r.prepareSchemaRegistryAPI(schemaRegistryIssuerRef, &keystoreKey)...)
	}

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
