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
	"fmt"
	"strings"

	"github.com/go-logr/logr"
	cmapiv1 "github.com/jetstack/cert-manager/pkg/apis/certmanager/v1"
	cmetav1 "github.com/jetstack/cert-manager/pkg/apis/meta/v1"
	redpandav1alpha1 "github.com/vectorizedio/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	"github.com/vectorizedio/redpanda/src/go/k8s/pkg/labels"
	"github.com/vectorizedio/redpanda/src/go/k8s/pkg/resources"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	k8sclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

var _ resources.Resource = &CertificateResource{}

// CAKey filename for root certificate
const CAKey = cmetav1.TLSCAKey

// CertificateResource is part of the reconciliation of redpanda.vectorized.io CRD
// creating Certificate from the Issuer resource to have TLS communication supported
type CertificateResource struct {
	k8sclient.Client
	scheme       *runtime.Scheme
	pandaCluster *redpandav1alpha1.Cluster
	key          types.NamespacedName
	issuerRef    *cmetav1.ObjectReference
	fqdn         string
	isCA         bool
	logger       logr.Logger
}

// NewCertificate creates CertificateResource
func NewCertificate(
	client k8sclient.Client,
	scheme *runtime.Scheme,
	pandaCluster *redpandav1alpha1.Cluster,
	key types.NamespacedName,
	issuerRef *cmetav1.ObjectReference,
	fqdn string,
	isCA bool,
	logger logr.Logger,
) *CertificateResource {
	return &CertificateResource{
		client, scheme, pandaCluster, key, issuerRef, fqdn, isCA, logger.WithValues("Kind", certificateKind()),
	}
}

// Ensure will manage cert-manager v1.Certificate for redpanda.vectorized.io custom resource
func (r *CertificateResource) Ensure(ctx context.Context) error {
	if !r.pandaCluster.Spec.Configuration.TLS.KafkaAPI.Enabled {
		return nil
	}

	obj, err := r.obj()
	if err != nil {
		return fmt.Errorf("unable to construct object: %w", err)
	}

	_, err = resources.CreateIfNotExists(ctx, r, obj, r.logger)
	return err
}

// obj returns resource managed client.Object
func (r *CertificateResource) obj() (k8sclient.Object, error) {
	objLabels := labels.ForCluster(r.pandaCluster)
	cert := &cmapiv1.Certificate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      r.Key().Name,
			Namespace: r.Key().Namespace,
			Labels:    objLabels,
		},
		Spec: cmapiv1.CertificateSpec{
			SecretName: r.Key().Name,
			IssuerRef:  *r.issuerRef,
			IsCA:       r.isCA,
		},
	}

	if r.fqdn != "" {
		cert.Spec.DNSNames = []string{
			"*." + strings.TrimSuffix(r.fqdn, "."),
		}
	} else {
		// Common name cannot exceed 64 bytes (cert-manager validates).
		cert.Spec.CommonName = fmt.Sprintf("rp-%s", r.key.Name)
	}

	err := controllerutil.SetControllerReference(r.pandaCluster, cert, r.scheme)
	if err != nil {
		return nil, err
	}

	return cert, nil
}

// Key returns namespace/name object that is used to identify object.
// For reference please visit types.NamespacedName docs in k8s.io/apimachinery
func (r *CertificateResource) Key() types.NamespacedName {
	return r.key
}

func certificateKind() string {
	var obj cmapiv1.Certificate
	return obj.Kind
}
