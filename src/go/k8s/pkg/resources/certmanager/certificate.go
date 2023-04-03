// Copyright 2021 Redpanda Data, Inc.
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
	"time"

	cmapiv1 "github.com/cert-manager/cert-manager/pkg/apis/certmanager/v1"
	cmetav1 "github.com/cert-manager/cert-manager/pkg/apis/meta/v1"
	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	k8sclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	redpandav1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/labels"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources"
)

var _ resources.Resource = &CertificateResource{}

const (
	// DefaultCertificateDuration default certification duration - 5 years
	DefaultCertificateDuration = 5 * 365 * 24 * time.Hour
	// DefaultRenewBefore default time length prior to expiration to attempt renewal - 90 days
	DefaultRenewBefore = 90 * 24 * time.Hour
	// EmptyCommonName should be used by default because the `common name` field has been deprecated since 2000
	// Also, cert-manager will set the `common name` field to be equal to the first value in the list of SANs.
	EmptyCommonName = CommonName("")
)

// CertificateResource is part of the reconciliation of redpanda.vectorized.io CRD
// creating Certificate from the Issuer resource to have TLS communication supported
type CertificateResource struct {
	k8sclient.Client
	scheme         *runtime.Scheme
	pandaCluster   *redpandav1alpha1.Cluster
	key            types.NamespacedName
	issuerRef      *cmetav1.ObjectReference
	fqdn           []string
	commonName     CommonName
	isCA           bool
	keystoreSecret *types.NamespacedName
	logger         logr.Logger
}

// NewCACertificate creates a root or intermediate CA certificate
func NewCACertificate(
	client k8sclient.Client,
	scheme *runtime.Scheme,
	pandaCluster *redpandav1alpha1.Cluster,
	key types.NamespacedName,
	issuerRef *cmetav1.ObjectReference,
	commonName CommonName,
	keystoreSecret *types.NamespacedName,
	logger logr.Logger,
) *CertificateResource {
	return &CertificateResource{
		client,
		scheme,
		pandaCluster,
		key,
		issuerRef,
		[]string{},
		commonName,
		true,
		keystoreSecret,
		logger.WithValues("Kind", certificateKind()),
	}
}

// NewNodeCertificate creates certificate with given FQDN that is either internal or external
func NewNodeCertificate(
	client k8sclient.Client,
	scheme *runtime.Scheme,
	pandaCluster *redpandav1alpha1.Cluster,
	key types.NamespacedName,
	issuerRef *cmetav1.ObjectReference,
	fqdn []string,
	commonName CommonName,
	keystoreSecret *types.NamespacedName,
	logger logr.Logger,
) *CertificateResource {
	return &CertificateResource{
		client,
		scheme,
		pandaCluster,
		key,
		issuerRef,
		fqdn,
		commonName,
		false,
		keystoreSecret,
		logger.WithValues("Kind", certificateKind()),
	}
}

// NewCertificate creates certificate with given common name
func NewCertificate(
	client k8sclient.Client,
	scheme *runtime.Scheme,
	pandaCluster *redpandav1alpha1.Cluster,
	key types.NamespacedName,
	issuerRef *cmetav1.ObjectReference,
	commonName CommonName,
	isCA bool,
	keystoreSecret *types.NamespacedName,
	logger logr.Logger,
) *CertificateResource {
	return &CertificateResource{
		client,
		scheme,
		pandaCluster,
		key,
		issuerRef,
		[]string{},
		commonName,
		isCA,
		keystoreSecret,
		logger.WithValues("Kind", certificateKind()),
	}
}

// Ensure will manage cert-manager v1.Certificate for redpanda.vectorized.io custom resource
func (r *CertificateResource) Ensure(ctx context.Context) error {
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
		TypeMeta: metav1.TypeMeta{
			Kind:       "Certificate",
			APIVersion: "cert-manager.io/v1",
		},
		Spec: cmapiv1.CertificateSpec{
			SecretName:  r.Key().Name,
			IssuerRef:   *r.issuerRef,
			IsCA:        r.isCA,
			Duration:    &metav1.Duration{Duration: DefaultCertificateDuration},
			RenewBefore: &metav1.Duration{Duration: DefaultRenewBefore},
			Keystores:   r.createKeystores(),
		},
	}

	for _, fqdn := range r.fqdn {
		name := strings.TrimSuffix(fqdn, ".")
		wildname := "*." + name
		cert.Spec.DNSNames = append(cert.Spec.DNSNames, name, wildname)
	}
	cert.Spec.CommonName = string(r.commonName)

	err := controllerutil.SetControllerReference(r.pandaCluster, cert, r.scheme)
	if err != nil {
		return nil, err
	}

	return cert, nil
}

func (r *CertificateResource) createKeystores() *cmapiv1.CertificateKeystores {
	if r.keystoreSecret == nil {
		return nil
	}

	return &cmapiv1.CertificateKeystores{
		JKS: &cmapiv1.JKSKeystore{
			Create: true,
			PasswordSecretRef: cmetav1.SecretKeySelector{
				LocalObjectReference: cmetav1.LocalObjectReference{
					Name: r.keystoreSecret.Name,
				},
				Key: passwordKey,
			},
		},
		PKCS12: &cmapiv1.PKCS12Keystore{
			Create: true,
			PasswordSecretRef: cmetav1.SecretKeySelector{
				LocalObjectReference: cmetav1.LocalObjectReference{
					Name: r.keystoreSecret.Name,
				},
				Key: passwordKey,
			},
		},
	}
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
