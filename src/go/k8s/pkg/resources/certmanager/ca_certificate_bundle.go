// Copyright 2021-2023 Redpanda Data, Inc.
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
	"crypto/x509"
	"encoding/pem"
	"fmt"

	cmmetav1 "github.com/cert-manager/cert-manager/pkg/apis/meta/v1"
	"github.com/go-logr/logr"
	redpandav1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/labels"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	k8sclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

var _ resources.Resource = &CACertificateBundleResource{}

// CACertificateBundleResource is part of the reconciliation of redpanda.vectorized.io CRD
// that creates a secret storing a set of CA certificates.
// The CA certificates will be stored under "ca.crt" in the secret data.
type CACertificateBundleResource struct {
	k8sclient.Client
	scheme       *runtime.Scheme
	pandaCluster *redpandav1alpha1.Cluster
	secrets      []*types.NamespacedName

	certBundleSecret *types.NamespacedName
	logger           logr.Logger
}

// NewCACertificateBundle creates CACertificateBundleResource
func NewCACertificateBundle(
	client k8sclient.Client,
	scheme *runtime.Scheme,
	pandaCluster *redpandav1alpha1.Cluster,
	secrets []*types.NamespacedName,
	caCertBundleSecretSuffix string,
	logger logr.Logger,
) *CACertificateBundleResource {
	return &CACertificateBundleResource{
		client,
		scheme,
		pandaCluster,
		secrets,
		&types.NamespacedName{Name: pandaCluster.Name + "-" + caCertBundleSecretSuffix, Namespace: pandaCluster.Namespace},
		logger.WithValues("Kind", CACertificateBundleKind()),
	}
}

// Ensure manages CACertificateBundleResource
func (s *CACertificateBundleResource) Ensure(ctx context.Context) error {
	s.logger.WithValues("key", s.Key()).Info("reconciling ca certificate bundle resource")

	obj, err := s.obj(ctx)
	if err != nil {
		return fmt.Errorf("unable to construct CACertificateBundleResource object: %w", err)
	}

	created, err := resources.CreateIfNotExists(ctx, s.Client, obj, s.logger)
	if err != nil || created {
		return err
	}

	currentCertBundleSecret := &corev1.Secret{}
	if err = s.Client.Get(ctx, s.Key(), currentCertBundleSecret); err != nil {
		return err
	}

	updated, err := resources.Update(ctx, currentCertBundleSecret, obj, s.Client, s.logger)
	s.logger.WithValues("key", s.Key(), "updated", updated).Info("done reconciling ca certificate bundle resource")
	return err
}

// obj returns resource managed client.Object
func (s *CACertificateBundleResource) obj(ctx context.Context) (k8sclient.Object, error) {
	caCertBundleSecret, err := s.readCACertBundle(ctx)
	if err != nil {
		return nil, err
	}

	err = controllerutil.SetControllerReference(s.pandaCluster, caCertBundleSecret, s.scheme)
	if err != nil {
		return nil, err
	}

	return caCertBundleSecret, nil
}

// Key returns namespace/name object that is used to identify object.
// For reference please visit types.NamespacedName docs in k8s.io/apimachinery
func (s *CACertificateBundleResource) Key() types.NamespacedName {
	return types.NamespacedName{Name: s.certBundleSecret.Name, Namespace: s.certBundleSecret.Namespace}
}

func CACertificateBundleKind() string {
	var cc corev1.Secret
	return cc.Kind
}

func (s *CACertificateBundleResource) readCACertBundle(ctx context.Context) (*corev1.Secret, error) {
	var certBundle []byte
	for _, name := range s.secrets {
		secret := corev1.Secret{}
		err := s.Client.Get(ctx, *name, &secret)
		if err != nil {
			return nil, fmt.Errorf(
				"couldn't retrieve ca secret %q: %w",
				name,
				errCertNotFound,
			)
		}

		// Get CA cert under ca.crt
		val, ok := secret.Data[cmmetav1.TLSCAKey]
		if ok {
			certBundle = append(certBundle, val...)
			continue
		}
		// Look for CA cert under tls.crt as well
		val, ok = secret.Data[corev1.TLSCertKey]
		if !ok {
			continue
		}

		for len(val) > 0 {
			pb, rest := pem.Decode(val)
			if pb == nil {
				break
			}
			cert, err := x509.ParseCertificate(pb.Bytes)
			if err != nil {
				return nil, err
			}
			if cert.IsCA {
				certBundle = append(certBundle, pem.EncodeToMemory(pb)...)
			} else {
				s.logger.Info(fmt.Sprintf("ignore certificate in setting trusted ca: not ca certificate: %+v", cert.Subject))
			}
			val = rest
		}
	}

	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      s.certBundleSecret.Name,
			Namespace: s.certBundleSecret.Namespace,
			Labels:    labels.ForCluster(s.pandaCluster),
		},
		TypeMeta: metav1.TypeMeta{
			Kind:       "Secret",
			APIVersion: "v1",
		},
		Data: map[string][]byte{
			cmmetav1.TLSCAKey: certBundle,
		},
	}, nil
}
