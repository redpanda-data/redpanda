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
	"context"
	"fmt"

	"github.com/go-logr/logr"
	cmapiv1 "github.com/jetstack/cert-manager/pkg/apis/certmanager/v1"
	redpandav1alpha1 "github.com/vectorizedio/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	"github.com/vectorizedio/redpanda/src/go/k8s/pkg/labels"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	k8sclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

var _ Resource = &IssuerResource{}

// IssuerResource is part of the reconciliation of redpanda.vectorized.io CRD
// creating certificate issuer when TLS is enabled
type IssuerResource struct {
	k8sclient.Client
	scheme       *runtime.Scheme
	pandaCluster *redpandav1alpha1.Cluster
	logger       logr.Logger
}

// NewIssuer creates IssuerResource
func NewIssuer(
	client k8sclient.Client,
	pandaCluster *redpandav1alpha1.Cluster,
	scheme *runtime.Scheme,
	logger logr.Logger,
) *IssuerResource {
	return &IssuerResource{
		client, scheme, pandaCluster, logger.WithValues("Kind", issuerKind()),
	}
}

// Ensure will manage cert-manager v1.Issuer for redpanda.vectorized.io custom resource
//nolint:dupl // we expect this to not be duplicated when more logic is added
func (r *IssuerResource) Ensure(ctx context.Context) error {
	if !r.pandaCluster.Spec.Configuration.TLS.KafkaAPIEnabled {
		return nil
	}

	var issuer cmapiv1.Issuer

	err := r.Get(ctx, r.Key(), &issuer)
	if err != nil && !errors.IsNotFound(err) {
		return err
	}

	if errors.IsNotFound(err) {
		r.logger.Info(fmt.Sprintf("Issuer %s does not exist, going to create one", r.Key().Name))

		obj, err := r.Obj()
		if err != nil {
			return err
		}

		return r.Create(ctx, obj)
	}

	return nil
}

// Obj returns resource managed client.Object
func (r *IssuerResource) Obj() (k8sclient.Object, error) {
	objLabels := labels.ForCluster(r.pandaCluster)
	issuer := &cmapiv1.Issuer{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: r.Key().Namespace,
			Name:      r.Key().Name,
			Labels:    objLabels,
		},
		Spec: cmapiv1.IssuerSpec{
			IssuerConfig: cmapiv1.IssuerConfig{
				SelfSigned: &cmapiv1.SelfSignedIssuer{},
			},
		},
	}

	err := controllerutil.SetControllerReference(r.pandaCluster, issuer, r.scheme)
	if err != nil {
		return nil, err
	}

	return issuer, nil
}

// Key returns namespace/name object that is used to identify object.
// For reference please visit types.NamespacedName docs in k8s.io/apimachinery
func (r *IssuerResource) Key() types.NamespacedName {
	return types.NamespacedName{Name: r.pandaCluster.Name, Namespace: r.pandaCluster.Namespace}
}

// Kind returns cert-manager v1.Issuer kind
func (r *IssuerResource) Kind() string {
	return issuerKind()
}

func issuerKind() string {
	var issuer cmapiv1.Issuer
	return issuer.Kind
}
