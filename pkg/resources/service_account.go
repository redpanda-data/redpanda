// Copyright 2021 Redpanda Data, Inc.
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
	redpandav1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	k8sclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

var _ Resource = &ServiceAccountResource{}

// ServiceAccountResource is part of the reconciliation of redpanda.vectorized.io CRD
// that gives init container ability to retrieve node external IP by RoleBinding.
type ServiceAccountResource struct {
	k8sclient.Client
	scheme       *runtime.Scheme
	pandaCluster *redpandav1alpha1.Cluster
	logger       logr.Logger
}

// NewServiceAccount creates ServiceAccountResource
func NewServiceAccount(
	client k8sclient.Client,
	pandaCluster *redpandav1alpha1.Cluster,
	scheme *runtime.Scheme,
	logger logr.Logger,
) *ServiceAccountResource {
	return &ServiceAccountResource{
		client,
		scheme,
		pandaCluster,
		logger.WithValues("Kind", serviceAccountKind()),
	}
}

// Ensure manages ServiceAccount that is used in initContainer
func (s *ServiceAccountResource) Ensure(ctx context.Context) error {
	obj, err := s.obj()
	if err != nil {
		return fmt.Errorf("unable to construct ServiceAccount object: %w", err)
	}

	_, err = CreateIfNotExists(ctx, s, obj, s.logger)
	return err
}

// obj returns resource managed client.Object
func (s *ServiceAccountResource) obj() (k8sclient.Object, error) {
	sa := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      s.Key().Name,
			Namespace: s.Key().Namespace,
		},
		TypeMeta: metav1.TypeMeta{
			Kind:       "ServiceAccount",
			APIVersion: "v1",
		},
	}

	err := controllerutil.SetControllerReference(s.pandaCluster, sa, s.scheme)
	if err != nil {
		return nil, err
	}

	return sa, nil
}

// Key returns namespace/name object that is used to identify object.
// For reference please visit types.NamespacedName docs in k8s.io/apimachinery
func (s *ServiceAccountResource) Key() types.NamespacedName {
	return types.NamespacedName{Name: s.pandaCluster.Name, Namespace: s.pandaCluster.Namespace}
}

func serviceAccountKind() string {
	var sa corev1.ServiceAccount
	return sa.Kind
}
