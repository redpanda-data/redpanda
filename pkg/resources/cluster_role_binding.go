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
	v1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	k8sclient "sigs.k8s.io/controller-runtime/pkg/client"
)

var _ Resource = &ClusterRoleBindingResource{}

// ClusterRoleBindingResource is part of the reconciliation of redpanda.vectorized.io CRD
// that gives init container ability to retrieve node external IP by RoleBinding.
type ClusterRoleBindingResource struct {
	k8sclient.Client
	scheme       *runtime.Scheme
	pandaCluster *redpandav1alpha1.Cluster
	logger       logr.Logger
}

// NewClusterRoleBinding creates ClusterRoleBindingResource
func NewClusterRoleBinding(
	client k8sclient.Client,
	pandaCluster *redpandav1alpha1.Cluster,
	scheme *runtime.Scheme,
	logger logr.Logger,
) *ClusterRoleBindingResource {
	return &ClusterRoleBindingResource{
		client,
		scheme,
		pandaCluster,
		logger.WithValues("Kind", clusterRoleBindingKind()),
	}
}

// Ensure manages v1.ClusterRoleBinding that is assigned to v1.ServiceAccount used in initContainer
func (r *ClusterRoleBindingResource) Ensure(ctx context.Context) error {
	var crb v1.ClusterRoleBinding

	err := r.Get(ctx, r.Key(), &crb)
	if err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("error while fetching ClusterRoleBinding resource: %w", err)
	}

	if errors.IsNotFound(err) {
		r.logger.Info(fmt.Sprintf("ClusterRoleBinding %s does not exist, going to create one", r.Key().Name))

		obj, err := r.Obj()
		if err != nil {
			return fmt.Errorf("unable to construct ClusterRoleBinding object: %w", err)
		}

		if err := r.Create(ctx, obj); err != nil {
			return fmt.Errorf("unable to create ClusterRoleBinding resource: %w", err)
		}

		return nil
	}

	sa := &ServiceAccountResource{pandaCluster: r.pandaCluster}

	found := false

	for i := range crb.Subjects {
		if crb.Subjects[i].Name == sa.Key().Name &&
			crb.Subjects[i].Namespace == sa.Key().Namespace {
			found = true
		}
	}
	if !found {
		crb.Subjects = append(crb.Subjects, v1.Subject{
			Kind:      "ServiceAccount",
			Name:      sa.Key().Name,
			Namespace: sa.Key().Namespace,
		})
		if err := r.Update(ctx, &crb); err != nil {
			return fmt.Errorf("unable to update ClusterRoleBinding: %w", err)
		}
	}
	return nil
}

// Obj returns resource managed client.Object
// The cluster.redpanda.vectorized.io custom resource is namespaced resource, that's
// why v1.ClusterRoleBinding can not have assigned controller reference.
func (r *ClusterRoleBindingResource) Obj() (k8sclient.Object, error) {
	role := &ClusterRoleResource{}
	sa := &ServiceAccountResource{pandaCluster: r.pandaCluster}

	return &v1.ClusterRoleBinding{
		// metav1.ObjectMeta can NOT have namespace set as
		// ClusterRoleBinding is the cluster wide resource.
		ObjectMeta: metav1.ObjectMeta{
			Name:      r.Key().Name,
			Namespace: "",
		},
		TypeMeta: metav1.TypeMeta{
			Kind:       "ClusterRoleBinding",
			APIVersion: "rbac.authorization.k8s.io/v1",
		},
		Subjects: []v1.Subject{
			{
				Kind:      "ServiceAccount",
				Name:      sa.Key().Name,
				Namespace: sa.Key().Namespace,
			},
		},
		RoleRef: v1.RoleRef{
			APIGroup: v1.GroupName,
			Kind:     "ClusterRole",
			Name:     role.Key().Name,
		},
	}, nil
}

// Key returns namespace/name object that is used to identify object.
// For reference please visit types.NamespacedName docs in k8s.io/apimachinery
// Note that Namespace can not be set as this is cluster scoped resource
func (r *ClusterRoleBindingResource) Key() types.NamespacedName {
	return types.NamespacedName{Name: "redpanda-init-configurator", Namespace: ""}
}

// RemoveSubject removes ServiceAccount from the ClusterRoleBinding subject list
func (r *ClusterRoleBindingResource) RemoveSubject(
	ctx context.Context, cluster types.NamespacedName,
) error {
	var crb v1.ClusterRoleBinding

	err := r.Get(ctx, r.Key(), &crb)
	if errors.IsNotFound(err) {
		// if CRB does not exist, there is nothing to remove
		return nil
	}
	if err != nil {
		return fmt.Errorf("error while fetching ClusterRoleBinding resource: %w", err)
	}

	sa := &ServiceAccountResource{
		pandaCluster: &redpandav1alpha1.Cluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:      cluster.Name,
				Namespace: cluster.Namespace,
			},
		},
	}

	k := 0
	for _, s := range crb.Subjects {
		if !(sa.Key().Name == s.Name &&
			sa.Key().Namespace == s.Namespace) {
			crb.Subjects[k] = s
			k++
		}
	}

	crb.Subjects = crb.Subjects[:k]

	if err := r.Update(ctx, &crb); err != nil {
		return fmt.Errorf("unable to update ClusterRoleBinding: %w", err)
	}

	return nil
}

func clusterRoleBindingKind() string {
	var r v1.ClusterRoleBinding
	return r.Kind
}
