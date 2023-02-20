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
	v1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	k8sclient "sigs.k8s.io/controller-runtime/pkg/client"
)

var _ Resource = &ClusterRoleResource{}

// ClusterRoleResource is part of the reconciliation of redpanda.vectorized.io CRD
// that gives init container ability to retrieve node external IP by RoleBinding.
type ClusterRoleResource struct {
	k8sclient.Client
	scheme       *runtime.Scheme
	pandaCluster *redpandav1alpha1.Cluster
	logger       logr.Logger
}

// NewClusterRole creates ClusterRoleResource
func NewClusterRole(
	client k8sclient.Client,
	pandaCluster *redpandav1alpha1.Cluster,
	scheme *runtime.Scheme,
	logger logr.Logger,
) *ClusterRoleResource {
	return &ClusterRoleResource{
		client,
		scheme,
		pandaCluster,
		logger.WithValues("Kind", clusterRoleKind()),
	}
}

// Ensure manages v1.ClusterRole that is assigned to v1.ServiceAccount used in initContainer
func (r *ClusterRoleResource) Ensure(ctx context.Context) error {
	obj := r.obj()
	created, err := CreateIfNotExists(ctx, r, obj, r.logger)
	if err != nil || created {
		return err
	}
	var cr v1.ClusterRole
	err = r.Get(ctx, r.Key(), &cr)
	if err != nil {
		return fmt.Errorf("error while fetching ClusterRole resource: %w", err)
	}

	_, err = Update(ctx, &cr, obj, r.Client, r.logger)
	return err
}

// obj returns resource managed client.Object
// The cluster.redpanda.vectorized.io custom resource is namespaced resource, that's
// why v1.ClusterRole can not have assigned controller reference.
func (r *ClusterRoleResource) obj() k8sclient.Object {
	return &v1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{
			// metav1.ObjectMeta can NOT have namespace set as
			// ClusterRole is the cluster wide resource.
			Name:      r.Key().Name,
			Namespace: "",
		},
		TypeMeta: metav1.TypeMeta{
			Kind:       "ClusterRole",
			APIVersion: "rbac.authorization.k8s.io/v1",
		},
		Rules: []v1.PolicyRule{
			{
				Verbs:     []string{"get"},
				APIGroups: []string{corev1.GroupName},
				Resources: []string{"nodes"},
			},
		},
	}
}

// Key returns namespace/name object that is used to identify object.
// For reference please visit types.NamespacedName docs in k8s.io/apimachinery
// Note that Namespace can not be set as this is cluster scoped resource
func (r *ClusterRoleResource) Key() types.NamespacedName {
	return types.NamespacedName{Name: "redpanda-init-configurator", Namespace: ""}
}

func clusterRoleKind() string {
	var r v1.ClusterRole
	return r.Kind
}
