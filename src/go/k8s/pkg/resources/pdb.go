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
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/labels"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	k8sclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

var _ Resource = &PDBResource{}

// PDBResource manages PodDisruptionBudget setup for the created redpanda
// cluster
type PDBResource struct {
	k8sclient.Client
	scheme       *runtime.Scheme
	pandaCluster *redpandav1alpha1.Cluster
	logger       logr.Logger
}

// NewPDB creates PDBResource that manages PodDisruptionBudget
func NewPDB(
	client k8sclient.Client,
	pandaCluster *redpandav1alpha1.Cluster,
	scheme *runtime.Scheme,
	logger logr.Logger,
) *PDBResource {
	return &PDBResource{
		client,
		scheme,
		pandaCluster,
		logger.WithValues(
			"Kind", pdbKind(),
		),
	}
}

// Ensure will create/update PDB resource based on configuration inside our CR.
func (r *PDBResource) Ensure(ctx context.Context) error {
	if r.pandaCluster.Spec.PodDisruptionBudget == nil || !r.pandaCluster.Spec.PodDisruptionBudget.Enabled {
		return nil
	}

	obj, err := r.obj()
	if err != nil {
		return fmt.Errorf("unable to construct object: %w", err)
	}
	created, err := CreateIfNotExists(ctx, r, obj, r.logger)
	if err != nil || created {
		return err
	}
	var pdb policyv1.PodDisruptionBudget
	err = r.Get(ctx, r.Key(), &pdb)
	if err != nil {
		return fmt.Errorf("error while fetching Service resource: %w", err)
	}
	_, err = Update(ctx, &pdb, obj, r.Client, r.logger)
	return err
}

func (r *PDBResource) obj() (k8sclient.Object, error) {
	objLabels := labels.ForCluster(r.pandaCluster)
	obj := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:      r.Key().Name,
			Namespace: r.Key().Namespace,
		},
		TypeMeta: metav1.TypeMeta{
			Kind:       "PodDisruptionBudget",
			APIVersion: "policy/v1",
		},
		Spec: policyv1.PodDisruptionBudgetSpec{
			MinAvailable:   r.pandaCluster.Spec.PodDisruptionBudget.MinAvailable,
			MaxUnavailable: r.pandaCluster.Spec.PodDisruptionBudget.MaxUnavailable,
			Selector:       objLabels.AsAPISelector(),
		},
	}

	err := controllerutil.SetControllerReference(r.pandaCluster, obj, r.scheme)
	if err != nil {
		return nil, err
	}

	return obj, nil
}

// Key returns namespace/name object that is used to identify object.
func (r *PDBResource) Key() types.NamespacedName {
	return types.NamespacedName{Name: r.pandaCluster.Name, Namespace: r.pandaCluster.Namespace}
}

func pdbKind() string {
	var obj policyv1.PodDisruptionBudget
	return obj.Kind
}
