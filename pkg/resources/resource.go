// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// Package resources contains reconciliation logic for redpanda.vectorized.io CRD
package resources

import (
	"context"
	"fmt"

	"github.com/banzaicloud/k8s-objectmatcher/patch"
	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// KafkaPortName is name of kafka port in Service definition
	KafkaPortName = "kafka"
	// AdminPortName is name of admin port in Service definition
	AdminPortName = "admin"
)

// NamedServicePort allows to pass name ports, e.g., to service resources
type NamedServicePort struct {
	Name string
	Port int
}

// Resource decompose the reconciliation loop to specific kubernetes objects
type Resource interface {
	Reconciler

	// Key returns namespace/name object that is used to identify object.
	// For reference please visit types.NamespacedName docs in k8s.io/apimachinery
	Key() types.NamespacedName
}

// Reconciler implements reconciliation logic
type Reconciler interface {
	// Ensure captures reconciliation logic that can end with error
	Ensure(ctx context.Context) error
}

// CreateIfNotExists tries to get a kubernetes resource and creates it if does not exist
func CreateIfNotExists(
	ctx context.Context, c client.Client, obj client.Object, l logr.Logger,
) (bool, error) {
	// we need to store the GVK before it enters client methods because client
	// wipes GVK. That's a bug in apimachinery, but I don't think it will be
	// fixed any time soon.
	gvk := obj.GetObjectKind().GroupVersionKind()
	if err := patch.DefaultAnnotator.SetLastAppliedAnnotation(obj); err != nil {
		return false, fmt.Errorf("unable to add last applied annotation to %s: %w", obj.GetObjectKind().GroupVersionKind().Kind, err)
	}
	err := c.Create(ctx, obj)
	if err != nil && !errors.IsAlreadyExists(err) {
		return false, fmt.Errorf("unable to create %s resource: %w", obj.GetObjectKind().GroupVersionKind().Kind, err)
	}
	if err == nil {
		l.Info(fmt.Sprintf("%s %s did not exist, was created", gvk.Kind, obj.GetName()))
		return true, nil
	}
	return false, nil
}

// Update ensures resource is updated if necessary. The method calculates patch
// and applies it if something changed
func Update(
	ctx context.Context,
	current runtime.Object,
	modified client.Object,
	c client.Client,
	logger logr.Logger,
) error {
	opts := []patch.CalculateOption{
		patch.IgnoreStatusFields(),
		patch.IgnoreVolumeClaimTemplateTypeMetaAndStatus(),
	}
	patchResult, err := patch.DefaultPatchMaker.Calculate(current, modified, opts...)
	if err != nil {
		return err
	}
	if !patchResult.IsEmpty() {
		// need to set current version first otherwise the request would get rejected
		logger.Info(fmt.Sprintf("Resource changed, updating %s. Diff: %v", modified.GetName(), string(patchResult.Patch)))
		if err := patch.DefaultAnnotator.SetLastAppliedAnnotation(modified); err != nil {
			return err
		}

		metaAccessor := meta.NewAccessor()
		currentVersion, err := metaAccessor.ResourceVersion(current)
		if err != nil {
			return err
		}
		err = metaAccessor.SetResourceVersion(modified, currentVersion)
		if err != nil {
			return err
		}
		if err := c.Update(ctx, modified); err != nil {
			return fmt.Errorf("failed to update resource: %w", err)
		}
	}
	return nil
}
