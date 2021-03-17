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
	if err := patch.DefaultAnnotator.SetLastAppliedAnnotation(obj); err != nil {
		return false, fmt.Errorf("unable to add last applied annotation to %s: %w", obj.GetObjectKind().GroupVersionKind().Kind, err)
	}
	err := c.Create(ctx, obj)
	if err != nil && !errors.IsAlreadyExists(err) {
		return false, fmt.Errorf("unable to create %s resource: %w", obj.GetObjectKind().GroupVersionKind().Kind, err)
	}
	if err == nil {
		l.Info(fmt.Sprintf("%s %s did not exist, was created", obj.GetObjectKind().GroupVersionKind().Kind, obj.GetName()))
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
	patchResult, err := patch.DefaultPatchMaker.Calculate(current, modified)
	if err != nil {
		return err
	}
	if !patchResult.IsEmpty() {
		// need to set current version first otherwise the request would get rejected
		metaAccessor := meta.NewAccessor()
		currentVersion, err := metaAccessor.ResourceVersion(current)
		if err != nil {
			return err
		}
		err = metaAccessor.SetResourceVersion(modified, currentVersion)
		if err != nil {
			return err
		}
		logger.Info(fmt.Sprintf("StatefulSet changed, updating %s. Diff: %v", modified.GetName(), string(patchResult.Patch)))
		if err := patch.DefaultAnnotator.SetLastAppliedAnnotation(modified); err != nil {
			return err
		}
		if err := c.Update(ctx, modified); err != nil {
			return fmt.Errorf("failed to update StatefulSet: %w", err)
		}
	}
	return nil
}
