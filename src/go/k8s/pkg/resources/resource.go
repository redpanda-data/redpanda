// Copyright 2021 Redpanda Data, Inc.
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

	"github.com/cisco-open/k8s-objectmatcher/patch"
	"github.com/go-logr/logr"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/utils"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// AdminPortName is name of admin port in Service definition
	AdminPortName = "admin"
	// AdminPortExternalName is name of external admin port
	AdminPortExternalName = "admin-external"
	// InternalListenerName is name of internal listener
	InternalListenerName = "kafka"
	// ExternalListenerName is name of external listener
	ExternalListenerName = "kafka-external"
	// ExternalListenerBootstrapName is the name of external bootstrap service
	ExternalListenerBootstrapName = "kafka-external-bootstrap"
	// PandaproxyPortInternalName is the name of the pandaproxy internal port
	PandaproxyPortInternalName = "proxy"
	// PandaproxyPortExternalName is the name of the pandaproxy external port
	PandaproxyPortExternalName = "proxy-external"
	// SchemaRegistryPortName is the name of the schema registry port internal and external
	SchemaRegistryPortName = "schema-registry"

	scramPasswordLength = 16

	separator = "-"

	redpandaAnnotatorKey = "redpanda.com/last-applied"
)

// NamedServicePort allows to pass name ports, e.g., to service resources
type NamedServicePort struct {
	Name       string
	Port       int
	TargetPort int
}

// NamedServiceNodePort allows to specify which nodeports should be created
type NamedServiceNodePort struct {
	NamedServicePort
	GenerateNodePort bool
}

// Resource decompose the reconciliation loop to specific kubernetes objects
type Resource interface {
	Reconciler

	// Key returns namespace/name object that is used to identify object.
	// For reference please visit types.NamespacedName docs in k8s.io/apimachinery
	Key() types.NamespacedName
}

// ManagedResource is similar to Resource but with proper cleanup on deletion
type ManagedResource interface {
	Resource

	// Cleanup properly deletes the resource by removing attached finalizers
	Cleanup(context.Context) error
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
	annotator := patch.NewAnnotator(redpandaAnnotatorKey)
	gvk := obj.GetObjectKind().GroupVersionKind()
	if err := annotator.SetLastAppliedAnnotation(obj); err != nil {
		return false, fmt.Errorf("unable to add last applied annotation to %s: %w", obj.GetObjectKind().GroupVersionKind().Kind, err)
	}
	// this is needed because we cannot pass obj into get as the client methods
	// modify the input received and we don't want to mutate the object passed
	// in
	actual := &unstructured.Unstructured{}
	actual.SetGroupVersionKind(gvk)
	err := c.Get(ctx, types.NamespacedName{
		Namespace: obj.GetNamespace(),
		Name:      obj.GetName(),
	}, actual)
	if err != nil && !errors.IsNotFound(err) {
		return false, fmt.Errorf("error while fetching %s resource kind %+v: %w", obj.GetName(), obj.GetObjectKind().GroupVersionKind().Kind, err)
	}
	if errors.IsNotFound(err) {
		// not exists, going to create it
		err = c.Create(ctx, obj)
		if err != nil && !errors.IsAlreadyExists(err) {
			return false, fmt.Errorf("unable to create %s resource: %w", obj.GetObjectKind().GroupVersionKind().Kind, err)
		}
		if err == nil {
			l.Info(fmt.Sprintf("%s %s did not exist, was created", gvk.Kind, obj.GetName()))
			return true, nil
		}
	}
	return false, nil
}

// Update ensures resource is updated if necessary. The method calculates patch
// and applies it if something changed
// returns true if resource was updated
func Update(
	ctx context.Context,
	current client.Object,
	modified client.Object,
	c client.Client,
	logger logr.Logger,
) (bool, error) {
	prepareResourceForPatch(current, modified)
	opts := []patch.CalculateOption{
		patch.IgnoreStatusFields(),
		patch.IgnoreVolumeClaimTemplateTypeMetaAndStatus(),
		patch.IgnorePDBSelector(),
		utils.IgnoreAnnotation(redpandaAnnotatorKey),
		utils.IgnoreAnnotation(LastAppliedConfigurationAnnotationKey),
	}
	annotator := patch.NewAnnotator(redpandaAnnotatorKey)
	patchResult, err := patch.NewPatchMaker(annotator, &patch.K8sStrategicMergePatcher{}, &patch.BaseJSONMergePatcher{}).Calculate(current, modified, opts...)
	if err != nil {
		return false, err
	}
	if !patchResult.IsEmpty() {
		// need to set current version first otherwise the request would get rejected
		logger.Info(fmt.Sprintf("Resource %s (%s) changed, updating. Diff: %v",
			modified.GetName(), modified.GetObjectKind().GroupVersionKind().Kind, string(patchResult.Patch)))
		if err := annotator.SetLastAppliedAnnotation(modified); err != nil {
			return false, err
		}

		metaAccessor := meta.NewAccessor()
		currentVersion, err := metaAccessor.ResourceVersion(current)
		if err != nil {
			return false, err
		}
		err = metaAccessor.SetResourceVersion(modified, currentVersion)
		if err != nil {
			return false, err
		}
		prepareResourceForUpdate(current, modified)
		if err := c.Update(ctx, modified); err != nil {
			return false, fmt.Errorf("failed to update resource: %w", err)
		}
		return true, nil
	}
	return false, nil
}

// DeleteIfExists can delete a resource if present on the cluster
func DeleteIfExists(
	ctx context.Context, obj client.Object, c client.Client,
) error {
	err := c.Delete(ctx, obj)
	if err != nil && !errors.IsNotFound(err) {
		return err
	}
	return nil
}

// normalization to be done on resource before the patch is computed
func prepareResourceForPatch(current runtime.Object, modified client.Object) {
	// when object is get from client via client.Get, GVK is wiped. To prevent
	// the diff being generated by one object having GVK and other not, we make
	// sure that the empty GVK is reset to the one of the modified object
	if current.GetObjectKind().GroupVersionKind() == schema.EmptyObjectKind.GroupVersionKind() &&
		modified.GetObjectKind().GroupVersionKind() != schema.EmptyObjectKind.GroupVersionKind() {
		current.GetObjectKind().SetGroupVersionKind(modified.GetObjectKind().GroupVersionKind())
	}
}

// reference
// https://github.com/banzaicloud/istio-operator/blob/8bd406e14079ce43fe1908403bb2892c2549ab1e/pkg/k8sutil/resource.go#L180
func prepareResourceForUpdate(current runtime.Object, modified client.Object) {
	switch t := modified.(type) {
	case *corev1.Service:
		svc := t
		svc.Spec.ClusterIP = current.(*corev1.Service).Spec.ClusterIP
	case *corev1.ServiceAccount:
		sa := t
		sa.Secrets = current.(*corev1.ServiceAccount).Secrets
	// Additional cases due to controller using update instead of patch
	case *corev1.ConfigMap:
		cm := t
		if ann, ok := current.(*corev1.ConfigMap).Annotations[LastAppliedConfigurationAnnotationKey]; ok {
			// We always ignore this annotation during normal reconciliation
			if cm.Annotations == nil {
				cm.Annotations = make(map[string]string)
			}
			cm.Annotations[LastAppliedConfigurationAnnotationKey] = ann
		}
	}
}

func resourceNameTrim(clusterName, suffix string) string {
	suffixLength := len(suffix)
	maxClusterNameLength := validation.DNS1123SubdomainMaxLength - suffixLength - len(separator)
	if len(clusterName) > maxClusterNameLength {
		clusterName = clusterName[:maxClusterNameLength]
	}
	return fmt.Sprintf("%s%s%s", clusterName, separator, suffix)
}
