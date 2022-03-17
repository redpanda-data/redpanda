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
	redpandav1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/labels"
	netv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	k8sclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const (
	nginx = "nginx"
	//nolint:gosec // This value does not contain credentials.
	sslPassthroughAnnotation = "nginx.ingress.kubernetes.io/ssl-passthrough"
)

var _ Resource = &IngressResource{}

// IngressResource is part of the reconciliation of redpanda.vectorized.io CRD
// focusing on the internal connectivity management of redpanda cluster
type IngressResource struct {
	k8sclient.Client
	scheme       *runtime.Scheme
	pandaCluster *redpandav1alpha1.Cluster
	host         string
	svcName      string
	svcPortName  string
	logger       logr.Logger
}

// NewIngress creates IngressResource
func NewIngress(
	client k8sclient.Client,
	pandaCluster *redpandav1alpha1.Cluster,
	scheme *runtime.Scheme,
	host string,
	svcName string,
	svcPortName string,
	logger logr.Logger,
) *IngressResource {
	return &IngressResource{
		client,
		scheme,
		pandaCluster,
		host,
		svcName,
		svcPortName,
		logger.WithValues(
			"Kind", ingressKind(),
		),
	}
}

// Ensure will manage kubernetes Ingress for redpanda.vectorized.io custom resource
func (r *IngressResource) Ensure(ctx context.Context) error {
	if r.host == "" {
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
	var ingress netv1.Ingress
	err = r.Get(ctx, r.Key(), &ingress)
	if err != nil {
		return fmt.Errorf("error while fetching Ingress resource: %w", err)
	}
	_, err = Update(ctx, &ingress, obj, r.Client, r.logger)
	return err
}

func (r *IngressResource) obj() (k8sclient.Object, error) {
	objLabels := labels.ForCluster(r.pandaCluster)
	ingressClassName := nginx
	pathTypePrefix := netv1.PathTypePrefix

	ingress := &netv1.Ingress{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Ingress",
			APIVersion: "networking.k8s.io/v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:        r.Key().Name,
			Namespace:   r.Key().Namespace,
			Labels:      objLabels,
			Annotations: map[string]string{sslPassthroughAnnotation: "true"},
		},
		Spec: netv1.IngressSpec{
			IngressClassName: &ingressClassName,
			Rules: []netv1.IngressRule{
				{
					Host: r.host,
					IngressRuleValue: netv1.IngressRuleValue{
						HTTP: &netv1.HTTPIngressRuleValue{
							Paths: []netv1.HTTPIngressPath{
								{
									Path:     "/",
									PathType: &pathTypePrefix,
									Backend: netv1.IngressBackend{
										Service: &netv1.IngressServiceBackend{
											Name: r.svcName,
											Port: netv1.ServiceBackendPort{
												Name: r.svcPortName,
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	err := controllerutil.SetControllerReference(r.pandaCluster, ingress, r.scheme)
	if err != nil {
		return nil, err
	}

	return ingress, nil
}

// Key returns namespace/name object that is used to identify object.
func (r *IngressResource) Key() types.NamespacedName {
	return types.NamespacedName{Name: r.pandaCluster.Name, Namespace: r.pandaCluster.Namespace}
}

func ingressKind() string {
	var obj netv1.Ingress
	return obj.Kind
}
