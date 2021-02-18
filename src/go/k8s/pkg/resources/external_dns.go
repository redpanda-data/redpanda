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
	"reflect"

	"github.com/go-logr/logr"
	redpandav1alpha1 "github.com/vectorizedio/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	"github.com/vectorizedio/redpanda/src/go/k8s/pkg/labels"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	k8sclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/scheme"
	"sigs.k8s.io/external-dns/endpoint"
)

var (
	// GroupVersion is group version used to register these objects
	GroupVersion	= schema.GroupVersion{Group: "externaldns.vectorized.io", Version: "v1alpha1"}

	// SchemeBuilder is used to add go types to the GroupVersionKind scheme
	SchemeBuilder	= &scheme.Builder{GroupVersion: GroupVersion}

	// AddToScheme adds the types in this group-version to the given scheme.
	AddToScheme	= SchemeBuilder.AddToScheme
)

func init() {
	SchemeBuilder.Register(&endpoint.DNSEndpoint{}, &endpoint.DNSEndpointList{})
}

var _ Resource = &externalDNS{}

// externalDNS is part of the reconciliation of redpanda.vectorized.io CRD
// focusing on the external connectivity management of redpanda cluster
type externalDNS struct {
	k8sclient.Client
	scheme		*runtime.Scheme
	pandaCluster	*redpandav1alpha1.Cluster
	logger		logr.Logger

	endpoints	[]*endpoint.Endpoint
}

// NewExternalDNS creates externalDNS
func NewExternalDNS(
	client k8sclient.Client,
	pandaCluster *redpandav1alpha1.Cluster,
	crlScheme *runtime.Scheme,
	logger logr.Logger,
) Resource {
	return &externalDNS{
		client,
		crlScheme,
		pandaCluster,
		logger.WithValues("Kind", dnsEndpoint()),
		nil,
	}
}

// Ensure will manage external dns endpoint.DNSEndpoint for redpanda.vectorized.io custom resource
func (r *externalDNS) Ensure(ctx context.Context) error {
	if !r.pandaCluster.Spec.ExternalConnectivity {
		return nil
	}

	var observedPods corev1.PodList

	err := r.List(ctx, &observedPods, &k8sclient.ListOptions{
		LabelSelector:	labels.ForCluster(r.pandaCluster).AsClientSelector(),
		Namespace:	r.pandaCluster.Namespace,
	})
	if err != nil {
		return fmt.Errorf("unable to list pods: %w", err)
	}

	var node corev1.Node

	for index := range observedPods.Items {
		pod := &observedPods.Items[index]
		if pod.Spec.NodeName == "" {
			continue
		}

		if err = r.Get(ctx, types.NamespacedName{Name: pod.Spec.NodeName}, &node); err != nil {
			return fmt.Errorf("unable to featch node info: %w", err)
		}

		r.endpoints = append(r.endpoints, &endpoint.Endpoint{
			DNSName:	fmt.Sprintf("%s.%s.%s.%s", pod.Name, r.pandaCluster.Name, r.pandaCluster.Namespace, r.pandaCluster.Spec.ExternalDNSSubdomain),
			Targets: endpoint.Targets{
				getExternalIP(node.Status.Addresses),
			},
			RecordType:	endpoint.RecordTypeA,
			RecordTTL:	180,
		})
	}

	var dnsEndpoint endpoint.DNSEndpoint

	err = r.Get(ctx, r.Key(), &dnsEndpoint)
	if err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("unable to featch dns endpoint: %w", err)
	}

	if errors.IsNotFound(err) {
		r.logger.Info(fmt.Sprintf("External DNS %s does not exist, going to create one", r.Key().Name))

		obj, err := r.Obj()
		if err != nil {
			return fmt.Errorf("unable to construct an dns endpoint with %d endpoints: %w", len(r.endpoints), err)
		}

		if err := r.Create(ctx, obj); err != nil {
			return fmt.Errorf("unable to create dns endpoint with %d endpoints: %w", len(r.endpoints), err)
		}

		return nil
	}

	if !reflect.DeepEqual(dnsEndpoint.Spec.Endpoints, r.endpoints) {
		dnsEndpoint.Spec.Endpoints = r.endpoints
		if err := r.Update(ctx, &dnsEndpoint); err != nil {
			return fmt.Errorf("unable to update dns endpoint with %d endpoints: %w", len(r.endpoints), err)
		}
	}

	return nil
}

func getExternalIP(addresses []corev1.NodeAddress) string {
	for _, addr := range addresses {
		if addr.Type == corev1.NodeExternalIP {
			return addr.Address
		}
	}

	return ""
}

// Obj returns resource managed client.Object
func (r *externalDNS) Obj() (k8sclient.Object, error) {
	objLabels := labels.ForCluster(r.pandaCluster)
	dnsEndpoint := &endpoint.DNSEndpoint{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:	r.Key().Namespace,
			Name:		r.Key().Name,
			Labels:		objLabels,
		},
		Spec: endpoint.DNSEndpointSpec{
			Endpoints: r.endpoints,
		},
	}

	err := controllerutil.SetControllerReference(r.pandaCluster, dnsEndpoint, r.scheme)
	if err != nil {
		return nil, err
	}

	return dnsEndpoint, nil
}

// Key returns namespace/name object that is used to identify object.
// For reference please visit types.NamespacedName docs in k8s.io/apimachinery
func (r *externalDNS) Key() types.NamespacedName {
	return types.NamespacedName{Name: r.pandaCluster.Name, Namespace: r.pandaCluster.Namespace}
}

// Kind returns endpoint.DNSEndpoint kind
func (r *externalDNS) Kind() string {
	return dnsEndpoint()
}

func dnsEndpoint() string {
	var dns endpoint.DNSEndpoint
	return dns.Kind
}
