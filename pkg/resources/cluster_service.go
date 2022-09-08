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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	k8sclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

var _ Resource = &ClusterServiceResource{}

// ClusterServiceResource is part of the reconciliation of redpanda.vectorized.io CRD
// focusing on the internal connectivity management of redpanda cluster
type ClusterServiceResource struct {
	k8sclient.Client
	scheme       *runtime.Scheme
	pandaCluster *redpandav1alpha1.Cluster
	svcPorts     []NamedServicePort
	logger       logr.Logger
}

// NewClusterService creates ClusterServiceResource
func NewClusterService(
	client k8sclient.Client,
	pandaCluster *redpandav1alpha1.Cluster,
	scheme *runtime.Scheme,
	svcPorts []NamedServicePort,
	logger logr.Logger,
) *ClusterServiceResource {
	return &ClusterServiceResource{
		client,
		scheme,
		pandaCluster,
		svcPorts,
		logger.WithValues(
			"Kind", serviceKind(),
			"ServiceType", corev1.ServiceTypeClusterIP,
		),
	}
}

// Ensure will manage kubernetes v1.Service for redpanda.vectorized.io custom resource
//
//nolint:dupl // TODO multiple services have the same Ensure function
func (r *ClusterServiceResource) Ensure(ctx context.Context) error {
	if len(r.svcPorts) == 0 {
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
	var svc corev1.Service
	err = r.Get(ctx, r.Key(), &svc)
	if err != nil {
		return fmt.Errorf("error while fetching Service resource: %w", err)
	}
	_, err = Update(ctx, &svc, obj, r.Client, r.logger)
	return err
}

// obj returns resource managed client.Object
func (r *ClusterServiceResource) obj() (k8sclient.Object, error) {
	ports := make([]corev1.ServicePort, 0, len(r.svcPorts))
	for _, svcPort := range r.svcPorts {
		ports = append(ports, corev1.ServicePort{
			Name:       svcPort.Name,
			Protocol:   corev1.ProtocolTCP,
			Port:       int32(svcPort.Port),
			TargetPort: intstr.FromInt(svcPort.Port),
		})
	}

	objLabels := labels.ForCluster(r.pandaCluster)
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: r.Key().Namespace,
			Name:      r.Key().Name,
			Labels:    objLabels,
		},
		TypeMeta: metav1.TypeMeta{
			Kind:       "Service",
			APIVersion: "v1",
		},
		Spec: corev1.ServiceSpec{
			PublishNotReadyAddresses: true,
			Type:                     corev1.ServiceTypeClusterIP,
			Ports:                    ports,
			Selector:                 objLabels.AsAPISelector().MatchLabels,
		},
	}

	err := controllerutil.SetControllerReference(r.pandaCluster, svc, r.scheme)
	if err != nil {
		return nil, err
	}

	return svc, nil
}

// Key returns namespace/name object that is used to identify object.
// For reference please visit types.NamespacedName docs in k8s.io/apimachinery
func (r *ClusterServiceResource) Key() types.NamespacedName {
	return types.NamespacedName{Name: r.pandaCluster.Name + "-cluster", Namespace: r.pandaCluster.Namespace}
}

// ServiceFQDN returns fully qualified domain name for cluster service.
// It can be used to communicate between namespaces if the network policy
// allows it.
func (r *ClusterServiceResource) ServiceFQDN(clusterDomain string) string {
	// In every dns name there is trailing dot to query absolute path
	// For trailing dot explanation please visit http://www.dns-sd.org/trailingdotsindomainnames.html
	if r.pandaCluster.Spec.DNSTrailingDotDisabled {
		return fmt.Sprintf("%s%c%s.svc.%s",
			r.Key().Name,
			'.',
			r.Key().Namespace,
			clusterDomain,
		)
	}
	return fmt.Sprintf("%s%c%s.svc.%s.",
		r.Key().Name,
		'.',
		r.Key().Namespace,
		clusterDomain,
	)
}
