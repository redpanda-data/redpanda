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

	"github.com/go-logr/logr"
	redpandav1alpha1 "github.com/vectorizedio/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	"github.com/vectorizedio/redpanda/src/go/k8s/pkg/labels"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	k8sclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

var _ Resource = &NodePortServiceResource{}

// NodePortServiceResource is part of the reconciliation of redpanda.vectorized.io CRD
// that assigns port on each node to enable external connectivity
type NodePortServiceResource struct {
	k8sclient.Client
	scheme       *runtime.Scheme
	pandaCluster *redpandav1alpha1.Cluster
	logger       logr.Logger
}

// NewNodePortService creates NodePortServiceResource
func NewNodePortService(
	client k8sclient.Client,
	pandaCluster *redpandav1alpha1.Cluster,
	scheme *runtime.Scheme,
	logger logr.Logger,
) *NodePortServiceResource {
	return &NodePortServiceResource{
		client,
		scheme,
		pandaCluster,
		logger.WithValues("Kind", serviceKind(), "ServiceType", "NodePort"),
	}
}

// Ensure will manage kubernetes v1.Service for redpanda.vectorized.io custom resource
func (r *NodePortServiceResource) Ensure(ctx context.Context) error {
	return getOrCreate(ctx, r, &corev1.Service{}, "Service NodePort", r.logger)
}

// Obj returns resource managed client.Object
func (r *NodePortServiceResource) Obj() (k8sclient.Object, error) {
	objLabels := labels.ForCluster(r.pandaCluster)
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: r.Key().Namespace,
			Name:      r.Key().Name,
			Labels:    objLabels,
		},
		Spec: corev1.ServiceSpec{
			// The service type node port assigned port to each node in the cluster.
			// This gives a way for operator to assign unused port to the redpanda cluster.
			// Reference:
			// https://kubernetes.io/docs/tutorials/services/source-ip/#source-ip-for-services-with-type-nodeport
			Type: corev1.ServiceTypeNodePort,
			// If you set service.spec.externalTrafficPolicy to the value Local,
			// kube-proxy only proxies proxy requests to local endpoints,
			// and does not forward traffic to other nodes.
			// Reference:
			// https://kubernetes.io/docs/tasks/access-application-cluster/create-external-load-balancer/#preserving-the-client-source-ip
			// https://blog.getambassador.io/externaltrafficpolicy-local-on-kubernetes-e66e498212f9
			ExternalTrafficPolicy: corev1.ServiceExternalTrafficPolicyTypeLocal,
			Ports: []corev1.ServicePort{
				{
					Name:       "kafka-tcp",
					Protocol:   corev1.ProtocolTCP,
					Port:       int32(r.pandaCluster.Spec.Configuration.KafkaAPI.Port),
					TargetPort: intstr.FromInt(r.pandaCluster.Spec.Configuration.KafkaAPI.Port),
				},
			},
			// The selector is purposely set to nil. Our external connectivity doesn't use
			// kubernetes service as kafka protocol need to have access to each broker individually.
			Selector: nil,
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
func (r *NodePortServiceResource) Key() types.NamespacedName {
	return types.NamespacedName{Name: r.pandaCluster.Name + "-external", Namespace: r.pandaCluster.Namespace}
}

// Kind returns v1.Service kind
func (r *NodePortServiceResource) Kind() string {
	return serviceKind()
}
