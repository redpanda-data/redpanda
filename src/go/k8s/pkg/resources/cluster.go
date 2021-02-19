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
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	k8sclient "sigs.k8s.io/controller-runtime/pkg/client"
)

var _ Resource = &ClusterResource{}

// NodeExternalIPFetcher give external IP based on Kubernetes node name
type NodeExternalIPFetcher interface {
	// GetExternalIP returns external IP for a given node name
	GetExternalIP(nodeName string) string
}

// ClusterResource represents v1alpha1.Cluster custom resource
type ClusterResource struct {
	k8sclient.Client
	scheme			*runtime.Scheme
	pandaCluster		*redpandav1alpha1.Cluster
	serviceFQDN		string
	nodePortName		types.NamespacedName
	stsName			types.NamespacedName
	nodePortSvc		corev1.Service
	externalIPFetcher	NodeExternalIPFetcher
	logger			logr.Logger
}

// NewClusterResource creates ClusterResource
func NewClusterResource(
	client k8sclient.Client,
	pandaCluster *redpandav1alpha1.Cluster,
	scheme *runtime.Scheme,
	serviceFQDN string,
	nodePortName types.NamespacedName,
	stsName types.NamespacedName,
	externalIPFetcher NodeExternalIPFetcher,
	logger logr.Logger,
) *ClusterResource {
	return &ClusterResource{
		client,
		scheme,
		pandaCluster,
		serviceFQDN,
		nodePortName,
		stsName,
		corev1.Service{},
		externalIPFetcher,
		logger.WithValues("Kind", clusterKind()),
	}
}

// Ensure will manage v1alpha1.Cluster custom resource
func (c *ClusterResource) Ensure(ctx context.Context) error {
	if c.pandaCluster.Spec.ExternalConnectivity {
		if err := c.Get(ctx, c.nodePortName, &c.nodePortSvc); err != nil {
			return fmt.Errorf("failed to retrieve node port service %s: %w", c.nodePortName, err)
		}

		if len(c.nodePortSvc.Spec.Ports) != 1 || c.nodePortSvc.Spec.Ports[0].NodePort == 0 {
			return fmt.Errorf("node port service %s: %w", c.nodePortName, errNodePortMissing)
		}
	}

	var observedPods corev1.PodList

	err := c.List(ctx, &observedPods, &k8sclient.ListOptions{
		LabelSelector:	labels.ForCluster(c.pandaCluster).AsClientSelector(),
		Namespace:	c.pandaCluster.Namespace,
	})
	if err != nil {
		return fmt.Errorf("failed to retrieve pods redpanda pods: %w", err)
	}

	observedNodesInternal := make([]string, 0, len(observedPods.Items))
	observedNodesExternal := make([]string, 0, len(observedPods.Items))
	for i := range observedPods.Items {
		item := observedPods.Items[i]
		observedNodesInternal = append(observedNodesInternal,
			fmt.Sprintf("%s.%s", item.Name, c.serviceFQDN))

		if c.pandaCluster.Spec.ExternalConnectivity {
			observedNodesExternal = append(observedNodesExternal,
				fmt.Sprintf("%s:%d",
					c.externalIPFetcher.GetExternalIP(item.Spec.NodeName),
					getNodePort(&c.nodePortSvc),
				))
		}
	}

	if !reflect.DeepEqual(observedNodesInternal, c.pandaCluster.Status.Nodes.Internal) ||
		!reflect.DeepEqual(observedNodesExternal, c.pandaCluster.Status.Nodes.External) {
		c.pandaCluster.Status.Nodes.Internal = observedNodesInternal
		c.pandaCluster.Status.Nodes.External = observedNodesExternal

		if err = c.Status().Update(ctx, c.pandaCluster); err != nil {
			return fmt.Errorf("failed to update cluster status nodes: %w", err)
		}
	}

	sts := appsv1.StatefulSet{}
	if err = c.Get(ctx, c.stsName, &c.nodePortSvc); err != nil {
		return fmt.Errorf("failed to retrieve node port service %s: %w", c.nodePortName, err)
	}

	if !reflect.DeepEqual(sts.Status.ReadyReplicas, c.pandaCluster.Status.Replicas) {
		c.pandaCluster.Status.Replicas = sts.Status.ReadyReplicas
		if err := c.Status().Update(ctx, c.pandaCluster); err != nil {
			return fmt.Errorf("unable to update cluster status replicas: %w", err)
		}
	}

	return nil
}

// Obj can not be called
func (c *ClusterResource) Obj() (k8sclient.Object, error) {
	panic("should be never called")
}

// Key returns namespace/name object that is used to identify object.
// For reference please visit types.NamespacedName docs in k8s.io/apimachinery
func (c *ClusterResource) Key() types.NamespacedName {
	return types.NamespacedName{Name: c.pandaCluster.Name, Namespace: c.pandaCluster.Namespace}
}

// Kind returns v1alpha1.Cluster kind
func (c *ClusterResource) Kind() string {
	return clusterKind()
}

func clusterKind() string {
	var c redpandav1alpha1.Cluster
	return c.Kind
}
