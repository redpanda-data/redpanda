// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package redpanda

import (
	"context"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/metrics"

	vectorizedv1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/vectorized/v1alpha1"
)

var (
	redpandaClusters = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "redpanda_clusters_total",
			Help: "Number of Redpanda clusters running",
		},
	)
	desireRedpandaNodes = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "desired_redpanda_nodes_total",
			Help: "Number of desire Redpanda nodes",
		}, []string{"cluster"},
	)
	actualRedpandaNodes = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "actual_redpanda_nodes_total",
			Help: "Number of actual Redpanda nodes",
		}, []string{"cluster"},
	)
	misconfiguredClusters = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "redpanda_misconfigured_clusters_total",
			Help: "Number of Redpanda clusters having configuration problems",
		}, []string{"reason"},
	)
)

func init() {
	// Register custom metrics with the global prometheus registry
	metrics.Registry.MustRegister(redpandaClusters, desireRedpandaNodes, actualRedpandaNodes, misconfiguredClusters)
}

// ClusterMetricController provides metrics for nodes and cluster
type ClusterMetricController struct {
	client.Client
	currentLabels              map[string]struct{}
	currentConfigurationLabels map[string]struct{}
}

// NewClusterMetricsController creates ClusterMetricController
func NewClusterMetricsController(c client.Client) *ClusterMetricController {
	return &ClusterMetricController{
		Client:                     c,
		currentLabels:              make(map[string]struct{}),
		currentConfigurationLabels: make(map[string]struct{}),
	}
}

// Reconcile gets all Redpanda cluster and registers metrics for them
func (r *ClusterMetricController) Reconcile(
	c context.Context, _ ctrl.Request,
) (ctrl.Result, error) {
	ctx, done := context.WithCancel(c)
	defer done()

	cl := vectorizedv1alpha1.ClusterList{}
	err := r.List(ctx, &cl, &client.ListOptions{})
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("unable to list the clusters: %w", err)
	}

	redpandaClusters.Set(float64(len(cl.Items)))

	curLabels := map[string]struct{}{}

	for i := range cl.Items {
		g, err := desireRedpandaNodes.GetMetricWithLabelValues(cl.Items[i].Name)
		if err != nil {
			return ctrl.Result{}, err
		}
		g.Set(float64(*cl.Items[i].Spec.Replicas))

		g, err = actualRedpandaNodes.GetMetricWithLabelValues(cl.Items[i].Name)
		if err != nil {
			return ctrl.Result{}, err
		}
		g.Set(float64(cl.Items[i].Status.ReadyReplicas))
		curLabels[cl.Items[i].Name] = struct{}{}
		r.currentLabels[cl.Items[i].Name] = struct{}{}
	}

	for key := range r.currentLabels {
		_, exist := curLabels[key]
		if !exist {
			desireRedpandaNodes.DeleteLabelValues(key)
			actualRedpandaNodes.DeleteLabelValues(key)
		}
	}

	misconfiguredClustersCount := make(map[string]int)
	for i := range cl.Items {
		if cond := cl.Items[i].Status.GetCondition(vectorizedv1alpha1.ClusterConfiguredConditionType); cond != nil && cond.Status != corev1.ConditionTrue {
			cur := misconfiguredClustersCount[cond.Reason]
			cur++
			misconfiguredClustersCount[cond.Reason] = cur
		}
	}
	for k, c := range misconfiguredClustersCount {
		g, err := misconfiguredClusters.GetMetricWithLabelValues(k)
		if err != nil {
			return ctrl.Result{}, err
		}
		g.Set(float64(c))
		r.currentConfigurationLabels[k] = struct{}{}
	}
	for k := range r.currentConfigurationLabels {
		if _, exists := misconfiguredClustersCount[k]; !exists {
			misconfiguredClusters.DeleteLabelValues(k)
		}
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *ClusterMetricController) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&vectorizedv1alpha1.Cluster{}).
		Owns(&corev1.Pod{}).
		Complete(r)
}
