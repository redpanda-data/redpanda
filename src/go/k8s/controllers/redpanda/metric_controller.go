package redpanda

import (
	"context"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	redpandav1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	v1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
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
)

func init() {
	// Register custom metrics with the global prometheus registry
	metrics.Registry.MustRegister(redpandaClusters, desireRedpandaNodes, actualRedpandaNodes)
}

// ClusterMetricController provides metrics for nodes and cluster
type ClusterMetricController struct {
	client.Client
	currentLabels map[string]struct{}
}

// NewClusterMetricsController creates ClusterMetricController
func NewClusterMetricsController(c client.Client) *ClusterMetricController {
	return &ClusterMetricController{
		Client:        c,
		currentLabels: make(map[string]struct{}),
	}
}

// Reconcile gets all Redpanda cluster and registers metrics for them
func (r *ClusterMetricController) Reconcile(
	ctx context.Context, _ ctrl.Request,
) (ctrl.Result, error) {
	cl := redpandav1alpha1.ClusterList{}
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
		g.Set(float64(cl.Items[i].Status.Replicas))
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

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *ClusterMetricController) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&redpandav1alpha1.Cluster{}).
		Owns(&v1.Pod{}).
		Complete(r)
}
