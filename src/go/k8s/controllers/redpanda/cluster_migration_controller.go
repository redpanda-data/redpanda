package redpanda

import (
	"context"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	vectorizedv1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/vectorized/v1alpha1"
	adminutils "github.com/redpanda-data/redpanda/src/go/k8s/pkg/admin"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/labels"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources"
)

var (
	StopManagingClusterLabel = "redpanda.vectorized.io/managed"
)

// ClusterToRedpandaReconciler reconciles a Cluster object by migrating it to a Redpanda object
type ClusterToRedpandaReconciler struct {
	client.Client
	Log                       logr.Logger
	configuratorSettings      resources.ConfiguratorSettings
	clusterDomain             string
	Scheme                    *runtime.Scheme
	AdminAPIClientFactory     adminutils.AdminAPIClientFactory
	DecommissionWaitInterval  time.Duration
	MetricsTimeout            time.Duration
	RestrictToRedpandaVersion string
	allowPVCDeletion          bool
}

func (r *ClusterToRedpandaReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	start := time.Now()
	log := ctrl.LoggerFrom(ctx).WithName("ClusterMigrationReconciler.Reconcile")
	log.Info("Starting reconcile loop")

	defer log.Info("Finished reconcile loop")

	cluster := &vectorizedv1alpha1.Cluster{}
	if err := r.Client.Get(ctx, req.NamespacedName, cluster); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Examine if the object is under deletion
	// if so, do not continue
	if !cluster.ObjectMeta.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	// add do not manage label does not exist
	if cluster.Annotations == nil {
		cluster.Annotations = make(map[string]string, 0)
	}

	clusterAnnotations := cluster.GetAnnotations()
	if managed, ok := clusterAnnotations[StopManagingClusterLabel]; !ok || managed == "true" {
		clusterAnnotations[StopManagingClusterLabel] = "false"
		cluster.SetAnnotations(clusterAnnotations)
		if err := r.Update(ctx, cluster); err != nil {
			log.Error(err, "unable to update annotations")
			return ctrl.Result{}, err
		}
	}

	result, err := r.migrate(ctx, cluster)

	// Log reconciliation duration
	durationMsg := fmt.Sprintf("reconciliation finished in %s", time.Since(start).String())
	if result.RequeueAfter > 0 {
		durationMsg = fmt.Sprintf("%s, next run in %s", durationMsg, result.RequeueAfter.String())
	}
	log.Info(durationMsg)

	return result, err
}

func (r *ClusterToRedpandaReconciler) migrate(ctx context.Context, cluster *vectorizedv1alpha1.Cluster) (ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx)
	log.WithName("ClusterMigrationReconciler.migrate")

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *ClusterToRedpandaReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := validateImagePullPolicy(r.configuratorSettings.ImagePullPolicy); err != nil {
		return fmt.Errorf("invalid image pull policy \"%s\": %w", r.configuratorSettings.ImagePullPolicy, err)
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&vectorizedv1alpha1.Cluster{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Service{}).
		Watches(
			&source.Kind{Type: &corev1.Pod{}},
			handler.EnqueueRequestsFromMapFunc(r.reconcileClusterForPods),
			builder.WithPredicates(predicate.ResourceVersionChangedPredicate{}),
		).
		Watches(
			&source.Kind{Type: &corev1.Secret{}},
			handler.EnqueueRequestsFromMapFunc(r.reconcileClusterForExternalCASecret),
			builder.WithPredicates(predicate.ResourceVersionChangedPredicate{}),
		).
		Complete(r)
}

func (r *ClusterToRedpandaReconciler) reconcileClusterForPods(pod client.Object) []reconcile.Request {
	return []reconcile.Request{
		{
			NamespacedName: types.NamespacedName{
				Namespace: pod.GetNamespace(),
				Name:      pod.GetLabels()[labels.InstanceKey],
			},
		},
	}
}

func (r *ClusterToRedpandaReconciler) reconcileClusterForExternalCASecret(s client.Object) []reconcile.Request {
	hasExternalCA, found := s.GetAnnotations()[SecretAnnotationExternalCAKey]
	if !found || hasExternalCA != "true" {
		return nil
	}

	clusterName, found := s.GetLabels()[labels.InstanceKey]
	if !found {
		return nil
	}

	return []reconcile.Request{{
		NamespacedName: types.NamespacedName{
			Namespace: s.GetNamespace(),
			Name:      clusterName,
		},
	}}
}

// WithClusterDomain set the clusterDomain
func (r *ClusterToRedpandaReconciler) WithClusterDomain(clusterDomain string) *ClusterToRedpandaReconciler {
	r.clusterDomain = clusterDomain
	return r
}

func (r *ClusterToRedpandaReconciler) WithAllowPVCDeletion(allowPVCDeletion bool) *ClusterToRedpandaReconciler {
	r.allowPVCDeletion = allowPVCDeletion
	return r
}

// WithConfiguratorSettings set the configurator image settings
func (r *ClusterToRedpandaReconciler) WithConfiguratorSettings(configuratorSettings resources.ConfiguratorSettings) *ClusterToRedpandaReconciler {
	r.configuratorSettings = configuratorSettings
	return r
}
