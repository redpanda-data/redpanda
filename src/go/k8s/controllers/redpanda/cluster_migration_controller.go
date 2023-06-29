package redpanda

import (
	"context"
	"fmt"
	vectorizedv1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/vectorized/v1alpha1"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/labels"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
	"time"

	"github.com/go-logr/logr"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	adminutils "github.com/redpanda-data/redpanda/src/go/k8s/pkg/admin"
)

// ClusterReconciler reconciles a Cluster object by migrating it to a repdanda object
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

func (r *ClusterToRedpandaReconciler) Reconcile(ctx context.Context, req ctrl.Request,
) (ctrl.Result, error) {

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
