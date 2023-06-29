package redpanda

import (
	"context"
	"fmt"
	"github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/pointer"
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
	// ManagingClusterAnnotation is the annotation that tells the cluster reconciler to start/stop managing obj
	ManagingClusterAnnotation = "redpanda.vectorized.io/managed"
	// MigrationCompleteAnnotation Set to false means we should retry/continue migration process, should be idempotent
	MigrationCompleteAnnotation = "redpanda.io/migration-complete"
	// MigrationFromLabel to indicate where the redpanda resource was migrated from
	MigrationFromLabel = "redpanda.io/migrated-from"

	// RedpandaImageRepository is the deefault image repo for redpanda
	RedpandaImageRepository = "docker.redpanda.com/redpandadata/redpanda"
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

	clusterAnnotations := cluster.GetAnnotations()
	if managed, ok := clusterAnnotations[ManagingClusterAnnotation]; !ok || managed == "true" {
		clusterAnnotations[ManagingClusterAnnotation] = "false"
		cluster.SetAnnotations(clusterAnnotations)
		if err := r.Update(ctx, cluster); err != nil {
			log.Error(err, "unable to update annotations")
			return ctrl.Result{}, err
		}
	}

	// should return a redpanda CR object with the expected changes,
	redpanda, result, err := r.migrate(ctx, cluster)

	log.Info(fmt.Sprintf("%v", redpanda))

	// Log reconciliation duration
	durationMsg := fmt.Sprintf("reconciliation finished in %s", time.Since(start).String())
	if result.RequeueAfter > 0 {
		durationMsg = fmt.Sprintf("%s, next run in %s", durationMsg, result.RequeueAfter.String())
	}
	log.Info(durationMsg)

	return result, err
}

func (r *ClusterToRedpandaReconciler) migrate(ctx context.Context, cluster *vectorizedv1alpha1.Cluster) (v1alpha1.Redpanda, ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx)
	log.WithName("ClusterMigrationReconciler.migrate")

	log.Info(fmt.Sprintf("migrating cluster obj with name %q in namespace %q", cluster.Name, cluster.Namespace))

	rp := v1alpha1.Redpanda{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
	}

	// Spec details including chart version etc..
	rpSpec := v1alpha1.RedpandaSpec{}

	// Values file entries are here, we should be filling the cluster details mostly here

	rpClusterSpec := r.migrateRedpandaClusterSpec(cluster)

	// Now place all the cluster spec details in
	rp.Spec = rpSpec
	rp.Spec.ClusterSpec = &rpClusterSpec

	// The final step is to add labels so Helm adopts the existing resources as we create the redpanda resource
	// Should we then delete old statefulset orphan old pods and then recreate pods?

	return rp, ctrl.Result{}, nil
}

func (r *ClusterToRedpandaReconciler) migrateRedpandaClusterSpec(cluster *vectorizedv1alpha1.Cluster) v1alpha1.RedpandaClusterSpec {

	// Cannot be nil
	oldSpec := cluster.Spec

	rpImage := v1alpha1.RedpandaImage{}
	rpStatefulset := v1alpha1.Statefulset{}
	rpResources := v1alpha1.Resources{}

	// Image information
	rpImage.Repository = RedpandaImageRepository
	if oldSpec.Image != "" {
		rpImage.Repository = oldSpec.Image
	}

	if oldSpec.Version != "" {
		rpImage.Tag = oldSpec.Version
	}

	// Annotations
	if oldSpec.Annotations != nil {
		rpStatefulset.Annotations = oldSpec.Annotations
	}

	oldPBD := oldSpec.PodDisruptionBudget
	if oldPBD != nil && oldPBD.Enabled && oldPBD.MaxUnavailable != nil {
		rpStatefulset.Budget = &v1alpha1.Budget{
			MaxUnavailable: oldPBD.MaxUnavailable.IntValue(),
		}
	}

	rpTolerations := make([]corev1.Toleration, 0)
	if oldSpec.Tolerations != nil && len(oldSpec.Tolerations) > 0 {
		rpTolerations = append(rpTolerations, oldSpec.Tolerations...)
	}

	// Replicas
	rpStatefulset.Replicas = int(pointer.Int32Deref(oldSpec.Replicas, 3))

	// Resources
	redpandaResources := oldSpec.Resources.Redpanda
	if redpandaResources != nil && redpandaResources.Cpu() != nil {

		// Limits, Requests and Redpanda, these need to be parsed correctly
		// On the chart we list these out a bit differently:
		// cpu:
		//   cores:
		// memory:
		//   container:
		//     max:
		//     min:
		//   redpanda:
		//
		//

		rpResources.CPU = &v1alpha1.CPU{
			Cores: redpandaResources.Cpu().String(),
		}

		rpResources.Memory = &v1alpha1.Memory{}

	}

	rpAuth := v1alpha1.Auth{}
	if pointer.BoolDeref(oldSpec.KafkaEnableAuthorization, false) || oldSpec.EnableSASL {
		rpAuth.SASL = &v1alpha1.SASL{
			Enabled: true,
		}

		// TODO: check to see what secret is being created with user information
		if len(oldSpec.Superusers) > 0 {
			users := make([]v1alpha1.UsersItems, 0)
			for i, _ := range oldSpec.Superusers {
				user := oldSpec.Superusers[i]
				userItem := v1alpha1.UsersItems{
					Name:      user.Username,
					Password:  "",
					Mechanism: "SCRAM-SHA-512",
				}

				users = append(users, userItem)

			}
		}
	}

	// license details
	rpLicenseRef := v1alpha1.LicenseSecretRef{}
	if oldSpec.LicenseRef != nil {
		if oldSpec.LicenseRef.Key != "" {
			rpLicenseRef.SecretKey = oldSpec.LicenseRef.Key
		}
		rpLicenseRef.SecretName = oldSpec.LicenseRef.Name
	}

	return v1alpha1.RedpandaClusterSpec{
		Image:            &rpImage,
		Statefulset:      &rpStatefulset,
		Resources:        &rpResources,
		Tolerations:      rpTolerations,
		Auth:             &rpAuth,
		LicenseSecretRef: &rpLicenseRef,
	}
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
