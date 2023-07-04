package redpanda

import (
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	"time"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/pointer"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	"github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	vectorizedv1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/vectorized/v1alpha1"
	adminutils "github.com/redpanda-data/redpanda/src/go/k8s/pkg/admin"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/labels"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources"
)

var (
	// ManagingClusterAnnotation is the annotation that tells the cluster reconciler to start/stop managing cluster
	ManagingClusterAnnotation = vectorizedv1alpha1.GroupVersion.Group + "/managed"

	// ManagingRedpandaAnnotation is the annotation that tells the cluster reconciler to start/stop managing redpandas
	ManagingRedpandaAnnotation = v1alpha1.GroupVersion.Group + "/managed"

	// MigrationCompleteAnnotation Set to false means we should retry/continue migration process, should be idempotent
	MigrationCompleteAnnotation = v1alpha1.GroupVersion.Group + "/migration-complete"

	// MigrationFromLabel to indicate where the redpanda resource was migrated from
	MigrationFromLabel = v1alpha1.GroupVersion.Group + "/migrated-from"

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

	if cluster.Spec.Migration == nil || !cluster.Spec.Migration.Enable {
		log.Info("cluster obj not set for migration, ignoring.")
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

	if err := r.createRedpandaResource(ctx, &redpanda); err != nil {
		log.Error(err, "create redpanda resource")
		return ctrl.Result{}, err
	}

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
			Name:        cluster.Name,
			Namespace:   cluster.Namespace,
			Annotations: map[string]string{ManagingRedpandaAnnotation: "false"},
		},
		Spec: v1alpha1.RedpandaSpec{
			ChartRef: v1alpha1.ChartRef{
				ChartVersion: cluster.Spec.Migration.ChartVersion,
			},
		},
	}

	// Migrating ClusterSpec
	r.migrateClusterSpec(cluster, &rp)
	// Migrating RedpandaConfig
	r.migrateRedpandaConfig(cluster, &rp)
	// The final step is to add labels so Helm adopts the existing resources as we create the redpanda resource
	// Should we then delete old statefulset orphan old pods and then recreate pods?

	return rp, ctrl.Result{}, nil
}

func (r *ClusterToRedpandaReconciler) migrateClusterSpec(cluster *vectorizedv1alpha1.Cluster, rp *v1alpha1.Redpanda) v1alpha1.Redpanda {
	// Cannot be nil
	oldSpec := cluster.Spec

	rpSpec := &v1alpha1.RedpandaClusterSpec{}
	if rp.Spec.ClusterSpec != nil {
		rpSpec = rp.Spec.ClusterSpec
	}

	rpImage := &v1alpha1.RedpandaImage{}
	if rpSpec.Image != nil {
		rpImage = rpSpec.Image
	}

	rpStatefulset := &v1alpha1.Statefulset{}
	if rpSpec.Statefulset != nil {
		rpStatefulset = rpSpec.Statefulset
	}

	rpResources := &v1alpha1.Resources{}
	if rpSpec.Resources != nil {
		rpResources = rpSpec.Resources
	}

	rpLicenseRef := &v1alpha1.LicenseSecretRef{}
	if rpSpec.LicenseSecretRef != nil {
		rpLicenseRef = rpSpec.LicenseSecretRef
	}

	rpAuth := &v1alpha1.Auth{}
	if rpSpec.Auth != nil {
		rpAuth = rpSpec.Auth
	}

	// --- Image information --
	rpImage.Repository = RedpandaImageRepository
	if oldSpec.Image != "" {
		rpImage.Repository = oldSpec.Image
	}

	if oldSpec.Version != "" {
		rpImage.Tag = oldSpec.Version
	}

	// -- Annotations ---
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

	// --- Replicas ---
	rpStatefulset.Replicas = int(pointer.Int32Deref(oldSpec.Replicas, 3))

	// --- Resources ---
	redpandaResources := oldSpec.Resources.Redpanda
	if redpandaResources != nil && redpandaResources.Cpu() != nil {
		redpandaResources.Memory()
		// on values
		// memory
		// reserveMemory

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
			Cores: redpandaResources.Cpu(),
		}

		rpResources.Memory = &v1alpha1.Memory{}

	}

	// migrate auth
	if pointer.BoolDeref(oldSpec.KafkaEnableAuthorization, false) || oldSpec.EnableSASL {
		rpAuth.SASL = &v1alpha1.SASL{
			Enabled: true,
		}

		// TODO: check to see what secret is being created with user information
		if len(oldSpec.Superusers) > 0 {
			users := make([]v1alpha1.UsersItems, 0)
			for i := range oldSpec.Superusers {
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
	if oldSpec.LicenseRef != nil {
		if oldSpec.LicenseRef.Key != "" {
			rpLicenseRef.SecretKey = oldSpec.LicenseRef.Key
		}
		rpLicenseRef.SecretName = oldSpec.LicenseRef.Name
	}

	rpSpec.Image = rpImage
	rpSpec.Statefulset = rpStatefulset
	rpSpec.Tolerations = rpTolerations
	rpSpec.Auth = rpAuth
	rpSpec.LicenseSecretRef = rpLicenseRef

	// cluster spec now updated, we should probably only recreate if needed
	rp.Spec.ClusterSpec = rpSpec

	return *rp
}

func (r *ClusterToRedpandaReconciler) migrateRedpandaConfig(cluster *vectorizedv1alpha1.Cluster, rp *v1alpha1.Redpanda) v1alpha1.Redpanda {
	oldConfig := cluster.Spec.Configuration

	rpSpec := &v1alpha1.RedpandaClusterSpec{}
	if rp.Spec.ClusterSpec != nil {
		rpSpec = rp.Spec.ClusterSpec
	}

	rpStatefulset := &v1alpha1.Statefulset{}
	if rpSpec.Statefulset != nil {
		rpStatefulset = rpSpec.Statefulset
	}

	rpLog := &v1alpha1.Logging{}
	if rpSpec.Logging != nil {
		rpLog = rpSpec.Logging
	}

	rpListeners := &v1alpha1.Listeners{}
	if rpSpec.Listeners != nil {
		rpListeners = rpSpec.Listeners
	}

	// --- Additional Command line ---
	if len(oldConfig.AdditionalCommandlineArguments) > 0 {
		if rpStatefulset.AdditionalRedpandaCmdFlags == nil {
			rpStatefulset.AdditionalRedpandaCmdFlags = make([]string, 0)
		}
		for k, v := range oldConfig.AdditionalCommandlineArguments {
			if v != "" {
				rpStatefulset.AdditionalRedpandaCmdFlags = append(rpStatefulset.AdditionalRedpandaCmdFlags, fmt.Sprintf("--%s=%s", k, v))
			} else {
				rpStatefulset.AdditionalRedpandaCmdFlags = append(rpStatefulset.AdditionalRedpandaCmdFlags, fmt.Sprintf("--%s", k))
			}
		}
	}

	// --- Developer mode set ---
	if oldConfig.DeveloperMode {
		rpLog.LogLevel = "trace"
	}

	// --- Listeners ---
	rpSpec.Listeners = rpListeners

	// Migrate Certificates required here:
	rpTLS := &v1alpha1.TLS{}
	if rpSpec.TLS != nil {
		rpTLS = rpSpec.TLS
	}

	kafkaListener := &v1alpha1.Kafka{}
	if rpSpec.Listeners.Kafka != nil {
		kafkaListener = rpSpec.Listeners.Kafka
	}

	if oldConfig.KafkaAPI != nil && len(oldConfig.KafkaAPI) > 0 {
		for i, _ := range oldConfig.KafkaAPI {
			toMigrate := oldConfig.KafkaAPI[i]
			migrateKafkaAPI(toMigrate, kafkaListener, rpTLS)
		}
	}

	log.Info(fmt.Sprintf("kafkaListeners %v", kafkaListener))

	rpListeners.Kafka = kafkaListener

	// -- Putting everything together ---
	rpSpec.Statefulset = rpStatefulset
	rpSpec.Logging = rpLog
	rpSpec.TLS = rpTLS
	rpSpec.Listeners = rpListeners

	rp.Spec.ClusterSpec = rpSpec

	return *rp
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

func (r *ClusterToRedpandaReconciler) createRedpandaResource(ctx context.Context, rp *v1alpha1.Redpanda) error {
	// Check if HelmRepository exists or create it
	rpGet := &v1alpha1.Redpanda{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: rp.Namespace, Name: rp.Name}, rpGet); err != nil {
		if apierrors.IsNotFound(err) {
			if errCreate := r.Client.Create(ctx, rp); errCreate != nil {
				return fmt.Errorf("error creating HelmRepository: %w", errCreate)
			}
		} else {
			return fmt.Errorf("error getting repanda: %w", err)
		}
	}

	// need to get previous resource version and update the old with the new.
	rp.ResourceVersion = rpGet.ResourceVersion

	// already exists, should try to update not create
	if err := r.Update(ctx, rp); err != nil {
		return err
	}

	return nil
}
