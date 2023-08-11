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
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"reflect"
	"time"

	helmv2beta1 "github.com/fluxcd/helm-controller/api/v2beta1"
	"github.com/fluxcd/pkg/apis/meta"
	"github.com/fluxcd/pkg/runtime/predicates"
	sourcev1 "github.com/fluxcd/source-controller/api/v1beta2"
	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	kuberecorder "k8s.io/client-go/tools/record"
	"k8s.io/utils/pointer"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/source"
	v2 "sigs.k8s.io/controller-runtime/pkg/webhook/conversion/testdata/api/v2"

	"github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
)

const (
	resourceReadyStrFmt    = "%s '%s/%s' is ready"
	resourceNotReadyStrFmt = "%s '%s/%s' is not ready"

	resourceTypeHelmRepository = "HelmRepository"
	resourceTypeHelmRelease    = "HelmRelease"
)

// RedpandaReconciler reconciles a Redpanda object
type RedpandaReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	kuberecorder.EventRecorder

	RequeueHelmDeps time.Duration
}

// flux resources main resources
// +kubebuilder:rbac:groups=helm.toolkit.fluxcd.io,namespace=default,resources=helmreleases,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=helm.toolkit.fluxcd.io,namespace=default,resources=helmreleases/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=helm.toolkit.fluxcd.io,namespace=default,resources=helmreleases/finalizers,verbs=update
// +kubebuilder:rbac:groups=source.toolkit.fluxcd.io,namespace=default,resources=helmcharts,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=source.toolkit.fluxcd.io,namespace=default,resources=helmcharts/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=source.toolkit.fluxcd.io,namespace=default,resources=helmcharts/finalizers,verbs=get;create;update;patch;delete
// +kubebuilder:rbac:groups=source.toolkit.fluxcd.io,namespace=default,resources=helmrepositories,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=source.toolkit.fluxcd.io,namespace=default,resources=helmrepositories/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=source.toolkit.fluxcd.io,namespace=default,resources=helmrepositories/finalizers,verbs=get;create;update;patch;delete

// flux additional resources
// +kubebuilder:rbac:groups=source.toolkit.fluxcd.io,namespace=default,resources=buckets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=source.toolkit.fluxcd.io,namespace=default,resources=gitrepositories,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,namespace=default,resources=replicasets,verbs=get;list;watch;create;update;patch;delete

// additional k8s resources required by flux
// +kubebuilder:rbac:groups="",namespace=default,resources=pods,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,namespace=default,resources=rolebindings,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,namespace=default,resources=roles,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=batch,namespace=default,resources=jobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,namespace=default,resources=secrets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,namespace=default,resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,namespace=default,resources=serviceaccounts,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,namespace=default,resources=statefulsets,verbs=get;create;update;patch;delete
// +kubebuilder:rbac:groups=policy,namespace=default,resources=poddisruptionbudgets,verbs=get;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,namespace=default,resources=deployments,verbs=get;create;update;patch;delete
// +kubebuilder:rbac:groups=cert-manager.io,namespace=default,resources=certificates,verbs=get;create;update;patch;delete
// +kubebuilder:rbac:groups=cert-manager.io,namespace=default,resources=issuers,verbs=get;create;update;patch;delete

// redpanda resources
// +kubebuilder:rbac:groups=cluster.redpanda.com,namespace=default,resources=redpandas,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cluster.redpanda.com,namespace=default,resources=redpandas/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=cluster.redpanda.com,namespace=default,resources=redpandas/finalizers,verbs=update
// +kubebuilder:rbac:groups=core,namespace=default,resources=events,verbs=create;patch

// SetupWithManager sets up the controller with the Manager.
func (r *RedpandaReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.Redpanda{}, builder.WithPredicates(
			predicate.Or(predicate.GenerationChangedPredicate{},
				predicates.ReconcileRequestedPredicate{}),
		)).
		Watches(
			&source.Kind{Type: &helmv2beta1.HelmRelease{}},
			&handler.EnqueueRequestForObject{},
		).
		Complete(r)
}

func (r *RedpandaReconciler) Reconcile(c context.Context, req ctrl.Request) (ctrl.Result, error) {
	ctx, done := context.WithCancel(c)
	defer done()

	start := time.Now()
	log := ctrl.LoggerFrom(ctx).WithName("RedpandaReconciler.Reconcile")

	log.Info("Starting reconcile loop")

	rp := &v1alpha1.Redpanda{}
	if err := r.Client.Get(ctx, req.NamespacedName, rp); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// add finalizer if not exist
	if !controllerutil.ContainsFinalizer(rp, FinalizerKey) {
		patch := client.MergeFrom(rp.DeepCopy())
		controllerutil.AddFinalizer(rp, FinalizerKey)
		if err := r.Patch(ctx, rp, patch); err != nil {
			log.Error(err, "unable to register finalizer")
			return ctrl.Result{}, err
		}
	}

	// Examine if the object is under deletion
	if !rp.ObjectMeta.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, rp)
	}

	if !isRedpandaManaged(ctx, rp) {
		return ctrl.Result{}, nil
	}

	rp, result, err := r.reconcile(ctx, rp)

	// Update status after reconciliation.
	if updateStatusErr := r.patchRedpandaStatus(ctx, rp); updateStatusErr != nil {
		log.Error(updateStatusErr, "unable to update status after reconciliation")
		return ctrl.Result{Requeue: true}, updateStatusErr
	}

	// Log reconciliation duration
	durationMsg := fmt.Sprintf("reconciliation finished in %s", time.Since(start).String())
	if result.RequeueAfter > 0 {
		durationMsg = fmt.Sprintf("%s, next run in %s", durationMsg, result.RequeueAfter.String())
	}
	log.Info(durationMsg)

	return result, err
}

func (r *RedpandaReconciler) reconcile(ctx context.Context, rp *v1alpha1.Redpanda) (*v1alpha1.Redpanda, ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx)
	log.WithName("RedpandaReconciler.reconcile")

	// Observe HelmRelease generation.
	if rp.Status.ObservedGeneration != rp.Generation {
		rp.Status.ObservedGeneration = rp.Generation
		rp = v1alpha1.RedpandaProgressing(rp)
		if updateStatusErr := r.patchRedpandaStatus(ctx, rp); updateStatusErr != nil {
			log.Error(updateStatusErr, "unable to update status after generation update")
			return rp, ctrl.Result{Requeue: true}, updateStatusErr
		}
	}

	// Check if HelmRepository exists or create it
	rp, repo, err := r.reconcileHelmRepository(ctx, rp)
	if err != nil {
		return rp, ctrl.Result{}, err
	}

	isGenerationCurrent := repo.Generation != repo.Status.ObservedGeneration
	isStatusConditionReady := apimeta.IsStatusConditionTrue(repo.Status.Conditions, meta.ReadyCondition)
	msgNotReady := fmt.Sprintf(resourceNotReadyStrFmt, resourceTypeHelmRepository, repo.GetNamespace(), repo.GetName())
	msgReady := fmt.Sprintf(resourceReadyStrFmt, resourceTypeHelmRepository, repo.GetNamespace(), repo.GetName())
	isStatusReadyNILorTRUE := IsBoolPointerNILorEqual(rp.Status.HelmRepositoryReady, true)
	isStatusReadyNILorFALSE := IsBoolPointerNILorEqual(rp.Status.HelmRepositoryReady, false)

	isResourceReady := r.checkIfResourceIsReady(log, msgNotReady, msgReady, resourceTypeHelmRepository, isGenerationCurrent, isStatusConditionReady, isStatusReadyNILorTRUE, isStatusReadyNILorFALSE, rp)
	if !isResourceReady {
		// need to requeue in this case
		return v1alpha1.RedpandaNotReady(rp, "ArtifactFailed", msgNotReady), ctrl.Result{RequeueAfter: r.RequeueHelmDeps}, nil
	}

	// Check if HelmRelease exists or create it also
	rp, hr, err := r.reconcileHelmRelease(ctx, rp)
	if err != nil {
		return rp, ctrl.Result{}, err
	}
	if hr.Name == "" {
		log.Info(fmt.Sprintf("Created HelmRelease for '%s/%s', will requeue", rp.Namespace, rp.Name))
		return rp, ctrl.Result{}, err
	}

	isGenerationCurrent = hr.Generation != hr.Status.ObservedGeneration
	isStatusConditionReady = apimeta.IsStatusConditionTrue(hr.Status.Conditions, meta.ReadyCondition)
	msgNotReady = fmt.Sprintf(resourceNotReadyStrFmt, resourceTypeHelmRelease, hr.GetNamespace(), hr.GetName())
	msgReady = fmt.Sprintf(resourceReadyStrFmt, resourceTypeHelmRelease, hr.GetNamespace(), hr.GetName())
	isStatusReadyNILorTRUE = IsBoolPointerNILorEqual(rp.Status.HelmReleaseReady, true)
	isStatusReadyNILorFALSE = IsBoolPointerNILorEqual(rp.Status.HelmReleaseReady, false)

	isResourceReady = r.checkIfResourceIsReady(log, msgNotReady, msgReady, resourceTypeHelmRelease, isGenerationCurrent, isStatusConditionReady, isStatusReadyNILorTRUE, isStatusReadyNILorFALSE, rp)
	if !isResourceReady {
		// need to requeue in this case
		return v1alpha1.RedpandaNotReady(rp, "ArtifactFailed", msgNotReady), ctrl.Result{RequeueAfter: r.RequeueHelmDeps}, nil
	}

	return v1alpha1.RedpandaReady(rp), ctrl.Result{}, nil
}

func (r *RedpandaReconciler) checkIfResourceIsReady(log logr.Logger, msgNotReady, msgReady, kind string, isGenerationCurrent, isStatusConditionReady, isStatusReadyNILorTRUE, isStatusReadyNILorFALSE bool, rp *v1alpha1.Redpanda) bool {
	if isGenerationCurrent || !isStatusConditionReady {
		// capture event only
		if isStatusReadyNILorTRUE {
			r.event(rp, rp.Status.LastAttemptedRevision, v1alpha1.EventSeverityInfo, msgNotReady)
		}

		switch kind {
		case resourceTypeHelmRepository:
			rp.Status.HelmRepositoryReady = pointer.Bool(false)
		case resourceTypeHelmRelease:
			rp.Status.HelmReleaseReady = pointer.Bool(false)
		}

		log.Info(msgNotReady)
		return false
	} else if isStatusConditionReady && isStatusReadyNILorFALSE {
		// here since the condition should be true, we update the value to
		// be true, and send an event
		switch kind {
		case resourceTypeHelmRepository:
			rp.Status.HelmRepositoryReady = pointer.Bool(true)
		case resourceTypeHelmRelease:
			rp.Status.HelmReleaseReady = pointer.Bool(true)
		}

		r.event(rp, rp.Status.LastAttemptedRevision, v1alpha1.EventSeverityInfo, msgReady)
	}

	return true
}

func (r *RedpandaReconciler) reconcileHelmRelease(ctx context.Context, rp *v1alpha1.Redpanda) (*v1alpha1.Redpanda, *helmv2beta1.HelmRelease, error) {
	var err error

	// Check if HelmRelease exists or create it
	hr := &helmv2beta1.HelmRelease{}

	// have we recorded a helmRelease, if not assume we have not created it
	if rp.Status.HelmRelease == "" {
		// did not find helmRelease, then create it
		hr, err = r.createHelmRelease(ctx, rp)
		return rp, hr, err
	}

	// if we are not empty, then we assume at some point this existed, let's check
	key := types.NamespacedName{Namespace: rp.Namespace, Name: rp.Status.GetHelmRelease()}
	err = r.Client.Get(ctx, key, hr)
	if err != nil {
		if apierrors.IsNotFound(err) {
			rp.Status.HelmRelease = ""
			hr, err = r.createHelmRelease(ctx, rp)
			return rp, hr, err
		}
		// if this is a not found error
		return rp, hr, fmt.Errorf("failed to get HelmRelease '%s/%s': %w", rp.Namespace, rp.Status.HelmRelease, err)
	}

	// Check if we need to update here
	hrTemplate, errTemplated := r.createHelmReleaseFromTemplate(ctx, rp)
	if errTemplated != nil {
		r.event(rp, rp.Status.LastAttemptedRevision, v1alpha1.EventSeverityError, errTemplated.Error())
		return rp, hr, errTemplated
	}

	if r.helmReleaseRequiresUpdate(ctx, hr, hrTemplate) {
		hr.Spec = hrTemplate.Spec
		if err = r.Client.Update(ctx, hr); err != nil {
			r.event(rp, rp.Status.LastAttemptedRevision, v1alpha1.EventSeverityError, err.Error())
			return rp, hr, err
		}
		r.event(rp, rp.Status.LastAttemptedRevision, v1alpha1.EventSeverityInfo, fmt.Sprintf("HelmRelease '%s/%s' updated", rp.Namespace, rp.GetHelmReleaseName()))
		rp.Status.HelmRelease = rp.GetHelmReleaseName()
	}

	return rp, hr, nil
}

func (r *RedpandaReconciler) reconcileHelmRepository(ctx context.Context, rp *v1alpha1.Redpanda) (*v1alpha1.Redpanda, *sourcev1.HelmRepository, error) {
	// Check if HelmRepository exists or create it
	repo := &sourcev1.HelmRepository{}
	if err := r.Client.Get(ctx, types.NamespacedName{Namespace: rp.Namespace, Name: rp.GetHelmRepositoryName()}, repo); err != nil {
		if apierrors.IsNotFound(err) {
			repo = r.createHelmRepositoryFromTemplate(rp)
			if errCreate := r.Client.Create(ctx, repo); errCreate != nil {
				r.event(rp, rp.Status.LastAttemptedRevision, v1alpha1.EventSeverityError, fmt.Sprintf("error creating HelmRepository: %s", errCreate))
				return rp, repo, fmt.Errorf("error creating HelmRepository: %w", errCreate)
			}
			r.event(rp, rp.Status.LastAttemptedRevision, v1alpha1.EventSeverityInfo, fmt.Sprintf("HelmRepository '%s/%s' created ", rp.Namespace, rp.GetHelmRepositoryName()))
		} else {
			r.event(rp, rp.Status.LastAttemptedRevision, v1alpha1.EventSeverityError, fmt.Sprintf("error getting HelmRepository: %s", err))
			return rp, repo, fmt.Errorf("error getting HelmRepository: %w", err)
		}
	}
	rp.Status.HelmRepository = rp.GetHelmRepositoryName()

	return rp, repo, nil
}

func (r *RedpandaReconciler) reconcileDelete(ctx context.Context, rp *v1alpha1.Redpanda) (ctrl.Result, error) {
	if err := r.deleteHelmRelease(ctx, rp); err != nil {
		return ctrl.Result{}, err
	}

	controllerutil.RemoveFinalizer(rp, FinalizerKey)
	if err := r.Client.Update(ctx, rp); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *RedpandaReconciler) createHelmRelease(ctx context.Context, rp *v1alpha1.Redpanda) (*helmv2beta1.HelmRelease, error) {
	// create helmRelease resource from template
	hRelease, err := r.createHelmReleaseFromTemplate(ctx, rp)
	if err != nil {
		r.event(rp, rp.Status.LastAttemptedRevision, v1alpha1.EventSeverityError, fmt.Sprintf("could not create helm release template: %s", err))
		return hRelease, fmt.Errorf("could not create HelmRelease template: %w", err)
	}

	// create helmRelease object here
	if err := r.Client.Create(ctx, hRelease); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			r.event(rp, rp.Status.LastAttemptedRevision, v1alpha1.EventSeverityError, err.Error())
			return hRelease, fmt.Errorf("failed to create HelmRelease '%s/%s': %w", rp.Namespace, rp.Status.HelmRelease, err)
		}
		// we already exist, then update the status to rp
		rp.Status.HelmRelease = rp.GetHelmReleaseName()
	}

	// we have created the resource, so we are ok to update events, and update the helmRelease name on the status object
	r.event(rp, rp.Status.LastAttemptedRevision, v1alpha1.EventSeverityInfo, fmt.Sprintf("HelmRelease '%s/%s' created ", rp.Namespace, rp.GetHelmReleaseName()))
	rp.Status.HelmRelease = rp.GetHelmReleaseName()

	return hRelease, nil
}

func (r *RedpandaReconciler) deleteHelmRelease(ctx context.Context, rp *v1alpha1.Redpanda) error {
	if rp.Status.HelmRelease == "" {
		return nil
	}

	var hr helmv2beta1.HelmRelease
	hrName := rp.Status.GetHelmRelease()
	err := r.Client.Get(ctx, types.NamespacedName{Namespace: rp.Namespace, Name: hrName}, &hr)
	if err != nil {
		if apierrors.IsNotFound(err) {
			rp.Status.HelmRelease = ""
			return nil
		}
		return fmt.Errorf("failed to get HelmRelease '%s': %w", rp.Status.HelmRelease, err)
	}
	if err = r.Client.Delete(ctx, &hr); err == nil {
		rp.Status.HelmRelease = ""
		rp.Status.HelmRepository = ""
	}

	return err
}

func (r *RedpandaReconciler) createHelmReleaseFromTemplate(ctx context.Context, rp *v1alpha1.Redpanda) (*helmv2beta1.HelmRelease, error) {
	log := ctrl.LoggerFrom(ctx).WithName("RedpandaReconciler.createHelmReleaseFromTemplate")

	values, err := rp.ValuesJSON()
	if err != nil {
		return nil, fmt.Errorf("could not parse clusterSpec to json: %w", err)
	}

	hasher := sha256.New()
	hasher.Write(values.Raw)
	sha := base64.URLEncoding.EncodeToString(hasher.Sum(nil))
	// TODO possibly add the SHA to the status
	log.Info(fmt.Sprintf("SHA of values file to use: %s", sha))

	timeout := rp.Spec.ChartRef.Timeout
	if timeout == nil {
		timeout = &metav1.Duration{Duration: 15 * time.Minute}
	}

	rollBack := helmv2beta1.RemediationStrategy("rollback")

	upgrade := &helmv2beta1.Upgrade{
		Remediation: &helmv2beta1.UpgradeRemediation{
			Retries:  1,
			Strategy: &rollBack,
		},
	}

	helmUpgrade := rp.Spec.ChartRef.Upgrade
	if rp.Spec.ChartRef.Upgrade != nil {
		if helmUpgrade.Force != nil {
			upgrade.Force = pointer.BoolDeref(helmUpgrade.Force, false)
		}
		if helmUpgrade.CleanupOnFail != nil {
			upgrade.CleanupOnFail = pointer.BoolDeref(helmUpgrade.CleanupOnFail, false)
		}
		if helmUpgrade.PreserveValues != nil {
			upgrade.PreserveValues = pointer.BoolDeref(helmUpgrade.PreserveValues, false)
		}
		if helmUpgrade.Remediation != nil {
			upgrade.Remediation = helmUpgrade.Remediation
		}
	}

	return &helmv2beta1.HelmRelease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      rp.GetHelmReleaseName(),
			Namespace: rp.Namespace,
		},
		Spec: helmv2beta1.HelmReleaseSpec{
			Chart: helmv2beta1.HelmChartTemplate{
				Spec: helmv2beta1.HelmChartTemplateSpec{
					Chart:    "redpanda",
					Version:  rp.Spec.ChartRef.ChartVersion,
					Interval: &metav1.Duration{Duration: 1 * time.Minute},
					SourceRef: helmv2beta1.CrossNamespaceObjectReference{
						Kind:      "HelmRepository",
						Name:      rp.GetHelmRepositoryName(),
						Namespace: rp.Namespace,
					},
				},
			},
			Values:   values,
			Interval: metav1.Duration{Duration: 30 * time.Second},
			Timeout:  timeout,
			Upgrade:  upgrade,
		},
	}, nil
}

func (r *RedpandaReconciler) createHelmRepositoryFromTemplate(rp *v1alpha1.Redpanda) *sourcev1.HelmRepository {
	return &sourcev1.HelmRepository{
		ObjectMeta: metav1.ObjectMeta{
			Name:            rp.GetHelmRepositoryName(),
			Namespace:       rp.Namespace,
			OwnerReferences: []metav1.OwnerReference{rp.OwnerShipRefObj()},
		},
		Spec: sourcev1.HelmRepositorySpec{
			Interval: metav1.Duration{Duration: 30 * time.Second},
			URL:      v1alpha1.RedpandaChartRepository,
		},
	}
}

func (r *RedpandaReconciler) patchRedpandaStatus(ctx context.Context, rp *v1alpha1.Redpanda) error {
	key := client.ObjectKeyFromObject(rp)
	latest := &v1alpha1.Redpanda{}
	if err := r.Client.Get(ctx, key, latest); err != nil {
		return err
	}
	return r.Client.Status().Patch(ctx, rp, client.MergeFrom(latest))
}

// event emits a Kubernetes event and forwards the event to notification controller if configured.
func (r *RedpandaReconciler) event(rp *v1alpha1.Redpanda, revision, severity, msg string) {
	var metaData map[string]string
	if revision != "" {
		metaData = map[string]string{v2.GroupVersion.Group + "/revision": revision}
	}
	eventType := "Normal"
	if severity == v1alpha1.EventSeverityError {
		eventType = "Warning"
	}
	r.EventRecorder.AnnotatedEventf(rp, metaData, eventType, severity, msg)
}

func (r *RedpandaReconciler) helmReleaseRequiresUpdate(ctx context.Context, hr, hrTemplate *helmv2beta1.HelmRelease) bool {
	log := ctrl.LoggerFrom(ctx).WithName("RedpandaReconciler.helmReleaseRequiresUpdate")

	switch {
	case !reflect.DeepEqual(hr.GetValues(), hrTemplate.GetValues()):
		log.Info("values found different")
		return true
	case helmChartRequiresUpdate(&hr.Spec.Chart, &hrTemplate.Spec.Chart):
		log.Info("chartTemplate found different")
		return true
	case hr.Spec.Interval != hrTemplate.Spec.Interval:
		log.Info("interval found different")
		return true
	default:
		return false
	}
}

// helmChartRequiresUpdate compares the v2beta1.HelmChartTemplate of the
// v2beta1.HelmRelease to the given v1beta2.HelmChart to determine if an
// update is required.
func helmChartRequiresUpdate(template, chart *helmv2beta1.HelmChartTemplate) bool {
	switch {
	case template.Spec.Chart != chart.Spec.Chart:
		fmt.Println("chart is different")
		return true
	case template.Spec.Version != "" && template.Spec.Version != chart.Spec.Version:
		fmt.Println("spec version is different")
		return true
	default:
		return false
	}
}

func isRedpandaManaged(ctx context.Context, redpandaCluster *v1alpha1.Redpanda) bool {
	log := ctrl.LoggerFrom(ctx).WithName("RedpandaReconciler.isRedpandaManaged")

	managedAnnotationKey := v1alpha1.GroupVersion.Group + "/managed"
	if managed, exists := redpandaCluster.Annotations[managedAnnotationKey]; exists && managed == NotManaged {
		log.Info(fmt.Sprintf("management is disabled; to enable it, change the '%s' annotation to true or remove it", managedAnnotationKey))
		return false
	}
	return true
}
