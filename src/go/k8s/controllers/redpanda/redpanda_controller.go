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
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"reflect"
	"time"

	helmv2beta1 "github.com/fluxcd/helm-controller/api/v2beta1"
	"github.com/fluxcd/pkg/apis/meta"
	"github.com/fluxcd/pkg/runtime/predicates"
	sourcev1 "github.com/fluxcd/source-controller/api/v1beta2"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	kuberecorder "k8s.io/client-go/tools/record"
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

// RedpandaReconciler reconciles a Redpanda object
type RedpandaReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	kuberecorder.EventRecorder

	RequeueHelmDeps time.Duration
}

// flux resources
// +kubebuilder:rbac:groups=helm.toolkit.fluxcd.io,resources=helmreleases,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=helm.toolkit.fluxcd.io,resources=helmreleases/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=helm.toolkit.fluxcd.io,resources=helmreleases/finalizers,verbs=update
// +kubebuilder:rbac:groups=source.toolkit.fluxcd.io,resources=helmcharts,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=source.toolkit.fluxcd.io,resources=helmcharts/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=source.toolkit.fluxcd.io,resources=helmcharts/finalizers,verbs=get;create;update;patch;delete
// +kubebuilder:rbac:groups=source.toolkit.fluxcd.io,resources=helmrepositories,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=source.toolkit.fluxcd.io,resources=helmrepositories/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=source.toolkit.fluxcd.io,resources=helmrepositories/finalizers,verbs=get;create;update;patch;delete

// flux additional resources
// +kubebuilder:rbac:groups=source.toolkit.fluxcd.io,resources=buckets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=source.toolkit.fluxcd.io,resources=gitrepositories,verbs=get;list;watch;create;update;patch;delete

// redpanda resources
//+kubebuilder:rbac:groups=redpanda.vectorized.io,resources=redpanda,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=redpanda.vectorized.io,resources=redpanda/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=redpanda.vectorized.io,resources=redpanda/finalizers,verbs=update

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

func (r *RedpandaReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	start := time.Now()
	log := ctrl.LoggerFrom(ctx)
	log.WithValues("redpanda", req.NamespacedName)

	log.Info(fmt.Sprintf("Starting reconcile loop for %v", req.NamespacedName))

	var rp v1alpha1.Redpanda
	if err := r.Get(ctx, req.NamespacedName, &rp); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// add finalizer if not exist
	if !controllerutil.ContainsFinalizer(&rp, FinalizerKey) {
		patch := client.MergeFrom(rp.DeepCopy())
		controllerutil.AddFinalizer(&rp, FinalizerKey)
		if err := r.Patch(ctx, &rp, patch); err != nil {
			log.Error(err, "unable to register finalizer")
			return ctrl.Result{}, err
		}
	}

	// Examine if the object is under deletion
	if !rp.ObjectMeta.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, req, rp)
	}

	rp, result, err := r.reconcile(ctx, req, rp)

	// Update status after reconciliation.
	if updateStatusErr := r.patchRedpandaStatus(ctx, &rp); updateStatusErr != nil {
		log.Error(updateStatusErr, "unable to update status after reconciliation")
		return ctrl.Result{Requeue: true}, updateStatusErr
	}

	// Log reconciliation duration
	durationMsg := fmt.Sprintf("reconcilation finished in %s", time.Now().Sub(start).String())
	if result.RequeueAfter > 0 {
		durationMsg = fmt.Sprintf("%s, next run in %s", durationMsg, result.RequeueAfter.String())
	}
	log.Info(durationMsg)

	return result, err
}

func (r *RedpandaReconciler) reconcile(ctx context.Context, req ctrl.Request, rp v1alpha1.Redpanda) (v1alpha1.Redpanda, ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx)
	log.WithValues("redpanda", req.NamespacedName)

	// Observe HelmRelease generation.
	if rp.Status.ObservedGeneration != rp.Generation {
		rp.Status.ObservedGeneration = rp.Generation
		rp = v1alpha1.RedpandaProgressing(rp)
		if updateStatusErr := r.patchRedpandaStatus(ctx, &rp); updateStatusErr != nil {
			log.Error(updateStatusErr, "unable to update status after generation update")
			return rp, ctrl.Result{Requeue: true}, updateStatusErr
		}
	}

	// Check if HelmRepository exists or create it
	rp, repo, err := r.reconcileHelmRepository(ctx, rp)
	if err != nil {
		return rp, ctrl.Result{}, err
	}

	if repo.Generation != repo.Status.ObservedGeneration || !apimeta.IsStatusConditionTrue(repo.Status.Conditions, meta.ReadyCondition) {
		msg := fmt.Sprintf("HelmRepository '%s/%s' is not ready", repo.GetNamespace(), repo.GetName())
		r.event(&rp, rp.Status.LastAttemptedRevision, v1alpha1.EventSeverityInfo, fmt.Sprintf("HelmRepository '%s/%s' is not ready", repo.GetNamespace(), repo.GetName()))
		log.Info(msg)
		// Do not requeue immediately.
		return v1alpha1.RedpandaNotReady(rp, "ArtifactFailed", msg), ctrl.Result{RequeueAfter: r.RequeueHelmDeps}, nil
	}

	r.event(&rp, rp.Status.LastAttemptedRevision, v1alpha1.EventSeverityInfo, fmt.Sprintf("HelmRepository '%s/%s' is ready!", repo.GetNamespace(), repo.GetName()))

	// Check if HelmRelease exists or create it also
	rp, hr, err := r.reconcileHelmRelease(ctx, rp)
	if err != nil {
		return rp, ctrl.Result{}, err
	}
	if hr.Name == "" {
		log.Info(fmt.Sprintf("Created HelmRelease for '%s/%s', will requeue", rp.Namespace, rp.Name))
		return rp, ctrl.Result{}, err
	}
	if hr.Generation != hr.Status.ObservedGeneration || !apimeta.IsStatusConditionTrue(hr.Status.Conditions, meta.ReadyCondition) {
		msg := fmt.Sprintf("HelmRelease '%s/%s' is not ready", hr.GetNamespace(), hr.GetName())
		r.event(&rp, rp.Status.LastAttemptedRevision, v1alpha1.EventSeverityInfo, msg)
		log.Info(msg)
		// Do not requeue immediately.
		return v1alpha1.RedpandaNotReady(rp, "ArtifactFailed", msg), ctrl.Result{RequeueAfter: r.RequeueHelmDeps}, nil
	}

	r.event(&rp, rp.Status.LastAttemptedRevision, v1alpha1.EventSeverityInfo, fmt.Sprintf("HelmRelease '%s/%s' is ready!", hr.GetNamespace(), hr.GetName()))

	// todo update status here before setting ready

	return v1alpha1.RedpandaReady(rp), ctrl.Result{}, nil
}

func (r *RedpandaReconciler) reconcileHelmRelease(ctx context.Context, rp v1alpha1.Redpanda) (v1alpha1.Redpanda, *helmv2beta1.HelmRelease, error) {
	log := ctrl.LoggerFrom(ctx)
	rpKey := types.NamespacedName{Namespace: rp.Namespace, Name: rp.Name}
	log.WithValues("redpanda", rpKey)

	// Check if HelmRelease exists or create it
	hr := &helmv2beta1.HelmRelease{}

	// check if object exists, if it does then update
	// if it does not, then create
	// if we are set for deletion, then delete
	if rp.Status.HelmRelease != "" {
		key := types.NamespacedName{Namespace: rp.Namespace, Name: rp.Status.GetHelmRelease()}
		err := r.Client.Get(ctx, key, hr)
		if err != nil {
			if apierrors.IsNotFound(err) {
				rp.Status.HelmRelease = ""
				if hr, err = r.createHelmRelease(ctx, rp); err != nil {
					if !apierrors.IsAlreadyExists(err) {
						r.event(&rp, rp.Status.LastAttemptedRevision, v1alpha1.EventSeverityError, err.Error())
						return rp, hr, fmt.Errorf("failed to create HelmRelease '%s/%s': %w", rp.Namespace, rp.Status.HelmRelease, err)
					}
					rp.Status.HelmRelease = rp.GetHelmReleaseName()
					return rp, hr, nil
				}
				r.event(&rp, rp.Status.LastAttemptedRevision, v1alpha1.EventSeverityInfo, fmt.Sprintf("HelmRelease '%s/%s' created ", rp.Namespace, rp.GetHelmReleaseName()))
			}
			return rp, hr, fmt.Errorf("failed to get HelmRelease '%s/%s': %w", rp.Namespace, rp.Status.HelmRelease, err)
		} else {
			// Check if we need to update here
			hrTemplate, errTemplated := r.createHelmReleaseFromTemplate(ctx, rp)
			if errTemplated != nil {
				r.event(&rp, rp.Status.LastAttemptedRevision, v1alpha1.EventSeverityError, errTemplated.Error())
				return rp, hr, errTemplated
			}

			if r.helmReleaseRequiresUpdate(hr, hrTemplate) {
				hr.Spec = hrTemplate.Spec
				if err = r.Client.Update(ctx, hr); err != nil {
					r.event(&rp, rp.Status.LastAttemptedRevision, v1alpha1.EventSeverityError, err.Error())
					return rp, hr, err
				}
				r.event(&rp, rp.Status.LastAttemptedRevision, v1alpha1.EventSeverityInfo, fmt.Sprintf("HelmRelease '%s/%s' updated", rp.Namespace, rp.GetHelmReleaseName()))
				rp.Status.HelmRelease = rp.GetHelmReleaseName()
			}

			rp.Status.HelmRelease = rp.GetHelmReleaseName()
			return rp, hr, nil
		}
		// ok found the release, let's just move on
	} else {
		// did not find helmRelease, then create it
		if hr, err := r.createHelmRelease(ctx, rp); err != nil {
			// could be we never updated the status and it already exists, continue and ignore error for now
			// TODO revise this logic: should we error out, or should we just continue and ignore
			if !apierrors.IsAlreadyExists(err) {
				r.event(&rp, rp.Status.LastAttemptedRevision, v1alpha1.EventSeverityError, err.Error())
				return rp, hr, fmt.Errorf("failed to create HelmRelease '%s/%s': %w", rp.Namespace, rp.Status.HelmRelease, err)
			}
		}
		r.event(&rp, rp.Status.LastAttemptedRevision, v1alpha1.EventSeverityInfo, fmt.Sprintf("HelmRelease '%s/%s' created ", rp.Namespace, rp.GetHelmReleaseName()))
	}
	rp.Status.HelmRelease = rp.GetHelmReleaseName()

	return rp, hr, nil
}

func (r *RedpandaReconciler) reconcileHelmRepository(ctx context.Context, rp v1alpha1.Redpanda) (v1alpha1.Redpanda, *sourcev1.HelmRepository, error) {
	// Check if HelmRepository exists or create it
	repo := &sourcev1.HelmRepository{}
	if err := r.Client.Get(ctx, types.NamespacedName{Namespace: rp.Namespace, Name: rp.GetHelmRepositoryName()}, repo); err != nil {
		if apierrors.IsNotFound(err) {
			repo, err = r.createHelmRepositoryFromTemplate(rp)
			if errCreate := r.Client.Create(ctx, repo); errCreate != nil {
				r.event(&rp, rp.Status.LastAttemptedRevision, v1alpha1.EventSeverityError, fmt.Sprintf("error creating HelmRepository: %s", errCreate))
				return rp, repo, fmt.Errorf("error creating HelmRepository: %s", errCreate)
			}
			r.event(&rp, rp.Status.LastAttemptedRevision, v1alpha1.EventSeverityInfo, fmt.Sprintf("HelmRepository '%s/%s' created ", rp.Namespace, rp.GetHelmRepositoryName()))
		} else {
			r.event(&rp, rp.Status.LastAttemptedRevision, v1alpha1.EventSeverityError, fmt.Sprintf("error getting HelmRepository: %s", err))
			return rp, repo, fmt.Errorf("error getting HelmRepository: %s", err)
		}
	}
	rp.Status.HelmRepository = rp.GetHelmRepositoryName()

	return rp, repo, nil
}

func (r *RedpandaReconciler) reconcileDelete(ctx context.Context, req ctrl.Request, rp v1alpha1.Redpanda) (ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx)
	log.WithValues("redpanda", req.NamespacedName)

	if err := r.deleteHelmRelease(ctx, rp); err != nil {
		return ctrl.Result{}, err
	}

	controllerutil.RemoveFinalizer(&rp, FinalizerKey)
	if err := r.Client.Update(ctx, &rp); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *RedpandaReconciler) createHelmRelease(ctx context.Context, rp v1alpha1.Redpanda) (*helmv2beta1.HelmRelease, error) {
	log := ctrl.LoggerFrom(ctx)
	log.WithValues("redpanda", rp.Name)

	// create helm release
	hRelease, err := r.createHelmReleaseFromTemplate(ctx, rp)
	if err != nil {
		r.event(&rp, rp.Status.LastAttemptedRevision, v1alpha1.EventSeverityError, fmt.Sprintf("could not create helm release template: %s", err))
		return hRelease, fmt.Errorf("could not create helm release template: %s", err)
	}

	return hRelease, r.Client.Create(ctx, hRelease)
}

func (r *RedpandaReconciler) deleteHelmRelease(ctx context.Context, rp v1alpha1.Redpanda) error {
	log := ctrl.LoggerFrom(ctx)
	log.WithValues("redpanda", rp.Name)

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

func (r *RedpandaReconciler) createHelmReleaseFromTemplate(ctx context.Context, rp v1alpha1.Redpanda) (*helmv2beta1.HelmRelease, error) {
	log := ctrl.LoggerFrom(ctx)
	log.WithValues("redpanda", rp.Name)

	values, err := rp.GetValuesJson()
	if err != nil {
		return nil, fmt.Errorf("could not parse clusterSpec to json: %s", err)
	}

	hasher := sha1.New()
	hasher.Write(values.Raw)
	sha := base64.URLEncoding.EncodeToString(hasher.Sum(nil))
	r.event(&rp, rp.Status.LastAttemptedRevision, v1alpha1.EventSeverityInfo, fmt.Sprintf("values file successfully parsed; %s", sha))
	log.Info(fmt.Sprintf("values file to use: %s", values))

	timeout := rp.Spec.ChartRef.Timeout
	if timeout == nil {
		timeout = &metav1.Duration{Duration: 10 * time.Minute}
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
		},
	}, nil
}

func (r *RedpandaReconciler) createHelmRepositoryFromTemplate(rp v1alpha1.Redpanda) (*sourcev1.HelmRepository, error) {
	owner := r.createOwnerShipRefObj(&rp)
	return &sourcev1.HelmRepository{
		ObjectMeta: metav1.ObjectMeta{
			Name:            rp.GetHelmRepositoryName(),
			Namespace:       rp.Namespace,
			OwnerReferences: []metav1.OwnerReference{owner},
		},
		Spec: sourcev1.HelmRepositorySpec{
			Interval: metav1.Duration{Duration: 30 * time.Second},
			URL:      v1alpha1.RedpandaChartRepository,
		},
	}, nil
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
	var meta map[string]string
	if revision != "" {
		meta = map[string]string{v2.GroupVersion.Group + "/revision": revision}
	}
	eventType := "Normal"
	if severity == v1alpha1.EventSeverityError {
		eventType = "Warning"
	}
	r.EventRecorder.AnnotatedEventf(rp, meta, eventType, severity, msg)
}

func (r *RedpandaReconciler) helmReleaseRequiresUpdate(hr *helmv2beta1.HelmRelease, hrTemplate *helmv2beta1.HelmRelease) bool {
	log := ctrl.LoggerFrom(context.Background())
	log.WithValues("redpanda", hr.Name)

	switch {
	case !reflect.DeepEqual(hr.Spec.Values, hrTemplate.Spec.Values):
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
func helmChartRequiresUpdate(template *helmv2beta1.HelmChartTemplate, chart *helmv2beta1.HelmChartTemplate) bool {
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

func (r *RedpandaReconciler) createOwnerShipRefObj(rp *v1alpha1.Redpanda) metav1.OwnerReference {
	return metav1.OwnerReference{
		APIVersion: rp.APIVersion,
		Kind:       rp.Kind,
		Name:       rp.Name,
		UID:        rp.UID,
	}
}
