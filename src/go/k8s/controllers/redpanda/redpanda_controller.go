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
	"time"

	helmv2beta1 "github.com/fluxcd/helm-controller/api/v2beta1"
	"github.com/fluxcd/pkg/runtime/predicates"
	sourcev1 "github.com/fluxcd/source-controller/api/v1beta2"
	"github.com/hashicorp/go-retryablehttp"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/source"

	"github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
)

// RedpandaReconciler reconciles a Redpanda object
type RedpandaReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	httpClient        *retryablehttp.Client
	requeueDependency time.Duration
}

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

	var hr helmv2beta1.HelmRelease

	// check if object exists, if it does then update
	// if it does not, then create
	// if we are set for deletion, then delete
	if rp.Status.HelmRelease != "" {
		hrName := rp.Status.GetHelmRelease()
		err := r.Client.Get(ctx, types.NamespacedName{Namespace: rp.Namespace, Name: hrName}, &hr)
		if err != nil {
			if apierrors.IsNotFound(err) {
				rp.Status.HelmRelease = ""
				if err = r.createHelmRelease(ctx, rp); err != nil {
					if !apierrors.IsAlreadyExists(err) {
						return rp, ctrl.Result{}, fmt.Errorf("failed to get HelmRelease '%s': %w", rp.Status.HelmRelease, err)
					}
					rp.Status.HelmRelease = rp.GetHelmReleaseName()
					return rp, ctrl.Result{}, nil
				}
			}
			return rp, ctrl.Result{}, fmt.Errorf("failed to get HelmRelease '%s': %w", rp.Status.HelmRelease, err)
		}
		// ok found the release, let's just move on
	} else {
		// did not find helmRelease, then create it
		if err := r.createHelmRelease(ctx, rp); err != nil {
			// could be we never updated the status and it already exists, continue and ignore error for now
			// TODO revise this logic: should we error out, or should we just continue and ignore
			if !apierrors.IsAlreadyExists(err) {
				err = fmt.Errorf("failed to create HelmRelease '%s': %w", rp.Status.HelmRelease, err)
			}
		}
	}

	rp.Status.HelmRelease = rp.GetHelmReleaseName()

	return v1alpha1.RedpandaReady(rp), ctrl.Result{}, nil
}

func (r *RedpandaReconciler) reconcileDelete(ctx context.Context, req ctrl.Request, rp v1alpha1.Redpanda) (ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx)
	log.WithValues("redpanda", req.NamespacedName)

	if err := r.deleteHelmRelease(ctx, rp); err != nil {
		return ctrl.Result{}, err
	}

	controllerutil.RemoveFinalizer(&rp, FinalizerKey)
	if err := r.Update(ctx, &rp); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *RedpandaReconciler) createHelmRelease(ctx context.Context, rp v1alpha1.Redpanda) error {
	log := ctrl.LoggerFrom(ctx)
	log.WithValues("redpanda", rp.Name)

	// create helm repository if needed
	hRepository := &sourcev1.HelmRepository{}
	if err := r.Client.Get(ctx, types.NamespacedName{Namespace: rp.Namespace, Name: rp.GetHelmRepositoryName()}, hRepository); err != nil {
		if apierrors.IsNotFound(err) {
			hRepository, err = r.createHelmRepositoryFromTemplate(rp)
			if errCreate := r.Client.Create(ctx, hRepository); errCreate != nil {
				return fmt.Errorf("error creating repository: %s", errCreate)
			}
		}
		return fmt.Errorf("error getting helmRepository %s", err)
	}

	// create helm release
	hRelease, err := r.createHelmReleaseFromTemplate(ctx, rp)
	if err != nil {
		return fmt.Errorf("could not create helm release template: %s", err)
	}

	if err = r.Client.Create(ctx, hRelease); err != nil {
		return fmt.Errorf("could not create helm release: %s", err)
	}

	return nil
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
		return fmt.Errorf("failed to delete HelmRelease '%s': %w", rp.Status.HelmRelease, err)
	}
	if err = r.Client.Delete(ctx, &hr); err != nil {
		return fmt.Errorf("failed to delete HelmRelease '%s': %w", rp.Status.HelmRelease, err)
	}

	rp.Status.HelmRelease = ""
	return nil
}

func (r *RedpandaReconciler) createHelmReleaseFromTemplate(ctx context.Context, rp v1alpha1.Redpanda) (*helmv2beta1.HelmRelease, error) {
	log := ctrl.LoggerFrom(ctx)
	log.WithValues("redpanda", rp.Name)

	values, err := rp.GetValuesJson()
	if err != nil {
		return nil, fmt.Errorf("could not parse clusterSpec to json: %s", err)
	}
	log.Info(fmt.Sprintf("values file to use: %s", values))

	return &helmv2beta1.HelmRelease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      rp.GetHelmReleaseName(),
			Namespace: rp.Namespace,
		},
		Spec: helmv2beta1.HelmReleaseSpec{
			Chart: helmv2beta1.HelmChartTemplate{
				Spec: helmv2beta1.HelmChartTemplateSpec{
					Chart:    "redpanda",
					Version:  rp.Spec.ChartVersion,
					Interval: &metav1.Duration{Duration: 1 * time.Minute},
					SourceRef: helmv2beta1.CrossNamespaceObjectReference{
						Kind:      "HelmRepository",
						Name:      rp.GetHelmRepositoryName(),
						Namespace: rp.Namespace,
					},
				},
			},
			Values:   values,
			Interval: metav1.Duration{Duration: 5 * time.Minute},
		},
	}, nil
}

func (r *RedpandaReconciler) createHelmRepositoryFromTemplate(rp v1alpha1.Redpanda) (*sourcev1.HelmRepository, error) {
	return &sourcev1.HelmRepository{
		ObjectMeta: metav1.ObjectMeta{
			Name:      rp.GetHelmRepositoryName(),
			Namespace: rp.Namespace,
		},
		Spec: sourcev1.HelmRepositorySpec{
			Interval: metav1.Duration{Duration: 5 * time.Minute},
			URL:      "https://charts.redpanda.com/",
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
