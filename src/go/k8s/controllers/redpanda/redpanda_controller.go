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
	sourcev1 "github.com/fluxcd/source-controller/api/v1beta2"
	"github.com/hashicorp/go-retryablehttp"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	redpandav1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
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
		For(&redpandav1alpha1.Redpanda{}).
		Complete(r)
}

func (r *RedpandaReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx)
	log.WithValues("redpanda", req.NamespacedName)

	log.Info(fmt.Sprintf("Starting reconcile loop for %v", req.NamespacedName))

	var rp redpandav1alpha1.Redpanda
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

	return r.reconcile(ctx, req, rp)
}

func (r *RedpandaReconciler) reconcile(ctx context.Context, req ctrl.Request, rp redpandav1alpha1.Redpanda) (ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx)
	log.WithValues("redpanda", req.NamespacedName)

	var hr helmv2beta1.HelmRelease

	// check if object exists, if it does then update
	// if it does not, then create
	// if we are set for deletion, then delete
	if rp.Status.HelmRelease != "" {
		hrNS, hrName := rp.Status.GetHelmRelease()
		err := r.Client.Get(ctx, types.NamespacedName{Namespace: hrNS, Name: hrName}, &hr)
		if err != nil {
			if apierrors.IsNotFound(err) {
				rp.Status.HelmRelease = ""
				if err = r.createHelmRelease(ctx, rp); err != nil {
					return ctrl.Result{}, fmt.Errorf("failed to create helmRelease: %s", err)
				}
				return ctrl.Result{}, nil
			}
			if !apierrors.IsAlreadyExists(err) {
				err = fmt.Errorf("failed to get HelmRelease '%s': %w", rp.Status.HelmRelease, err)
			}
			return ctrl.Result{}, nil
		}
	} else {
		// did not find helmRelease, then create it
		if err := r.createHelmRelease(ctx, rp); err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to create helmRelease: %s", err)
		}
		hrName, _ := rp.Status.GetHelmRelease()
		rp.Status.HelmRelease = hrName
		return ctrl.Result{}, nil
	}

	return ctrl.Result{}, nil
}

func (r *RedpandaReconciler) reconcileDelete(ctx context.Context, req ctrl.Request, rp redpandav1alpha1.Redpanda) (ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx)
	log.WithValues("redpanda", req.NamespacedName)

	if err := r.deleteHelmRelease(ctx, rp); err != nil {
		return ctrl.Result{}, err
	}

	if err := r.Client.Delete(ctx, &rp); err != nil {
		return ctrl.Result{}, err
	}

	controllerutil.RemoveFinalizer(&rp, FinalizerKey)
	if err := r.Update(ctx, &rp); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *RedpandaReconciler) createHelmRelease(ctx context.Context, rp redpandav1alpha1.Redpanda) error {
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
			return nil
		}
		return fmt.Errorf("error getting helmRepository %s", err)
	}

	// create helm release
	hRelease, err := r.createHelmReleaseFromTemplate(ctx, rp)
	if err != nil {
		return fmt.Errorf("could not create helm template: %s", err)
	}

	if err = r.Client.Create(ctx, hRelease); err != nil {
		return fmt.Errorf("could not create helm release: %s", err)
	}

	return nil
}

func (r *RedpandaReconciler) deleteHelmRelease(ctx context.Context, rp redpandav1alpha1.Redpanda) error {
	log := ctrl.LoggerFrom(ctx)
	log.WithValues("redpanda", rp.Name)

	if rp.Status.HelmRelease == "" {
		return nil
	}

	var hr helmv2beta1.HelmRelease
	hrNS, hrName := rp.Status.GetHelmRelease()
	err := r.Client.Get(ctx, types.NamespacedName{Namespace: hrNS, Name: hrName}, &hr)
	if err != nil {
		if apierrors.IsNotFound(err) {
			rp.Status.HelmRelease = ""
			return nil
		}
		err = fmt.Errorf("failed to delete HelmRelease '%s': %w", rp.Status.HelmRelease, err)
		return err
	}
	if err = r.Client.Delete(ctx, &hr); err != nil {
		err = fmt.Errorf("failed to delete HelmRelease '%s': %w", rp.Status.HelmRelease, err)
		return err
	}

	rp.Status.HelmRelease = ""
	return nil
}

func (r *RedpandaReconciler) createHelmReleaseFromTemplate(ctx context.Context, rp redpandav1alpha1.Redpanda) (*helmv2beta1.HelmRelease, error) {
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

func (r *RedpandaReconciler) createHelmRepositoryFromTemplate(rp redpandav1alpha1.Redpanda) (*sourcev1.HelmRepository, error) {
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
