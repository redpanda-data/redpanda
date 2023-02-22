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
	"github.com/ghodss/yaml"
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
				if err := r.createHelmRelease(ctx, rp); err != nil {
					return ctrl.Result{}, fmt.Errorf("failed to create helmRelease")
				}
				return ctrl.Result{}, nil
			}
			err = fmt.Errorf("failed to get HelmRelease '%s': %w", rp.Status.HelmRelease, err)
			return ctrl.Result{}, nil
		}
	} else {
		// did not find helmRelease, then create it
		if err := r.createHelmRelease(ctx, rp); err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to create helmRelease")
		}
		return ctrl.Result{}, nil
	}

	return ctrl.Result{}, nil
}

func (r *RedpandaReconciler) reconcileDelete(ctx context.Context, req ctrl.Request, rp redpandav1alpha1.Redpanda) (ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx)
	log.WithValues("redpanda", req.NamespacedName)

	if err := r.Delete(ctx, &rp); err != nil {
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

	var hr helmv2beta1.HelmRelease

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

func (r *RedpandaReconciler) createHelmReleaseFromTemplate(ctx context.Context, rp redpandav1alpha1.Redpanda) *helmv2beta1.HelmRelease {

	rpSpec := rp.Spec

	vyaml, err := yaml.Marshal(rp.Spec)

	v, err := yaml.YAMLToJSON(vyaml)

	return &helmv2beta1.HelmRelease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      rp.GetHelmReleaseName(),
			Namespace: rp.Namespace,
		},
		Spec: helmv2beta1.HelmReleaseSpec{
			Chart:  helmv2beta1.HelmChartTemplate{},
			Values: &v,
		},
	}
}
