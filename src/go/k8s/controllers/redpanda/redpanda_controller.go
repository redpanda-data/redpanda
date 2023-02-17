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
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"time"

	helmControllerAPIV2 "github.com/fluxcd/helm-controller/api/v2beta1"
	helmControllerV2 "github.com/fluxcd/helm-controller/controllers"
	"github.com/hashicorp/go-retryablehttp"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	redpandav1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
)

// RedpandaReconciler reconciles a Redpanda object
type RedpandaReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	httpClient        *retryablehttp.Client
	requeueDependency time.Duration

	helmController *helmControllerV2.HelmReleaseReconciler
}

//+kubebuilder:rbac:groups=redpanda.vectorized.io,resources=redpanda,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=redpanda.vectorized.io,resources=redpanda/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=redpanda.vectorized.io,resources=redpanda/finalizers,verbs=update

// SetupWithManager sets up the controller with the Manager.
func (r *RedpandaReconciler) SetupWithManager(mgr ctrl.Manager, opts helmControllerV2.HelmReleaseReconcilerOptions) error {
	if err := mgr.GetFieldIndexer().IndexField(context.TODO(), &helmControllerAPIV2.HelmRelease{}, helmControllerAPIV2.SourceIndexKey,
		func(o client.Object) []string {
			hr := o.(*helmControllerAPIV2.HelmRelease)
			return []string{
				fmt.Sprintf("%s/%s", hr.Spec.Chart.GetNamespace(hr.GetNamespace()), hr.GetHelmChartName()),
			}
		},
	); err != nil {
		return err
	}

	r.requeueDependency = opts.DependencyRequeueInterval

	// Configure the retryable http client used for fetching artifacts.
	// By default it retries 10 times within a 3.5 minutes window.
	httpClient := retryablehttp.NewClient()
	httpClient.RetryWaitMin = 5 * time.Second
	httpClient.RetryWaitMax = 30 * time.Second
	httpClient.RetryMax = opts.HTTPRetry
	httpClient.Logger = nil
	r.httpClient = httpClient

	recoverPanic := true

	return ctrl.NewControllerManagedBy(mgr).
		For(&redpandav1alpha1.Redpanda{}).
		// TODO add watches probably for helmchart and helmControllerAPIV2.helmReleases
		WithOptions(controller.Options{
			MaxConcurrentReconciles: opts.MaxConcurrentReconciles,
			RateLimiter:             opts.RateLimiter,
			RecoverPanic:            &recoverPanic,
		}).
		Complete(r)
}

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the Redpanda object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.14.1/pkg/reconcile
func (r *RedpandaReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	//HelmRonciler := helmControllerV2.HelmReleaseReconciler{}

	return r.reconcile(ctx, req)
}

func (r *RedpandaReconciler) reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {

	return ctrl.Result{}, nil
}
