// Copyright 2021-2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package clusterredpandacom

import (
	"context"

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	clusterredpandacomv1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/cluster.redpanda.com/v1alpha1"
)

// TopicReconciler reconciles a Topic object
type TopicReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

//+kubebuilder:rbac:groups=cluster.redpanda.com,namespace=default,resources=topics,verbs=get;list;watch;update;patch
//+kubebuilder:rbac:groups=cluster.redpanda.com,namespace=default,resources=topics/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=cluster.redpanda.com,namespace=default,resources=topics/finalizers,verbs=update

// For cluster scoped operator

//+kubebuilder:rbac:groups=cluster.redpanda.com,resources=topics,verbs=get;list;watch;update;patch
//+kubebuilder:rbac:groups=cluster.redpanda.com,resources=topics/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=cluster.redpanda.com,resources=topics/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the Topic object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.14.4/pkg/reconcile
func (r *TopicReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	_ = log.FromContext(ctx)

	// TODO(user): your logic here

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *TopicReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&clusterredpandacomv1alpha1.Topic{}).
		Complete(r)
}
