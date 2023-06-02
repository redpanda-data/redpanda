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
	"fmt"
	"time"

	"github.com/redpanda-data/redpanda/src/go/k8s/apis/cluster.redpanda.com/v1alpha1"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const FinalizerKey = "operator.redpanda.com/finalizer"

// TopicReconciler reconciles a Topic object
type TopicReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

//+kubebuilder:rbac:groups=cluster.redpanda.com,namespace=default,resources=topics,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=cluster.redpanda.com,namespace=default,resources=topics/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=cluster.redpanda.com,namespace=default,resources=topics/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.14.4/pkg/reconcile
func (r *TopicReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	start := time.Now()
	l := log.FromContext(ctx).WithName("TopicReconciler.Reconcile")

	l.Info("Starting reconcile loop")

	topic := &v1alpha1.Topic{}
	if err := r.Client.Get(ctx, req.NamespacedName, topic); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// add finalizer if not exist
	if !controllerutil.ContainsFinalizer(topic, FinalizerKey) {
		patch := client.MergeFrom(topic.DeepCopy())
		topic.Status.LastAttemptedRevision = topic.ResourceVersion
		controllerutil.AddFinalizer(topic, FinalizerKey)
		if err := r.Patch(ctx, topic, patch); err != nil {
			l.Error(err, "unable to register finalizer")
			return ctrl.Result{}, err
		}
	}

	// Examine if the object is under deletion
	if !topic.ObjectMeta.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	result, err := r.reconcile(ctx, topic)

	// Log reconciliation duration
	durationMsg := fmt.Sprintf("reconciliation finished in %s", time.Since(start).String())
	if result.RequeueAfter > 0 {
		durationMsg = fmt.Sprintf("%s, next run in %s", durationMsg, result.RequeueAfter.String())
	}
	l.Info(durationMsg)

	return ctrl.Result{}, err
}

// SetupWithManager sets up the controller with the Manager.
func (r *TopicReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.Topic{}).
		Complete(r)
}

func (r *TopicReconciler) reconcile(ctx context.Context, topic *v1alpha1.Topic) (ctrl.Result, error) {
	if topic.Spec.KafkaAPISpec == nil {
		return ctrl.Result{}, fmt.Errorf("empty kafka api spec")
	}

	kgoOpts, err := newKgoConfig(ctx, topic)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("creating kgo configuration options: %w", err)
	}

	kafkaClient, err := kgo.NewClient(kgoOpts...)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("creating franz-go kafka client: %w", err)
	}

	topicName := ""

	topicName = topic.Name
	if topic.Spec.OverwriteTopicName != nil && *topic.Spec.OverwriteTopicName != "" {
		topicName = *topic.Spec.OverwriteTopicName
	}

	kadmClient := kadm.NewClient(kafkaClient)

	partition := int32(-1)
	if topic.Spec.Partitions != nil {
		partition = int32(*topic.Spec.Partitions)
	}
	replicationFactor := int16(-1)
	if topic.Spec.ReplicationFactor != nil {
		replicationFactor = int16(*topic.Spec.ReplicationFactor)
	}
	ctr, err := kadmClient.CreateTopic(ctx, partition, replicationFactor, topic.Spec.AdditionalConfig, topicName)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("creating topic : %w", err)
	}

	if ctr.Err != nil {
		return ctrl.Result{}, fmt.Errorf("creating topic : %w", ctr.Err)
	}

	// TODO: Retrieve the information how to contact Admin API to get configuration
	// TODO: Create franz-go instance that would use kafka Admin command to CRUD some topic.

	return ctrl.Result{}, nil
}
