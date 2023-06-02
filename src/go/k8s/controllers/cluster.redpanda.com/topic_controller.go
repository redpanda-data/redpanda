// Copyright 2021-2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// Package clusterredpandacom reconciles resources that comes from Redpanda dictionary like Topic, ACL and more.
package clusterredpandacom

import (
	"context"
	"errors"
	"fmt"
	"time"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/go-logr/logr"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/redpanda-data/redpanda/src/go/k8s/apis/cluster.redpanda.com/v1alpha1"
)

const FinalizerKey = "operator.redpanda.com/finalizer"

// These are for convenience when doing log.V(...) to log at a particular level. They correspond to the logr
// equivalents of the zap levels above.
const (
	TraceLevel = 2
	DebugLevel = 1
	InfoLevel  = 0
)

var (
	ErrEmptyKafkaAPISpec       = errors.New("empty kafka api spec")
	ErrScaleDownPartitionCount = errors.New("unable to scale down number of partition in topic")
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

	if !controllerutil.ContainsFinalizer(topic, FinalizerKey) {
		patch := client.MergeFrom(topic.DeepCopy())
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

	result, err := r.reconcile(ctx, topic, l)

	// Log reconciliation duration
	durationMsg := fmt.Sprintf("reconciliation finished in %s", time.Since(start).String())
	if result.RequeueAfter > 0 {
		durationMsg = fmt.Sprintf("%s, next run in %s", durationMsg, result.RequeueAfter.String())
	}
	l.Info(durationMsg, "result", result)

	if err != nil {
		l.V(4).Error(err, "failed to reconcile", "topic", topic)
	}
	return result, err
}

// SetupWithManager sets up the controller with the Manager.
func (r *TopicReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.Topic{}).
		Complete(r)
}

func (r *TopicReconciler) reconcile(ctx context.Context, topic *v1alpha1.Topic, l logr.Logger) (ctrl.Result, error) {
	l = l.WithName("reconcile")

	interval := v1.Duration{Duration: time.Second * 3}
	if topic.Spec.SynchronizationInterval != nil {
		interval = *topic.Spec.SynchronizationInterval
	}

	if topic.Spec.KafkaAPISpec == nil {
		return ctrl.Result{}, ErrEmptyKafkaAPISpec
	}

	kafkaClient, err := r.createKafkaClient(ctx, topic, l)
	if err != nil {
		return ctrl.Result{}, err
	}
	defer kafkaClient.Close()

	topicName := ""

	topicName = topic.Name
	if topic.Spec.OverwriteTopicName != nil && *topic.Spec.OverwriteTopicName != "" {
		topicName = *topic.Spec.OverwriteTopicName
	}

	kadmClient := kadm.NewClient(kafkaClient)
	defer kadmClient.Close()

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

	return ctrl.Result{RequeueAfter: interval.Duration}, nil
}

func (r *TopicReconciler) createKafkaClient(ctx context.Context, topic *v1alpha1.Topic, l logr.Logger) (*kgo.Client, error) {
	l.WithName("kafkaClient")

	if topic.Spec.KafkaAPISpec == nil {
		return nil, ErrEmptyKafkaAPISpec
	}

	kgoOpts, err := newKgoConfig(ctx, r.Client, topic, l)
	if err != nil {
		return nil, fmt.Errorf("creating kgo configuration options: %w", err)
	}

	kafkaClient, err := kgo.NewClient(kgoOpts...)
	if err != nil {
		return nil, fmt.Errorf("creating franz-go kafka client: %w", err)
	}

	return kafkaClient, nil
}
