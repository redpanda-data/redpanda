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
	"strconv"
	"time"

	"github.com/go-logr/logr"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kuberecorder "k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
	v2 "sigs.k8s.io/controller-runtime/pkg/webhook/conversion/testdata/api/v2"

	"github.com/redpanda-data/redpanda/src/go/k8s/apis/cluster.redpanda.com/v1alpha1"
)

const (
	NoneConstantString = "none"
	FinalizerKey       = "operator.redpanda.com/finalizer"
)

// These are for convenience when doing log.V(...) to log at a particular level. They correspond to the logr
// equivalents of the zap levels above.
const (
	TraceLevel = 2
	DebugLevel = 1
	InfoLevel  = 0
)

var (
	ErrEmptyKafkaAPISpec           = errors.New("empty kafka api spec")
	ErrScaleDownPartitionCount     = errors.New("unable to scale down number of partition in topic")
	ErrEmptyTopicConfigDescription = errors.New("topic config description response is empty")
	ErrEmptyMetadataTopic          = errors.New("metadata topic response is empty")
	ErrWrongCreateTopicResponse    = errors.New("requested topic was not part of create topic response")
)

// TopicReconciler reconciles a Topic object
type TopicReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	kuberecorder.EventRecorder
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

	topic, result, err := r.reconcile(ctx, topic, l)

	// Update status after reconciliation.
	if updateStatusErr := r.patchTopicStatus(ctx, topic, l); updateStatusErr != nil {
		l.Error(updateStatusErr, "unable to update topic status after reconciliation")
		err = errors.Join(err, updateStatusErr)
		result.Requeue = true
	}

	// Log reconciliation duration
	durationMsg := fmt.Sprintf("reconciliation finished in %s", time.Since(start).String())
	if result.RequeueAfter > 0 {
		durationMsg = fmt.Sprintf("%s, next run in %s", durationMsg, result.RequeueAfter.String())
	}
	l.Info(durationMsg, "result", result)

	if err != nil {
		l.V(DebugLevel).Error(err, "failed to reconcile", "topic", topic)
	} else if !topic.DeletionTimestamp.IsZero() {
		patch := client.MergeFrom(topic.DeepCopy())
		controllerutil.RemoveFinalizer(topic, FinalizerKey)
		topic = v1alpha1.TopicReady(topic)
		if err = r.Patch(ctx, topic, patch); err != nil {
			l.Error(err, "unable to remove finalizer")
			return ctrl.Result{}, err
		}
	}
	return result, err
}

// SetupWithManager sets up the controller with the Manager.
func (r *TopicReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.Topic{}).
		Complete(r)
}

func (r *TopicReconciler) reconcile(ctx context.Context, topic *v1alpha1.Topic, l logr.Logger) (*v1alpha1.Topic, ctrl.Result, error) {
	l = l.WithName("reconcile")

	interval := v1.Duration{Duration: time.Second * 3}
	if topic.Spec.SynchronizationInterval != nil {
		interval = *topic.Spec.SynchronizationInterval
	}

	if topic.Status.ObservedGeneration != topic.Generation {
		topic.Status.ObservedGeneration = topic.Generation
		topic = v1alpha1.TopicProgressing(topic)
		l.V(TraceLevel).Info("bump observed generation", "observed generation", topic.Generation)
	}

	kafkaClient, err := r.createKafkaClient(ctx, topic, l)
	if err != nil {
		return v1alpha1.TopicFailed(topic), ctrl.Result{}, err
	}
	defer kafkaClient.Close()

	partition := int32(-1)
	if topic.Spec.Partitions != nil {
		partition = int32(*topic.Spec.Partitions)
	}
	replicationFactor := int16(-1)
	if topic.Spec.ReplicationFactor != nil {
		replicationFactor = int16(*topic.Spec.ReplicationFactor)
	}

	// Examine if the object is under deletion
	if !topic.ObjectMeta.DeletionTimestamp.IsZero() {
		l.V(DebugLevel).Info("delete topic", "topic-name", topic.GetName())
		err = r.deleteTopic(ctx, topic, kafkaClient)
		if err != nil {
			return v1alpha1.TopicFailed(topic), ctrl.Result{}, fmt.Errorf("unable to delete topic: %w", err)
		}
		return v1alpha1.TopicReady(topic), ctrl.Result{}, nil
	}

	if _, err = r.describeTopic(ctx, topic, kafkaClient); errors.Is(err, kerr.UnknownTopicOrPartition) {
		err = r.createTopic(ctx, topic, kafkaClient, partition, replicationFactor)
		if err != nil && !errors.Is(err, kerr.TopicAlreadyExists) {
			return v1alpha1.TopicFailed(topic), ctrl.Result{}, err
		}
		l.V(DebugLevel).Info("topic created",
			"topic-name", topic.GetName(),
			"topic-configuration", topic.Spec.AdditionalConfig,
			"topic-partition", partition,
			"topic-replication-factor", replicationFactor)
		return v1alpha1.TopicReady(topic), ctrl.Result{RequeueAfter: interval.Duration}, nil
	}

	l.V(DebugLevel).Info("reconcile partition count", "partition", partition)
	var numReplicas int16
	numReplicas, err = r.reconcilePartition(ctx, topic, kafkaClient, int(partition))
	if err != nil {
		return v1alpha1.TopicFailed(topic), ctrl.Result{}, err
	}

	l.V(DebugLevel).Info("topic configuration synchronization", "topic-configuration", topic.Spec.AdditionalConfig)
	resp, err := r.describeTopic(ctx, topic, kafkaClient)
	if err != nil {
		return v1alpha1.TopicFailed(topic), ctrl.Result{}, err
	}

	setConf, specialWriteConf, deleteConf := generateConf(resp.Resources[0].Configs, topic.Spec.AdditionalConfig, replicationFactor, numReplicas)
	// Redpanda fails to set both remote.read and remote.write when passed
	// at the same time, so we issue first the set request for write,
	// then the rest of the requests.
	// See https://github.com/redpanda-data/redpanda/issues/9191 and
	// https://github.com/redpanda-data/redpanda/issues/4499
	if len(specialWriteConf) > 0 {
		if err = r.alterTopicConfiguration(ctx, topic, specialWriteConf, deleteConf, kafkaClient, l); err != nil {
			return v1alpha1.TopicFailed(topic), ctrl.Result{}, err
		}
	}

	if err = r.alterTopicConfiguration(ctx, topic, setConf, deleteConf, kafkaClient, l); err != nil {
		return v1alpha1.TopicFailed(topic), ctrl.Result{}, err
	}

	return r.successfulTopicReconciliation(topic), ctrl.Result{RequeueAfter: interval.Duration}, nil
}

func (r *TopicReconciler) successfulTopicReconciliation(topic *v1alpha1.Topic) *v1alpha1.Topic {
	if r.EventRecorder != nil {
		r.EventRecorder.AnnotatedEventf(topic,
			map[string]string{v2.GroupVersion.Group + "/revision": topic.ResourceVersion},
			corev1.EventTypeNormal, v1alpha1.EventTopicSynced, "configuration synced")
	}
	return v1alpha1.TopicReady(topic)
}

func (r *TopicReconciler) reconcilePartition(ctx context.Context, topic *v1alpha1.Topic, cl *kgo.Client, partition int) (int16, error) {
	reqMetadata := kmsg.NewPtrMetadataRequest()
	reqTopic := kmsg.NewMetadataRequestTopic()
	reqTopic.Topic = kmsg.StringPtr(topic.GetName())
	reqMetadata.Topics = append(reqMetadata.Topics, reqTopic)

	respMetadata, err := reqMetadata.RequestWith(ctx, cl)
	if err != nil {
		return 0, r.recordErrorEvent(err, topic, v1alpha1.EventTopicConfigurationDescribeFailure, "failed topic (%s) metadata retrieval library error", topic.GetName())
	}

	if len(respMetadata.Topics) == 0 {
		return 0, r.recordErrorEvent(ErrEmptyMetadataTopic, topic, v1alpha1.EventTopicConfigurationDescribeFailure, "metadata topic (%s) request return empty response", topic.GetName())
	}

	if err = kerr.ErrorForCode(respMetadata.Topics[0].ErrorCode); err != nil {
		return 0, r.recordErrorEvent(err, topic, v1alpha1.EventTopicConfigurationDescribeFailure, "failed topic (%s) metadata retrieval library error", topic.GetName())
	}

	if len(respMetadata.Topics[0].Partitions) > partition {
		return 0, r.recordErrorEvent(ErrScaleDownPartitionCount, topic, v1alpha1.EventTopicConfigurationDescribeFailure, "unable to update topic (%s)", topic.GetName())
	}

	if len(respMetadata.Topics[0].Partitions) == partition {
		return int16(len(respMetadata.Topics[0].Partitions[0].Replicas)), nil
	}

	reqPartition := kmsg.NewCreatePartitionsRequest()
	rt := kmsg.NewCreatePartitionsRequestTopic()
	rt.Topic = topic.GetName()
	rt.Count = int32(partition)
	reqPartition.Topics = append(reqPartition.Topics, rt)

	respPartition, err := reqPartition.RequestWith(ctx, cl)
	if err != nil {
		return 0, r.recordErrorEvent(err, topic, v1alpha1.EventTopicConfigurationAlteringFailure, "failed change topic (%s) partition count (%d) library error", topic.GetName(), partition)
	}
	if err = kerr.ErrorForCode(respPartition.Topics[0].ErrorCode); err != nil {
		errMsg := NoneConstantString
		if respPartition.Topics[0].ErrorMessage != nil {
			errMsg = *respPartition.Topics[0].ErrorMessage
		}
		return 0, r.recordErrorEvent(err, topic, v1alpha1.EventTopicConfigurationAlteringFailure, "failed change topic (%s) partition count (%d) library error (%s)", topic.GetName(), partition, errMsg)
	}

	return int16(len(respMetadata.Topics[0].Partitions[0].Replicas)), nil
}

func (r *TopicReconciler) alterTopicConfiguration(ctx context.Context, topic *v1alpha1.Topic, setConf map[string]string, deleteConf map[string]any, kafkaClient *kgo.Client, l logr.Logger) error {
	l.WithName("alterTopicConfiguration")
	reqAltConfig := kmsg.NewPtrIncrementalAlterConfigsRequest()
	size := len(setConf) + len(deleteConf)
	configs := make([]kmsg.IncrementalAlterConfigsRequestResourceConfig, 0, size)
	for _, pair := range []struct {
		kvs map[string]string
		op  kmsg.IncrementalAlterConfigOp
	}{
		{setConf, kmsg.IncrementalAlterConfigOpSet},                  // 0 == set
		{map[string]string{}, kmsg.IncrementalAlterConfigOpAppend},   // 2 == append
		{map[string]string{}, kmsg.IncrementalAlterConfigOpSubtract}, // 3 == subtract
	} {
		for k, v := range pair.kvs {
			config := kmsg.NewIncrementalAlterConfigsRequestResourceConfig()
			config.Name = k
			config.Op = pair.op
			config.Value = kmsg.StringPtr(v)
			configs = append(configs, config)
		}
	}

	for keyToDelete := range deleteConf {
		config := kmsg.NewIncrementalAlterConfigsRequestResourceConfig()
		config.Name = keyToDelete
		config.Op = kmsg.IncrementalAlterConfigOpDelete // 1 == delete
		configs = append(configs, config)
	}

	if len(configs) == 0 {
		if r.EventRecorder != nil {
			r.EventRecorder.AnnotatedEventf(topic,
				map[string]string{v2.GroupVersion.Group + "/revision": topic.ResourceVersion},
				corev1.EventTypeNormal, v1alpha1.EventTopicAlreadySynced, "configuration not changed")
		}
		return nil
	}

	reqTopic := kmsg.NewIncrementalAlterConfigsRequestResource()
	reqTopic.ResourceType = kmsg.ConfigResourceTypeTopic
	reqTopic.ResourceName = topic.GetName()
	reqTopic.Configs = configs
	reqAltConfig.Resources = append(reqAltConfig.Resources, reqTopic)

	l.V(TraceLevel).Info("alter topic configuration", "topic-name", topic.GetName(), "configs", configs)
	respAltConfig, err := reqAltConfig.RequestWith(ctx, kafkaClient)
	if err != nil {
		return r.recordErrorEvent(err, topic, v1alpha1.EventTopicConfigurationAlteringFailure, "alter topic configuration (%s) library error", topic.GetName())
	}

	if err = kerr.ErrorForCode(respAltConfig.Resources[0].ErrorCode); err != nil {
		errMsg := NoneConstantString
		if respAltConfig.Resources[0].ErrorMessage != nil {
			errMsg = *respAltConfig.Resources[0].ErrorMessage
		}
		return r.recordErrorEvent(err, topic, v1alpha1.EventTopicConfigurationAlteringFailure, "alter topic configuration (%s) incremental alter config (%s)", topic.GetName(), errMsg)
	}
	return nil
}

func (r *TopicReconciler) describeTopic(ctx context.Context, topic *v1alpha1.Topic, kafkaClient *kgo.Client) (*kmsg.DescribeConfigsResponse, error) {
	req := kmsg.NewPtrDescribeConfigsRequest()
	reqResource := kmsg.NewDescribeConfigsRequestResource()
	reqResource.ResourceType = kmsg.ConfigResourceTypeTopic
	reqResource.ResourceName = topic.GetName()
	req.Resources = append(req.Resources, reqResource)
	resp, err := req.RequestWith(ctx, kafkaClient)
	if err != nil {
		return nil, r.recordErrorEvent(err, topic, v1alpha1.EventTopicConfigurationDescribeFailure, "describing topic configuration (%s) library error", topic.GetName())
	}

	if len(resp.Resources) == 0 {
		return nil, r.recordErrorEvent(ErrEmptyTopicConfigDescription, topic, v1alpha1.EventTopicConfigurationDescribeFailure, "describing topic configuration (%s) DescribeConfigsResponse error", topic.GetName())
	}

	if err = kerr.ErrorForCode(resp.Resources[0].ErrorCode); err != nil {
		errMsg := NoneConstantString
		if resp.Resources[0].ErrorMessage != nil {
			errMsg = *resp.Resources[0].ErrorMessage
		}
		return nil, r.recordErrorEvent(err, topic, v1alpha1.EventTopicConfigurationDescribeFailure, "describing topic configuration (%s) DescribeConfigsResponse error (%s)", topic.GetName(), errMsg)
	}

	return resp, nil
}

func (r *TopicReconciler) createTopic(ctx context.Context, topic *v1alpha1.Topic, kafkaClient *kgo.Client, partition int32, replicationFactor int16) error {
	req := kmsg.NewCreateTopicsRequest()
	rt := kmsg.NewCreateTopicsRequestTopic()
	rt.Topic = topic.GetName()
	rt.NumPartitions = partition
	rt.ReplicationFactor = replicationFactor
	for k, v := range topic.Spec.AdditionalConfig {
		rc := kmsg.NewCreateTopicsRequestTopicConfig()
		rc.Name = k
		rc.Value = v
		rt.Configs = append(rt.Configs, rc)
	}
	req.Topics = append(req.Topics, rt)
	resp, err := req.RequestWith(ctx, kafkaClient)
	if err != nil {
		return r.recordErrorEvent(err, topic, v1alpha1.EventTopicCreationFailure, "creating topic (%s) library error", topic.GetName())
	}

	if len(resp.Topics) == 0 {
		return r.recordErrorEvent(ErrEmptyTopicConfigDescription, topic, v1alpha1.EventTopicCreationFailure, "creating topic (%s) return empty response", topic.GetName())
	}

	err = kerr.ErrorForCode(resp.Topics[0].ErrorCode)
	if err != nil && !errors.Is(err, kerr.TopicAlreadyExists) {
		errMsg := NoneConstantString
		if resp.Topics[0].ErrorMessage != nil {
			errMsg = *resp.Topics[0].ErrorMessage
		}
		return r.recordErrorEvent(err, topic, v1alpha1.EventTopicCreationFailure, "creating topic (%s) CreateTopicsResponse error (%s)", topic.GetName(), errMsg)
	}

	if resp.Topics[0].Topic != topic.GetName() {
		return r.recordErrorEvent(ErrWrongCreateTopicResponse, topic, v1alpha1.EventTopicCreationFailure, "creating topic (%s) response does not match requested topic", topic.GetName())
	}

	if errors.Is(err, kerr.TopicAlreadyExists) {
		return err
	}
	return nil
}

func (r *TopicReconciler) deleteTopic(ctx context.Context, topic *v1alpha1.Topic, kafkaClient *kgo.Client) error {
	req := kmsg.NewDeleteTopicsRequest()
	req.TopicNames = []string{topic.GetName()}
	rt := kmsg.NewDeleteTopicsRequestTopic()
	rt.Topic = kmsg.StringPtr(topic.GetName())
	req.Topics = append(req.Topics, rt)
	resp, err := req.RequestWith(ctx, kafkaClient)
	if err != nil {
		return r.recordErrorEvent(err, topic, v1alpha1.EventTopicDeletionFailure, "deleting topic (%s) library error", topic.GetName())
	}

	if len(resp.Topics) == 0 {
		return r.recordErrorEvent(ErrEmptyTopicConfigDescription, topic, v1alpha1.EventTopicDeletionFailure, "deleting topic (%s) return empty response", topic.GetName())
	}

	if err = kerr.ErrorForCode(resp.Topics[0].ErrorCode); err != nil && !errors.Is(err, kerr.UnknownTopicOrPartition) {
		errMsg := NoneConstantString
		if resp.Topics[0].ErrorMessage != nil {
			errMsg = *resp.Topics[0].ErrorMessage
		}
		return r.recordErrorEvent(err, topic, v1alpha1.EventTopicDeletionFailure, "deleting topic (%s) library error (%s)", topic.GetName(), errMsg)
	}

	if resp.Topics[0].Topic == nil || *resp.Topics[0].Topic != topic.GetName() {
		return r.recordErrorEvent(ErrWrongCreateTopicResponse, topic, v1alpha1.EventTopicDeletionFailure, "deleting topic (%s) response does not match requested topic", topic.GetName())
	}

	return nil
}

func (r *TopicReconciler) patchTopicStatus(ctx context.Context, topic *v1alpha1.Topic, l logr.Logger) error {
	key := client.ObjectKeyFromObject(topic)
	latest := &v1alpha1.Topic{}
	err := r.Client.Get(ctx, key, latest)
	if client.IgnoreNotFound(err) != nil {
		return fmt.Errorf("retrieve current topic resource for update statue: %w", err)
	}
	if apierrors.IsNotFound(err) {
		return nil
	}

	patch := client.MergeFrom(latest)
	b, err := patch.Data(topic)
	if err == nil && len(b) != 0 {
		l.V(TraceLevel).Info("patch topic status", "patch-type", patch.Type(), "patch-body", string(b))
	}

	return r.Client.Status().Patch(ctx, topic, patch)
}

func generateConf(
	describedConfig []kmsg.DescribeConfigsResponseResourceConfig,
	topicSpecSingleValue map[string]*string,
	desiredReplicationFactor,
	actualReplicationFactor int16,
) (setConf, specialSetConf map[string]string, deleteConf map[string]any) {
	deleteConf = make(map[string]any)
	setConf = make(map[string]string)
	specialSetConf = make(map[string]string)

	for _, conf := range describedConfig {
		if conf.Source != kmsg.ConfigSourceDefaultConfig && conf.Value != nil && conf.Name != "cleanup.policy" {
			deleteConf[conf.Name] = nil
		}
	}

	remoteRead := false
	remoteWrite := false

	for k, v := range topicSpecSingleValue {
		switch k {
		case "redpanda.remote.read":
			remoteRead = true
		case "redpanda.remote.write":
			remoteWrite = true
		}
		_, exists := deleteConf[k]
		if exists {
			delete(deleteConf, k)
		}
		if v != nil {
			setConf[k] = *v
		}
	}
	if remoteWrite && remoteRead {
		delete(setConf, "redpanda.remote.write")
		specialSetConf["redpanda.remote.write"] = *topicSpecSingleValue["redpanda.remote.write"]
	}

	if desiredReplicationFactor != actualReplicationFactor {
		setConf["replication.factor"] = strconv.Itoa(int(desiredReplicationFactor))
	}

	return setConf, specialSetConf, deleteConf
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

func (r *TopicReconciler) recordErrorEvent(err error, topic *v1alpha1.Topic, eventType, message string, args ...any) error {
	if r.EventRecorder != nil {
		var eventArgs []any
		copy(args, eventArgs)
		eventArgs = append(eventArgs, err.Error())
		r.EventRecorder.AnnotatedEventf(topic,
			map[string]string{v2.GroupVersion.Group + "/revision": topic.ResourceVersion},
			corev1.EventTypeWarning, eventType, fmt.Sprintf(message+": %s", eventArgs))
	}
	args = append(args, err)
	return fmt.Errorf(message+": %w", args...) // nolint:goerr113 // That is not dynamic error
}
