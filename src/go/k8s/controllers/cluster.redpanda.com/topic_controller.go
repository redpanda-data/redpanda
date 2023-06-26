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

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"k8s.io/apimachinery/pkg/runtime"
	kuberecorder "k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
	v2 "sigs.k8s.io/controller-runtime/pkg/webhook/conversion/testdata/api/v2"

	"github.com/redpanda-data/redpanda/src/go/k8s/apis/cluster.redpanda.com/v1alpha1"
)

const FinalizerKey = "operator.redpanda.com/finalizer"

var (
	ErrEmptyKafkaAPISpec       = errors.New("empty kafka api spec")
	ErrScaleDownPartitionCount = errors.New("unable to scale down number of partition in topic")
)

// TopicReconciler reconciles a Topic object
type TopicReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	kuberecorder.EventRecorder
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
		return ctrl.Result{}, ErrEmptyKafkaAPISpec
	}

	kgoOpts, err := newKgoConfig(ctx, topic)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("creating kgo configuration options: %w", err)
	}

	kafkaClient, err := kgo.NewClient(kgoOpts...)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("creating franz-go kafka client: %w", err)
	}
	defer kafkaClient.Close()

	kadmClient := kadm.NewClient(kafkaClient)

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
		err = r.deleteTopic(ctx, topic, kadmClient)
		return ctrl.Result{}, err
	}

	err = r.createTopic(ctx, topic, kadmClient, partition, replicationFactor)
	if err != nil && !errors.Is(err, kerr.TopicAlreadyExists) {
		return ctrl.Result{}, err
	}

	// Topic created
	if err == nil {
		return ctrl.Result{}, nil
	}

	err = r.reconcilePartition(ctx, topic, kafkaClient)
	if err != nil {
		return ctrl.Result{}, err
	}

	resp, err := r.describeTopic(ctx, topic, kafkaClient)
	if err != nil {
		return ctrl.Result{}, err
	}

	setConf, specialWriteConf, deleteConf := generateConf(resp.Resources[0].Configs, topic.Spec.AdditionalConfigSingleValue)
	// Redpanda fails to set both remote.read and remote.write when passed
	// at the same time, so we issue first the set request for write,
	// then the rest of the requests.
	// See https://github.com/redpanda-data/redpanda/issues/9191 and
	// https://github.com/redpanda-data/redpanda/issues/4499
	if len(specialWriteConf) > 0 {
		err = r.alterTopicConfiguration(ctx, topic, specialWriteConf, deleteConf, kafkaClient)
		if err != nil {
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{}, r.alterTopicConfiguration(ctx, topic, setConf, deleteConf, kafkaClient)
}

func (r *TopicReconciler) reconcilePartition(ctx context.Context, topic *v1alpha1.Topic, cl *kgo.Client) error {
	reqMetadata := kmsg.NewPtrMetadataRequest()
	reqTopic := kmsg.NewMetadataRequestTopic()
	reqTopic.Topic = kmsg.StringPtr(topic.GetName())
	reqMetadata.Topics = append(reqMetadata.Topics, reqTopic)

	respMetadata, err := reqMetadata.RequestWith(ctx, cl)
	if err != nil {
		if r.EventRecorder != nil {
			r.EventRecorder.AnnotatedEventf(topic,
				map[string]string{v2.GroupVersion.Group + "/revision": topic.ResourceVersion},
				v1alpha1.EventTopicConfigurationDescribeFailure, "Warning", "failed topic (%s) metadata retrieval library error: %w", topic.GetName(), err)
		}
		return fmt.Errorf("failed topic (%s) metadata retrieval library error: %w", topic.GetName(), err)
	}

	if len(respMetadata.Topics[0].Partitions) > *topic.Spec.Partitions {
		if r.EventRecorder != nil {
			r.EventRecorder.AnnotatedEventf(topic,
				map[string]string{v2.GroupVersion.Group + "/revision": topic.ResourceVersion},
				v1alpha1.EventTopicConfigurationDescribeFailure, "Error", ErrScaleDownPartitionCount.Error())
		}
		return fmt.Errorf("unable to update topic (%s): %w", topic.GetName(), ErrScaleDownPartitionCount)
	}

	if len(respMetadata.Topics[0].Partitions) == *topic.Spec.Partitions {
		return nil
	}

	reqPartition := kmsg.NewCreatePartitionsRequest()
	rt := kmsg.NewCreatePartitionsRequestTopic()
	rt.Topic = topic.GetName()
	rt.Count = int32(*topic.Spec.Partitions)
	reqPartition.Topics = append(reqPartition.Topics, rt)

	respPartition, err := reqPartition.RequestWith(ctx, cl)
	if err != nil {
		if r.EventRecorder != nil {
			r.EventRecorder.AnnotatedEventf(topic,
				map[string]string{v2.GroupVersion.Group + "/revision": topic.ResourceVersion},
				v1alpha1.EventTopicConfigurationDescribeFailure, "Warning", "failed change topic (%s) partition count (%d) library error: %w", topic.GetName(), *topic.Spec.Partitions, err)
		}
		return fmt.Errorf("failed change topic (%s) partition count (%d) library error: %w", topic.GetName(), *topic.Spec.Partitions, err)
	}
	if err = kerr.ErrorForCode(respPartition.Topics[0].ErrorCode); err != nil {
		if r.EventRecorder != nil {
			r.EventRecorder.AnnotatedEventf(topic,
				map[string]string{v2.GroupVersion.Group + "/revision": topic.ResourceVersion},
				v1alpha1.EventTopicConfigurationAlteringFailure, "Warning", "failed change topic (%s) partition count (%d) library error: %w", topic.GetName(), *topic.Spec.Partitions, err)
		}
		return fmt.Errorf("failed change topic (%s) partition count (%d) library error: %w", topic.GetName(), *topic.Spec.Partitions, err)
	}

	return nil
}

func (r *TopicReconciler) alterTopicConfiguration(ctx context.Context, topic *v1alpha1.Topic, setConf map[string]string, deleteConf map[string]any, kafkaClient *kgo.Client) error {
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
				v1alpha1.EventTopicAlreadySynced, "Normal", "configuration not changed")
		}
		return nil
	}

	reqTopic := kmsg.NewIncrementalAlterConfigsRequestResource()
	reqTopic.ResourceType = kmsg.ConfigResourceTypeTopic
	reqTopic.ResourceName = topic.GetName()
	reqTopic.Configs = configs
	reqAltConfig.Resources = append(reqAltConfig.Resources, reqTopic)

	respAltConfig, err := reqAltConfig.RequestWith(ctx, kafkaClient)
	if err != nil {
		if r.EventRecorder != nil {
			r.EventRecorder.AnnotatedEventf(topic,
				map[string]string{v2.GroupVersion.Group + "/revision": topic.ResourceVersion},
				v1alpha1.EventTopicConfigurationAlteringFailure, "Warning", "alter topic configuration (%s) library error: %w", topic.GetName(), err)
		}
		return fmt.Errorf("describing topic configuration (%s) library error: %w", topic.GetName(), err)
	}

	if err = kerr.ErrorForCode(respAltConfig.Resources[0].ErrorCode); err != nil {
		if r.EventRecorder != nil {
			r.EventRecorder.AnnotatedEventf(topic,
				map[string]string{v2.GroupVersion.Group + "/revision": topic.ResourceVersion},
				v1alpha1.EventTopicConfigurationAlteringFailure, "Warning", "alter topic configuration (%s) DescribeConfigsResponse error: %w", topic.GetName(), err)
		}
		return fmt.Errorf("describing topic configuration (%s) DescribeConfigsResponse error: %w", topic.GetName(), err)
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
		if r.EventRecorder != nil {
			r.EventRecorder.AnnotatedEventf(topic,
				map[string]string{v2.GroupVersion.Group + "/revision": topic.ResourceVersion},
				v1alpha1.EventTopicConfigurationDescribeFailure, "Warning", "describing topic configuration (%s) library error: %w", topic.GetName(), err)
		}
		return nil, fmt.Errorf("describing topic configuration (%s) library error: %w", topic.GetName(), err)
	}

	if err = kerr.ErrorForCode(resp.Resources[0].ErrorCode); err != nil {
		if r.EventRecorder != nil {
			r.EventRecorder.AnnotatedEventf(topic,
				map[string]string{v2.GroupVersion.Group + "/revision": topic.ResourceVersion},
				v1alpha1.EventTopicConfigurationDescribeFailure, "Warning", "describing topic configuration (%s) DescribeConfigsResponse error: %w", topic.GetName(), err)
		}
		return nil, fmt.Errorf("describing topic configuration (%s) DescribeConfigsResponse error: %w", topic.GetName(), err)
	}
	return resp, nil
}

func (r *TopicReconciler) createTopic(ctx context.Context, topic *v1alpha1.Topic, kadmClient *kadm.Client, partition int32, replicationFactor int16) error {
	ctr, err := kadmClient.CreateTopic(ctx, partition, replicationFactor, topic.Spec.AdditionalConfigSingleValue, topic.GetName())
	if err != nil && !errors.Is(err, kerr.TopicAlreadyExists) {
		if r.EventRecorder != nil {
			r.EventRecorder.AnnotatedEventf(topic,
				map[string]string{v2.GroupVersion.Group + "/revision": topic.ResourceVersion},
				v1alpha1.EventTopicCreationFailure, "Warning", "creating topic (%s) library error: %w", topic.GetName(), err)
		}
		return fmt.Errorf("creating topic (%s) library error: %w", topic.GetName(), err)
	}

	if ctr.Err != nil && !errors.Is(ctr.Err, kerr.TopicAlreadyExists) {
		if r.EventRecorder != nil {
			r.EventRecorder.AnnotatedEventf(topic,
				map[string]string{v2.GroupVersion.Group + "/revision": topic.ResourceVersion},
				v1alpha1.EventTopicCreationFailure, "Warning", "creating topic (%s) CreateTopicsResponse error: %w", topic.GetName(), err)
		}
		return fmt.Errorf("creating topic (%s) CreateTopicsResponse error: %w", topic.GetName(), ctr.Err)
	}
	return err
}

func (r *TopicReconciler) deleteTopic(ctx context.Context, topic *v1alpha1.Topic, kadmClient *kadm.Client) error {
	dtr, err := kadmClient.DeleteTopics(ctx, topic.GetName())
	if err != nil {
		if r.EventRecorder != nil {
			r.EventRecorder.AnnotatedEventf(topic,
				map[string]string{v2.GroupVersion.Group + "/revision": topic.ResourceVersion},
				v1alpha1.EventTopicDeletionFailure, "Warning", "deleting topic (%s) library error: %w", topic.GetName(), err)
		}
		return fmt.Errorf("deleting topic (%s) library error: %w", topic.GetName(), err)
	}

	resp, err := dtr.On(topic.GetName(), nil)
	if resp.Err != nil && !errors.Is(resp.Err, kerr.UnknownTopicOrPartition) {
		if r.EventRecorder != nil {
			r.EventRecorder.AnnotatedEventf(topic,
				map[string]string{v2.GroupVersion.Group + "/revision": topic.ResourceVersion},
				v1alpha1.EventTopicDeletionFailure, "Warning", "deleting topic (%s) library error: %w", topic.GetName(), resp.Err)
		}
		return fmt.Errorf("deleting topic (%s) library error: %w", topic.GetName(), resp.Err)
	}

	return err
}

func generateConf(describedConfig []kmsg.DescribeConfigsResponseResourceConfig, topicSpecSingleValue map[string]*string) (setConf, specialSetConf map[string]string, deleteConf map[string]any) {
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

	return setConf, specialSetConf, deleteConf
}
