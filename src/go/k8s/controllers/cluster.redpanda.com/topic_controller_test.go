package clusterredpandacom_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/pointer"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/redpanda-data/redpanda/src/go/k8s/apis/cluster.redpanda.com/v1alpha1"
	clusterredpandacom "github.com/redpanda-data/redpanda/src/go/k8s/controllers/cluster.redpanda.com"
)

func TestReconcile(t *testing.T) { // nolint:funlen // These tests have clear subtests.
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*2)
	defer cancel()

	var kafkaAdmCl *kadm.Client
	var kafkaCl *kgo.Client
	var seedBroker string
	var c client.WithWatch

	defer os.Unsetenv("TESTCONTAINERS_RYUK_DISABLED")

	testNamespace := "test-namespace"
	{
		if os.Getenv("CI") == "true" {
			err := os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")
			require.NoError(t, err)
		}
		container, err := redpanda.RunContainer(ctx, testcontainers.WithImage("docker.redpanda.com/redpandadata/redpanda:v23.2.8"))
		require.NoError(t, err)

		t.Cleanup(func() {
			ctxCleanup, cancelCleanup := context.WithTimeout(context.Background(), time.Minute*2)
			defer cancelCleanup()
			if err = container.Terminate(ctxCleanup); err != nil {
				t.Fatalf("failed to terminate container: %s", err)
			}
		})

		seedBroker, err = container.KafkaSeedBroker(ctx)
		require.NoError(t, err)

		kafkaCl, err = kgo.NewClient(
			kgo.SeedBrokers(seedBroker),
		)
		require.NoError(t, err)
		defer kafkaCl.Close()

		kafkaAdmCl = kadm.NewClient(kafkaCl)

		c = fake.NewClientBuilder().Build()
		err = v1alpha1.AddToScheme(scheme.Scheme)
		require.NoError(t, err)
	}

	tr := clusterredpandacom.TopicReconciler{
		Client: c,
		Scheme: scheme.Scheme,
	}

	t.Run("create_topic", func(t *testing.T) {
		topicName := "create-test-topic"

		createTopic := v1alpha1.Topic{
			ObjectMeta: metav1.ObjectMeta{
				Name:      topicName,
				Namespace: testNamespace,
			},
			Spec: v1alpha1.TopicSpec{
				Partitions:        pointer.Int(3),
				ReplicationFactor: pointer.Int(1),
				AdditionalConfig:  nil,
				KafkaAPISpec: &v1alpha1.KafkaAPISpec{
					Brokers: []string{seedBroker},
				},
				SynchronizationInterval: &metav1.Duration{Duration: time.Second * 5},
			},
		}

		err := c.Create(ctx, &createTopic)
		require.NoError(t, err)

		req := ctrl.Request{
			NamespacedName: types.NamespacedName{
				Name:      topicName,
				Namespace: testNamespace,
			},
		}
		result, err := tr.Reconcile(ctx, req)
		assert.NoError(t, err)

		assert.False(t, result.Requeue)
		assert.Equal(t, time.Second*5, result.RequeueAfter)

		var mrt kmsg.MetadataResponseTopic
		{
			metaReq := kmsg.NewPtrMetadataRequest()
			reqTopic := kmsg.NewMetadataRequestTopic()
			reqTopic.Topic = kmsg.StringPtr(topicName)
			metaReq.Topics = append(metaReq.Topics, reqTopic)
			resp, errMetadata := metaReq.RequestWith(context.Background(), kafkaCl)
			require.NoError(t, errMetadata)

			mrt = resp.Topics[0]
		}

		assert.Equal(t, *createTopic.Spec.ReplicationFactor, len(mrt.Partitions[0].Replicas))
		assert.Equal(t, *createTopic.Spec.Partitions, len(mrt.Partitions))

		err = c.Get(ctx, types.NamespacedName{
			Name:      topicName,
			Namespace: testNamespace,
		}, &createTopic)
		require.NoError(t, err)

		assert.Equal(t, "operator.redpanda.com/finalizer", createTopic.ObjectMeta.Finalizers[0])
		assert.NotEmpty(t, createTopic.Status.Conditions)
		cond := createTopic.Status.Conditions[0]
		assert.Equal(t, v1alpha1.ReadyCondition, cond.Type)
		assert.Equal(t, metav1.ConditionTrue, cond.Status)
		assert.Equal(t, v1alpha1.SucceededReason, cond.Reason)
		assert.NotEqual(t, 0, len(createTopic.Status.TopicConfiguration))
	})
	t.Run("create_topic_that_already_exist", func(t *testing.T) {
		topicName := "create-already-existent-test-topic"

		_, err := kafkaAdmCl.CreateTopic(ctx, -1, -1, nil, topicName)
		require.NoError(t, err)

		createTopic := v1alpha1.Topic{
			ObjectMeta: metav1.ObjectMeta{
				Name:      topicName,
				Namespace: testNamespace,
			},
			Spec: v1alpha1.TopicSpec{
				Partitions:        pointer.Int(1),
				ReplicationFactor: pointer.Int(1),
				AdditionalConfig:  nil,
				KafkaAPISpec: &v1alpha1.KafkaAPISpec{
					Brokers: []string{seedBroker},
				},
			},
		}

		err = c.Create(ctx, &createTopic)
		require.NoError(t, err)

		req := ctrl.Request{
			NamespacedName: types.NamespacedName{
				Name:      topicName,
				Namespace: testNamespace,
			},
		}
		result, err := tr.Reconcile(ctx, req)
		assert.NoError(t, err)

		assert.False(t, result.Requeue)
		assert.Equal(t, time.Second*3, result.RequeueAfter)

		var mrt kmsg.MetadataResponseTopic
		{
			metaReq := kmsg.NewPtrMetadataRequest()
			reqTopic := kmsg.NewMetadataRequestTopic()
			reqTopic.Topic = kmsg.StringPtr(topicName)
			metaReq.Topics = append(metaReq.Topics, reqTopic)
			resp, errMetadata := metaReq.RequestWith(context.Background(), kafkaCl)
			require.NoError(t, errMetadata)

			mrt = resp.Topics[0]
		}

		assert.Equal(t, *createTopic.Spec.ReplicationFactor, len(mrt.Partitions[0].Replicas))
		assert.Equal(t, *createTopic.Spec.Partitions, len(mrt.Partitions))

		err = c.Get(ctx, types.NamespacedName{
			Name:      topicName,
			Namespace: testNamespace,
		}, &createTopic)
		require.NoError(t, err)

		assert.Equal(t, "operator.redpanda.com/finalizer", createTopic.ObjectMeta.Finalizers[0])
		assert.NotEmpty(t, createTopic.Status.Conditions)
		cond := createTopic.Status.Conditions[0]
		assert.Equal(t, v1alpha1.ReadyCondition, cond.Type)
		assert.Equal(t, metav1.ConditionTrue, cond.Status)
		assert.Equal(t, v1alpha1.SucceededReason, cond.Reason)
		assert.NotEqual(t, 0, len(createTopic.Status.TopicConfiguration))
	})
	t.Run("add_partition", func(t *testing.T) {
		topicName := "partition-count-change"

		// given topic custom resource
		partitionCountChange := v1alpha1.Topic{
			ObjectMeta: metav1.ObjectMeta{
				Name:      topicName,
				Namespace: testNamespace,
			},
			Spec: v1alpha1.TopicSpec{
				Partitions:        pointer.Int(3),
				ReplicationFactor: pointer.Int(1),
				AdditionalConfig: map[string]*string{
					"segment.bytes": pointer.String("7654321"),
				},
				KafkaAPISpec: &v1alpha1.KafkaAPISpec{
					Brokers: []string{seedBroker},
				},
			},
		}
		err := c.Create(ctx, &partitionCountChange)
		require.NoError(t, err)

		// when topic custom resource reconciled
		req := ctrl.Request{
			NamespacedName: types.NamespacedName{
				Name:      topicName,
				Namespace: testNamespace,
			},
		}
		_, err = tr.Reconcile(ctx, req)
		assert.NoError(t, err)

		err = c.Get(ctx, types.NamespacedName{
			Name:      topicName,
			Namespace: testNamespace,
		}, &partitionCountChange)
		require.NoError(t, err)

		partitionCountChange.Spec.Partitions = pointer.Int(6)

		err = c.Update(ctx, &partitionCountChange)
		require.NoError(t, err)

		// and reconcile
		_, err = tr.Reconcile(ctx, req)
		assert.NoError(t, err)

		// then check partition count
		reqMeta := kmsg.NewPtrMetadataRequest()
		reqTopic := kmsg.NewMetadataRequestTopic()
		reqTopic.Topic = kmsg.StringPtr(topicName)
		reqMeta.Topics = append(reqMeta.Topics, reqTopic)

		resp, err := reqMeta.RequestWith(ctx, kafkaCl)
		assert.NoError(t, err)
		assert.Equal(t, *partitionCountChange.Spec.Partitions, len(resp.Topics[0].Partitions))

		err = c.Get(ctx, types.NamespacedName{
			Name:      topicName,
			Namespace: testNamespace,
		}, &partitionCountChange)
		require.NoError(t, err)

		assert.NotEmpty(t, partitionCountChange.Status.Conditions)
		cond := partitionCountChange.Status.Conditions[0]
		assert.Equal(t, v1alpha1.ReadyCondition, cond.Type)
		assert.Equal(t, metav1.ConditionTrue, cond.Status)
		assert.Equal(t, v1alpha1.SucceededReason, cond.Reason)
		assert.NotEqual(t, 0, len(partitionCountChange.Status.TopicConfiguration))
	})
	t.Run("unable_to_remove_partition", func(t *testing.T) {
		topicName := "scale-down-partition-count"

		// given topic custom resource
		partitionCountChange := v1alpha1.Topic{
			ObjectMeta: metav1.ObjectMeta{
				Name:      topicName,
				Namespace: testNamespace,
			},
			Spec: v1alpha1.TopicSpec{
				Partitions:        pointer.Int(6),
				ReplicationFactor: pointer.Int(1),
				AdditionalConfig: map[string]*string{
					"segment.bytes": pointer.String("7654321"),
				},
				KafkaAPISpec: &v1alpha1.KafkaAPISpec{
					Brokers: []string{seedBroker},
				},
			},
		}
		err := c.Create(ctx, &partitionCountChange)
		require.NoError(t, err)

		// when topic custom resource reconciled
		req := ctrl.Request{
			NamespacedName: types.NamespacedName{
				Name:      topicName,
				Namespace: testNamespace,
			},
		}
		_, err = tr.Reconcile(ctx, req)
		assert.NoError(t, err)

		err = c.Get(ctx, types.NamespacedName{
			Name:      topicName,
			Namespace: testNamespace,
		}, &partitionCountChange)
		require.NoError(t, err)

		partitionCountChange.Spec.Partitions = pointer.Int(3)

		err = c.Update(ctx, &partitionCountChange)
		require.NoError(t, err)

		// and reconcile
		_, err = tr.Reconcile(ctx, req)
		assert.Error(t, err)

		err = c.Get(ctx, types.NamespacedName{
			Name:      topicName,
			Namespace: testNamespace,
		}, &partitionCountChange)
		require.NoError(t, err)

		assert.NotEmpty(t, partitionCountChange.Status.Conditions)
		cond := partitionCountChange.Status.Conditions[0]
		assert.Equal(t, v1alpha1.ReadyCondition, cond.Type)
		assert.Equal(t, metav1.ConditionFalse, cond.Status)
		assert.Equal(t, v1alpha1.FailedReason, cond.Reason)
		assert.NotEqual(t, 0, len(partitionCountChange.Status.TopicConfiguration))
	})
	t.Run("unable_to_increase_replication_factor", func(t *testing.T) {
		topicName := "change-replication-factor"

		// given topic custom resource
		replicationFactorChange := v1alpha1.Topic{
			ObjectMeta: metav1.ObjectMeta{
				Name:      topicName,
				Namespace: testNamespace,
			},
			Spec: v1alpha1.TopicSpec{
				Partitions:        pointer.Int(6),
				ReplicationFactor: pointer.Int(1),
				KafkaAPISpec: &v1alpha1.KafkaAPISpec{
					Brokers: []string{seedBroker},
				},
			},
		}
		err := c.Create(ctx, &replicationFactorChange)
		require.NoError(t, err)

		// when topic custom resource reconciled
		req := ctrl.Request{
			NamespacedName: types.NamespacedName{
				Name:      topicName,
				Namespace: testNamespace,
			},
		}
		_, err = tr.Reconcile(ctx, req)
		assert.NoError(t, err)

		err = c.Get(ctx, types.NamespacedName{
			Name:      topicName,
			Namespace: testNamespace,
		}, &replicationFactorChange)
		require.NoError(t, err)

		replicationFactorChange.Spec.ReplicationFactor = pointer.Int(3)

		err = c.Update(ctx, &replicationFactorChange)
		require.NoError(t, err)

		// and reconcile
		_, err = tr.Reconcile(ctx, req)
		assert.Error(t, err)

		err = c.Get(ctx, types.NamespacedName{
			Name:      topicName,
			Namespace: testNamespace,
		}, &replicationFactorChange)
		require.NoError(t, err)

		assert.NotEmpty(t, replicationFactorChange.Status.Conditions)
		cond := replicationFactorChange.Status.Conditions[0]
		assert.Equal(t, v1alpha1.ReadyCondition, cond.Type)
		assert.Equal(t, metav1.ConditionFalse, cond.Status)
		assert.Equal(t, v1alpha1.FailedReason, cond.Reason)
		assert.NotEqual(t, 0, len(replicationFactorChange.Status.TopicConfiguration))
	})
	t.Run("delete_a_key`s_config_value", func(t *testing.T) {
		removeTopicPropertyName := "remote-topic-property"
		testPropertyKey := "max.message.bytes"
		testPropertyValue := "87678987"

		// given topic custom resource
		removeProperty := v1alpha1.Topic{
			ObjectMeta: metav1.ObjectMeta{
				Name:      removeTopicPropertyName,
				Namespace: testNamespace,
			},
			Spec: v1alpha1.TopicSpec{
				Partitions:        pointer.Int(3),
				ReplicationFactor: pointer.Int(1),
				AdditionalConfig: map[string]*string{
					testPropertyKey: pointer.String(testPropertyValue),
					"segment.bytes": pointer.String("7654321"),
				},
				KafkaAPISpec: &v1alpha1.KafkaAPISpec{
					Brokers: []string{seedBroker},
				},
			},
		}
		err := c.Create(ctx, &removeProperty)
		require.NoError(t, err)

		// when topic custom resource reconciled
		req := ctrl.Request{
			NamespacedName: types.NamespacedName{
				Name:      removeTopicPropertyName,
				Namespace: testNamespace,
			},
		}
		_, err = tr.Reconcile(ctx, req)
		assert.NoError(t, err)

		err = c.Get(ctx, types.NamespacedName{
			Name:      removeTopicPropertyName,
			Namespace: testNamespace,
		}, &removeProperty)
		require.NoError(t, err)

		// then remove tiered storage property
		delete(removeProperty.Spec.AdditionalConfig, testPropertyKey)

		err = c.Update(ctx, &removeProperty)
		require.NoError(t, err)

		// and reconcile
		_, err = tr.Reconcile(ctx, req)
		assert.NoError(t, err)

		// then confirm there is no that property
		rc, err := kafkaAdmCl.DescribeTopicConfigs(ctx, removeTopicPropertyName)
		require.NoError(t, err)

		config, err := rc.On(removeTopicPropertyName, nil)
		require.NoError(t, err)
		for _, conf := range config.Configs {
			if conf.Key == testPropertyKey {
				assert.NotEqual(t, testPropertyValue, *conf.Value)
			}
			value, exist := removeProperty.Spec.AdditionalConfig[conf.Key]
			if !exist {
				continue
			}

			require.NotNil(t, conf.Value, "topic configuration should not be empty", "key", conf.Key)
			assert.Equal(t, *value, *conf.Value, "topic configuration mismatch", "key", conf.Key)
			delete(removeProperty.Spec.AdditionalConfig, conf.Key)
		}
		assert.Len(t, removeProperty.Spec.AdditionalConfig, 0)

		err = c.Get(ctx, types.NamespacedName{
			Name:      removeTopicPropertyName,
			Namespace: testNamespace,
		}, &removeProperty)
		require.NoError(t, err)

		assert.NotEmpty(t, removeProperty.Status.Conditions)
		cond := removeProperty.Status.Conditions[0]
		assert.Equal(t, v1alpha1.ReadyCondition, cond.Type)
		assert.Equal(t, metav1.ConditionTrue, cond.Status)
		assert.Equal(t, v1alpha1.SucceededReason, cond.Reason)
		assert.NotEqual(t, 0, len(removeProperty.Status.TopicConfiguration))
	})
	t.Run("both_tiered_storage_property", func(t *testing.T) {
		// Redpanda fails to set both remote.read and write when passed
		// at the same time, so we issue first the set request for write,
		// then the rest of the requests.
		// See https://github.com/redpanda-data/redpanda/issues/9191 and
		// https://github.com/redpanda-data/redpanda/issues/4499
		twoTieredStorageConfTopicName := "both-tiered-storage-conf" // nolint:gosec // this is not credentials

		tieredStorageConf := v1alpha1.Topic{
			ObjectMeta: metav1.ObjectMeta{
				Name:      twoTieredStorageConfTopicName,
				Namespace: testNamespace,
			},
			Spec: v1alpha1.TopicSpec{
				Partitions:        pointer.Int(3),
				ReplicationFactor: pointer.Int(1),
				AdditionalConfig: map[string]*string{
					"redpanda.remote.read":  pointer.String("true"),
					"redpanda.remote.write": pointer.String("true"),
					"segment.bytes":         pointer.String("7654321"),
				},
				KafkaAPISpec: &v1alpha1.KafkaAPISpec{
					Brokers: []string{seedBroker},
				},
			},
		}

		err := c.Create(ctx, &tieredStorageConf)
		require.NoError(t, err)

		req := ctrl.Request{
			NamespacedName: types.NamespacedName{
				Name:      twoTieredStorageConfTopicName,
				Namespace: testNamespace,
			},
		}
		result, err := tr.Reconcile(ctx, req)
		assert.NoError(t, err)

		assert.False(t, result.Requeue)

		rc, err := kafkaAdmCl.DescribeTopicConfigs(ctx, twoTieredStorageConfTopicName)
		require.NoError(t, err)

		config, err := rc.On(twoTieredStorageConfTopicName, nil)
		require.NoError(t, err)
		for _, conf := range config.Configs {
			value, exist := tieredStorageConf.Spec.AdditionalConfig[conf.Key]
			if !exist {
				continue
			}
			require.NotNil(t, conf.Value, "topic configuration should not be empty", "key", conf.Key)
			assert.Equal(t, *value, *conf.Value, "topic configuration mismatch", "key", conf.Key)
			delete(tieredStorageConf.Spec.AdditionalConfig, conf.Key)
		}
		assert.Len(t, tieredStorageConf.Spec.AdditionalConfig, 0)

		err = c.Get(ctx, types.NamespacedName{
			Name:      twoTieredStorageConfTopicName,
			Namespace: testNamespace,
		}, &tieredStorageConf)
		require.NoError(t, err)

		tieredStorageConf.Spec.AdditionalConfig["redpanda.remote.read"] = pointer.String("false")
		tieredStorageConf.Spec.AdditionalConfig["redpanda.remote.write"] = pointer.String("false")

		err = c.Update(ctx, &tieredStorageConf)
		require.NoError(t, err)

		result, err = tr.Reconcile(ctx, req)
		assert.NoError(t, err)

		assert.False(t, result.Requeue)

		rc, err = kafkaAdmCl.DescribeTopicConfigs(ctx, twoTieredStorageConfTopicName)
		require.NoError(t, err)

		config, err = rc.On(twoTieredStorageConfTopicName, nil)
		require.NoError(t, err)
		for _, conf := range config.Configs {
			value, exist := tieredStorageConf.Spec.AdditionalConfig[conf.Key]
			if !exist {
				continue
			}
			require.NotNil(t, conf.Value, "topic configuration should not be empty", "key", conf.Key)
			assert.Equal(t, *value, *conf.Value, "topic configuration mismatch", "key", conf.Key)
			delete(tieredStorageConf.Spec.AdditionalConfig, conf.Key)
		}
		assert.Len(t, tieredStorageConf.Spec.AdditionalConfig, 0)

		err = c.Get(ctx, types.NamespacedName{
			Name:      twoTieredStorageConfTopicName,
			Namespace: testNamespace,
		}, &tieredStorageConf)
		require.NoError(t, err)

		assert.NotEmpty(t, tieredStorageConf.Status.Conditions)
		cond := tieredStorageConf.Status.Conditions[0]
		assert.Equal(t, v1alpha1.ReadyCondition, cond.Type)
		assert.Equal(t, metav1.ConditionTrue, cond.Status)
		assert.Equal(t, v1alpha1.SucceededReason, cond.Reason)
		assert.NotEqual(t, 0, len(tieredStorageConf.Status.TopicConfiguration))
	})
	t.Run("check_status_after_reconciliation", func(t *testing.T) {
		topicName := "topic-cr-status"

		topicStatus := v1alpha1.Topic{
			ObjectMeta: metav1.ObjectMeta{
				Name:       topicName,
				Namespace:  testNamespace,
				Generation: 1,
			},
			Spec: v1alpha1.TopicSpec{
				Partitions:        pointer.Int(3),
				ReplicationFactor: pointer.Int(1),
				AdditionalConfig:  nil,
				KafkaAPISpec: &v1alpha1.KafkaAPISpec{
					Brokers: []string{seedBroker},
				},
			},
		}

		err := c.Create(ctx, &topicStatus)
		require.NoError(t, err)

		req := ctrl.Request{
			NamespacedName: types.NamespacedName{
				Name:      topicName,
				Namespace: testNamespace,
			},
		}
		result, err := tr.Reconcile(ctx, req)
		assert.NoError(t, err)

		assert.False(t, result.Requeue)
		assert.Equal(t, time.Second*3, result.RequeueAfter)

		err = c.Get(ctx, types.NamespacedName{
			Name:      topicName,
			Namespace: testNamespace,
		}, &topicStatus)
		assert.NoError(t, err)

		assert.Equal(t, topicStatus.Generation, topicStatus.Status.ObservedGeneration)
		cond := topicStatus.Status.Conditions[0]
		assert.Equal(t, v1alpha1.ReadyCondition, cond.Type)
		assert.Equal(t, metav1.ConditionTrue, cond.Status)
		assert.Equal(t, v1alpha1.SucceededReason, cond.Reason)
		assert.Equal(t, topicStatus.Generation, cond.ObservedGeneration)
		assert.NotEqual(t, 0, len(topicStatus.Status.TopicConfiguration))
	})
	t.Run("update_topic_configuration", func(t *testing.T) {
		updateTopicName := "update-topic-config"

		_, err := kafkaAdmCl.CreateTopic(ctx, -1, -1, nil, updateTopicName)
		require.NoError(t, err)

		updateTopic := v1alpha1.Topic{
			ObjectMeta: metav1.ObjectMeta{
				Name:      updateTopicName,
				Namespace: testNamespace,
			},
			Spec: v1alpha1.TopicSpec{
				Partitions:        pointer.Int(3),
				ReplicationFactor: pointer.Int(1),
				AdditionalConfig: map[string]*string{
					"redpanda.remote.read": pointer.String("true"),
					"segment.bytes":        pointer.String("7654321"),
				},
				KafkaAPISpec: &v1alpha1.KafkaAPISpec{
					Brokers: []string{seedBroker},
				},
			},
		}

		err = c.Create(ctx, &updateTopic)
		require.NoError(t, err)

		req := ctrl.Request{
			NamespacedName: types.NamespacedName{
				Name:      updateTopicName,
				Namespace: testNamespace,
			},
		}
		result, err := tr.Reconcile(ctx, req)
		assert.NoError(t, err)

		assert.False(t, result.Requeue)

		rc, err := kafkaAdmCl.DescribeTopicConfigs(ctx, updateTopicName)
		require.NoError(t, err)

		topic, err := rc.On(updateTopicName, nil)
		require.NoError(t, err)
		for _, conf := range topic.Configs {
			value, exist := updateTopic.Spec.AdditionalConfig[conf.Key]
			if !exist {
				continue
			}
			require.NotNil(t, conf.Value, "topic configuration should not be empty", "key", conf.Key)
			assert.Equal(t, *value, *conf.Value, "topic configuration mismatch", "key", conf.Key)
			delete(updateTopic.Spec.AdditionalConfig, conf.Key)
		}
		assert.Len(t, updateTopic.Spec.AdditionalConfig, 0)

		err = c.Get(ctx, types.NamespacedName{
			Name:      updateTopicName,
			Namespace: testNamespace,
		}, &updateTopic)
		assert.NoError(t, err)

		assert.NotEmpty(t, updateTopic.Status.Conditions)
		cond := updateTopic.Status.Conditions[0]
		assert.Equal(t, v1alpha1.ReadyCondition, cond.Type)
		assert.Equal(t, metav1.ConditionTrue, cond.Status)
		assert.Equal(t, v1alpha1.SucceededReason, cond.Reason)
		assert.NotEqual(t, 0, len(updateTopic.Status.TopicConfiguration))
	})
	t.Run("ignore_not_found", func(t *testing.T) {
		topicName := "ignore-not-found"

		req := ctrl.Request{
			NamespacedName: types.NamespacedName{
				Name:      topicName,
				Namespace: testNamespace,
			},
		}
		result, err := tr.Reconcile(ctx, req)
		assert.NoError(t, err)

		assert.False(t, result.Requeue)
		assert.Equal(t, time.Duration(0), result.RequeueAfter)
	})
	t.Run("empty_kafka_api_spec", func(t *testing.T) {
		topicName := "test-empty-kafka-api-spec"
		testTopic := v1alpha1.Topic{
			ObjectMeta: metav1.ObjectMeta{
				Name:      topicName,
				Namespace: testNamespace,
			},
		}

		err := c.Create(ctx, &testTopic)
		require.NoError(t, err)

		req := ctrl.Request{
			NamespacedName: types.NamespacedName{
				Name:      topicName,
				Namespace: testNamespace,
			},
		}
		_, err = tr.Reconcile(ctx, req)
		assert.Error(t, err)
	})
	t.Run("delete_existent_topic_k8s_meta_deletion_timestamp", func(t *testing.T) {
		deleteTopicName := "delete-test-topic"

		_, err := kafkaAdmCl.CreateTopic(ctx, -1, -1, nil, deleteTopicName)
		require.NoError(t, err)

		deleteTopic := v1alpha1.Topic{
			ObjectMeta: metav1.ObjectMeta{
				Name:              deleteTopicName,
				Namespace:         testNamespace,
				DeletionTimestamp: &metav1.Time{Time: time.Now()},
				Finalizers:        []string{clusterredpandacom.FinalizerKey},
			},
			Spec: v1alpha1.TopicSpec{
				KafkaAPISpec: &v1alpha1.KafkaAPISpec{
					Brokers: []string{seedBroker},
				},
			},
		}

		err = c.Create(ctx, &deleteTopic)
		require.NoError(t, err)

		req := ctrl.Request{
			NamespacedName: types.NamespacedName{
				Name:      deleteTopicName,
				Namespace: testNamespace,
			},
		}
		result, err := tr.Reconcile(ctx, req)
		assert.NoError(t, err)

		assert.False(t, result.Requeue)
		assert.Equal(t, time.Duration(0), result.RequeueAfter)

		td, err := kafkaAdmCl.ListTopics(ctx, deleteTopicName)
		assert.NoError(t, err)

		assert.False(t, td.Has(deleteTopicName))

		err = c.Get(ctx, types.NamespacedName{
			Name:      deleteTopicName,
			Namespace: testNamespace,
		}, &deleteTopic)
		assert.True(t, apierrors.IsNotFound(err))
	})
	t.Run("delete_none_existent_topic_k8s_meta_deletion_timestamp", func(t *testing.T) {
		deleteNoneExistentTopicName := "delete-none-existent-test-topic"

		noneExistentTestTopic := v1alpha1.Topic{
			ObjectMeta: metav1.ObjectMeta{
				Name:              deleteNoneExistentTopicName,
				Namespace:         testNamespace,
				DeletionTimestamp: &metav1.Time{Time: time.Now()},
				Finalizers:        []string{clusterredpandacom.FinalizerKey},
			},

			Spec: v1alpha1.TopicSpec{
				KafkaAPISpec: &v1alpha1.KafkaAPISpec{
					Brokers: []string{seedBroker},
				},
			},
		}

		err := c.Create(ctx, &noneExistentTestTopic)
		require.NoError(t, err)

		req := ctrl.Request{
			NamespacedName: types.NamespacedName{
				Name:      deleteNoneExistentTopicName,
				Namespace: testNamespace,
			},
		}
		result, err := tr.Reconcile(ctx, req)
		assert.NoError(t, err)

		assert.False(t, result.Requeue)
		assert.Equal(t, time.Duration(0), result.RequeueAfter)
	})
}
