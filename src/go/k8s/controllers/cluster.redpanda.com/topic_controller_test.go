package clusterredpandacom

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/redpanda-data/redpanda/src/go/k8s/apis/cluster.redpanda.com/v1alpha1"
)

func TestReconcile(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*2)
	defer cancel()

	c := fake.NewClientBuilder().Build()
	err := v1alpha1.AddToScheme(scheme.Scheme)
	require.NoError(t, err)

	tr := TopicReconciler{
		Client: c,
		Scheme: scheme.Scheme,
	}

	t.Run("ignore not found", func(t *testing.T) {
		req := ctrl.Request{
			NamespacedName: types.NamespacedName{
				Name:      "test-topic",
				Namespace: "test-namespace",
			},
		}
		result, err := tr.Reconcile(ctx, req)
		assert.NoError(t, err)

		assert.False(t, result.Requeue)
		assert.Equal(t, time.Duration(0), result.RequeueAfter)
	})

	t.Run("empty kafka api spec", func(t *testing.T) {
		testTopic := v1alpha1.Topic{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "empty-spec",
				Namespace: "test-namespace",
			},
		}

		err = c.Create(ctx, &testTopic)
		require.NoError(t, err)

		req := ctrl.Request{
			NamespacedName: types.NamespacedName{
				Name:      "empty-spec",
				Namespace: "test-namespace",
			},
		}
		_, err = tr.Reconcile(ctx, req)
		assert.Error(t, err)
	})

	t.Run("deleted in metadata is not reconciled", func(t *testing.T) {
		testTopic := v1alpha1.Topic{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "delete-test-topic",
				Namespace:         "test-namespace",
				DeletionTimestamp: &metav1.Time{Time: time.Now()},
			},
		}

		err = c.Create(ctx, &testTopic)
		require.NoError(t, err)

		req := ctrl.Request{
			NamespacedName: types.NamespacedName{
				Name:      "test-topic",
				Namespace: "test-namespace",
			},
		}
		result, err := tr.Reconcile(ctx, req)
		assert.NoError(t, err)

		assert.False(t, result.Requeue)
		assert.Equal(t, time.Duration(0), result.RequeueAfter)
	})
}
