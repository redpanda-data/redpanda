package main_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/kudobuilder/kuttl/pkg/apis"
	kuttl "github.com/kudobuilder/kuttl/pkg/test/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vectorizedio/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	"github.com/vectorizedio/redpanda/src/go/k8s/pkg/labels"
	corev1 "k8s.io/api/core/v1"
	apiextv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/utils/pointer"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ensure that we only add to the scheme once.
var schemeLock sync.Once

const (
	kafkaPort		= 9092
	replicas		= 1
	redpandaContainerTag	= "latest"
	redpandaContainerImage	= "vectorized/redpanda"
)

func TestE2E(t *testing.T) {
	kubeconfig, exist := os.LookupEnv("KUBECONFIG")
	if !exist || len(kubeconfig) == 0 {
		t.Fatalf("KUBECONFIG environement variable missing")
	}

	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	require.NoError(t, err)

	c, err := kuttl.NewRetryClient(config, client.Options{
		Scheme: Scheme(),
	})
	require.NoError(t, err)

	ns := corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-namespace",
		},
	}

	err = c.Create(context.TODO(), &ns)
	require.NoError(t, err)

	t.Run("overwrite instance label", func(t *testing.T) {
		// Given 2 clusters.redpanda.vectorized.io that overwrite labels.InstanceKey
		resources := corev1.ResourceList{
			corev1.ResourceCPU:	resource.MustParse("1"),
			corev1.ResourceMemory:	resource.MustParse("100M"),
		}

		key1 := client.ObjectKey{
			Name:		"cluster-sample-1",
			Namespace:	ns.Name,
		}
		key2 := client.ObjectKey{
			Name:		"cluster-sample-2",
			Namespace:	ns.Name,
		}

		redpandaCluster1 := &v1alpha1.Cluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:		key1.Name,
				Namespace:	key1.Namespace,
				Labels: map[string]string{
					labels.InstanceKey: "overwrited-instance-key",
				},
			},
			Spec: v1alpha1.ClusterSpec{
				Image:		redpandaContainerImage,
				Version:	redpandaContainerTag,
				Replicas:	pointer.Int32Ptr(replicas),
				Configuration: v1alpha1.RedpandaConfig{
					KafkaAPI:	v1alpha1.SocketAddress{Port: kafkaPort},
					AdminAPI:	v1alpha1.SocketAddress{Port: kafkaPort + 1},
					RPCServer:	v1alpha1.SocketAddress{Port: kafkaPort + 2},
					DeveloperMode:	true,
				},
				Resources: corev1.ResourceRequirements{
					Limits:		resources,
					Requests:	resources,
				},
			},
		}

		redpandaCluster2 := redpandaCluster1.DeepCopy()
		redpandaCluster2.Name = "cluster-sample-2"

		// When redpanda custom resources are created
		err = c.Create(context.TODO(), redpandaCluster1)
		assert.NoError(t, err)

		err = c.Create(context.TODO(), redpandaCluster2)
		assert.NoError(t, err)

		// Then wait for Clusters to report 1 ready replica
		wait.PollImmediate(500*time.Millisecond, 60*time.Second, func() (done bool, err error) {
			err = c.Get(context.TODO(), key1, redpandaCluster1)
			if k8serrors.IsNotFound(err) {
				return false, nil
			}
			if err != nil {
				return false, err
			}
			if redpandaCluster1.Status.Replicas != 1 {
				return false, nil
			}

			err = c.Get(context.TODO(), key2, redpandaCluster2)
			if k8serrors.IsNotFound(err) {
				return false, nil
			}
			if err != nil {
				return false, err
			}
			if redpandaCluster2.Status.Replicas != 1 {
				return false, nil
			}
			return true, nil
		})

		// Then each Cluster reports only its redpanda nodes
		assert.Equal(t, []string{
			fmt.Sprintf("%s-0.%s.%s.svc.cluster.local", key1.Name, key1.Name, key1.Namespace),
		}, redpandaCluster1.Status.Nodes)
		assert.Equal(t, []string{
			fmt.Sprintf("%s-0.%s.%s.svc.cluster.local", key2.Name, key2.Name, key2.Namespace),
		}, redpandaCluster2.Status.Nodes)
	})

	err = c.Delete(context.TODO(), &ns)
	require.NoError(t, err)
}

func Scheme() *runtime.Scheme {
	schemeLock.Do(func() {
		if err := apis.AddToScheme(scheme.Scheme); err != nil {
			fmt.Printf("failed to add API resources to the scheme: %v", err)
			os.Exit(-1)
		}
		if err := apiextv1.AddToScheme(scheme.Scheme); err != nil {
			fmt.Printf("failed to add V1 API extension resources to the scheme: %v", err)
			os.Exit(-1)
		}
		if err := apiextv1beta1.AddToScheme(scheme.Scheme); err != nil {
			fmt.Printf("failed to add V1beta1 API extension resources to the scheme: %v", err)
			os.Exit(-1)
		}
		if err := v1alpha1.AddToScheme(scheme.Scheme); err != nil {
			fmt.Printf("failed to add v1alpha1 redoanda API resources to the scheme: %v", err)
			os.Exit(-1)
		}
	})
	return scheme.Scheme
}
