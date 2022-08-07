package console

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources"
	"gopkg.in/yaml.v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	redpandav1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	labels "github.com/redpanda-data/redpanda/src/go/k8s/pkg/labels"
)

// ConfigMap is a Console resource
type ConfigMap struct {
	client.Client
	scheme     *runtime.Scheme
	consoleobj *redpandav1alpha1.Console
	clusterobj *redpandav1alpha1.Cluster
	log        logr.Logger
}

// NewConfigMap instantiates a new ConfigMap
func NewConfigMap(cl client.Client, scheme *runtime.Scheme, consoleobj *redpandav1alpha1.Console, clusterobj *redpandav1alpha1.Cluster, log logr.Logger) *ConfigMap {
	return &ConfigMap{
		Client:     cl,
		scheme:     scheme,
		consoleobj: consoleobj,
		clusterobj: clusterobj,
		log:        log,
	}
}

// Ensure implements Resource interface
func (cm *ConfigMap) Ensure(ctx context.Context) error {
	config, err := generateConsoleConfig(cm.consoleobj)
	if err != nil {
		return err
	}

	obj := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cm.consoleobj.GetName(),
			Namespace: cm.consoleobj.GetNamespace(),
			Labels:    labels.ForConsole(cm.consoleobj),
		},
		TypeMeta: metav1.TypeMeta{
			Kind:       "ConfigMap",
			APIVersion: "v1",
		},
		Data: map[string]string{
			"config.yaml": config,
		},
	}

	if err := controllerutil.SetControllerReference(cm.consoleobj, obj, cm.scheme); err != nil {
		return err
	}

	created, err := resources.CreateIfNotExists(ctx, cm.Client, obj, cm.log)
	if err != nil {
		return fmt.Errorf("creating Console configmap: %w", err)
	}

	if !created {
		// Update resource if not created.
		var current corev1.ConfigMap
		err = cm.Get(ctx, types.NamespacedName{Name: obj.GetName(), Namespace: obj.GetNamespace()}, &current)
		if err != nil {
			return fmt.Errorf("fetching Console configmap: %w", err)
		}
		_, err = resources.Update(ctx, &current, obj, cm.Client, cm.log)
		if err != nil {
			return fmt.Errorf("updating Console configmap: %w", err)
		}
	}

	return nil
}

// Key implements Resource interface
func (cm *ConfigMap) Key() types.NamespacedName {
	return types.NamespacedName{Name: cm.consoleobj.GetName(), Namespace: cm.consoleobj.GetNamespace()}
}

func generateConsoleConfig(consoleobj *redpandav1alpha1.Console) (string, error) {
	consoleConfig := redpandav1alpha1.ConsoleConfig{
		Server: redpandav1alpha1.Server{
			HTTPListenAddress: consoleobj.Spec.Server.HTTPListenAddress,
			HTTPListenPort:    consoleobj.Spec.Server.HTTPListenPort,
		},
	}

	config, err := yaml.Marshal(consoleConfig)
	if err != nil {
		return "", err
	}
	return string(config), nil
}
