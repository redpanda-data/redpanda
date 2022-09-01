package console

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	redpandav1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	labels "github.com/redpanda-data/redpanda/src/go/k8s/pkg/labels"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// Service is a Console resource
type Service struct {
	client.Client
	scheme        *runtime.Scheme
	consoleobj    *redpandav1alpha1.Console
	clusterDomain string
	log           logr.Logger
}

// NewService instantiates a new Service
func NewService(
	cl client.Client,
	scheme *runtime.Scheme,
	consoleobj *redpandav1alpha1.Console,
	clusterDomain string,
	log logr.Logger,
) *Service {
	return &Service{
		Client:        cl,
		scheme:        scheme,
		consoleobj:    consoleobj,
		clusterDomain: clusterDomain,
		log:           log,
	}
}

const (
	// ServicePortName is the HTTP port name
	ServicePortName = "http"
)

// Ensure implements Resource interface
func (s *Service) Ensure(ctx context.Context) error {
	objLabels := labels.ForConsole(s.consoleobj)
	obj := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      s.consoleobj.GetName(),
			Namespace: s.consoleobj.GetNamespace(),
			Labels:    objLabels,
		},
		TypeMeta: metav1.TypeMeta{
			Kind:       "Service",
			APIVersion: "v1",
		},
		Spec: corev1.ServiceSpec{
			Type: corev1.ServiceTypeClusterIP,
			Ports: []corev1.ServicePort{
				{
					Name:       ServicePortName,
					Port:       int32(s.consoleobj.Spec.Server.HTTPListenPort),
					TargetPort: intstr.IntOrString{Type: intstr.String, StrVal: ServicePortName},
				},
			},
			Selector: objLabels,
		},
	}

	if err := controllerutil.SetControllerReference(s.consoleobj, obj, s.scheme); err != nil {
		return err
	}

	created, err := resources.CreateIfNotExists(ctx, s.Client, obj, s.log)
	if err != nil {
		return fmt.Errorf("creating Console service: %w", err)
	}

	if !created {
		// Update resource if not created.
		var current corev1.Service
		err = s.Get(ctx, types.NamespacedName{Name: obj.GetName(), Namespace: obj.GetNamespace()}, &current)
		if err != nil {
			return fmt.Errorf("fetching Console service: %w", err)
		}
		_, err = resources.Update(ctx, &current, obj, s.Client, s.log)
		if err != nil {
			return fmt.Errorf("updating Console service: %w", err)
		}
	}

	s.consoleobj.Status.Connectivity = &redpandav1alpha1.Connectivity{
		Internal: fmt.Sprintf(
			"%s.%s.svc.%s:%d",
			obj.GetName(), obj.GetNamespace(),
			s.clusterDomain,
			s.consoleobj.Spec.Server.HTTPListenPort,
		),
	}
	return s.Status().Update(ctx, s.consoleobj)
}

// Key implements Resource interface
func (s *Service) Key() types.NamespacedName {
	return types.NamespacedName{Name: s.consoleobj.GetName(), Namespace: s.consoleobj.GetNamespace()}
}
