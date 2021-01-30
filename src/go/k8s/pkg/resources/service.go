package resources

import (
	"context"

	redpandav1alpha1 "github.com/vectorizedio/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

var _ Resource = &ServiceResource{}

type ServiceResource struct {
	client.Client
	scheme		*runtime.Scheme
	pandaCluster	*redpandav1alpha1.Cluster
}

func NewService(
	client client.Client,
	pandaCluster *redpandav1alpha1.Cluster,
	scheme *runtime.Scheme,
) *ServiceResource {
	return &ServiceResource{
		client, scheme, pandaCluster,
	}
}

func (r *ServiceResource) Ensure(ctx context.Context) error {
	var svc corev1.Service

	err := r.Get(ctx, r.Key(), &svc)
	if err != nil && !errors.IsNotFound(err) {
		return err
	}
	if errors.IsNotFound(err) {
		obj, err := r.Obj()
		if err != nil {
			return err
		}
		return r.Create(ctx, obj)
	}
	return nil
}

func (r *ServiceResource) Obj() (client.Object, error) {
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:	r.Key().Namespace,
			Name:		r.Key().Name,
			Labels:		r.pandaCluster.Labels,
		},
		Spec: corev1.ServiceSpec{
			ClusterIP:	corev1.ClusterIPNone,
			Ports: []corev1.ServicePort{
				{
					Name:		"kafka-tcp",
					Protocol:	corev1.ProtocolTCP,
					Port:		int32(r.pandaCluster.Spec.Configuration.KafkaAPI.Port),
					TargetPort:	intstr.FromInt(r.pandaCluster.Spec.Configuration.KafkaAPI.Port),
				},
			},
			Selector:	r.pandaCluster.Labels,
		},
	}

	err := controllerutil.SetControllerReference(r.pandaCluster, svc, r.scheme)
	if err != nil {
		return nil, err
	}

	return svc, nil
}

func (r *ServiceResource) Key() types.NamespacedName {
	return types.NamespacedName{Name: r.pandaCluster.Name, Namespace: r.pandaCluster.Namespace}
}

func (r *ServiceResource) Kind() string {
	var svc corev1.Service
	return svc.Kind
}
