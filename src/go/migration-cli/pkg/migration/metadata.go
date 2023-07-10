package migration

import (
	"github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	vectorizedv1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/vectorized/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func CreateMigratedObj(cluster *vectorizedv1alpha1.Cluster, version string, defaultName string, defaultNamespace string) v1alpha1.Redpanda {
	if cluster == nil {
		return v1alpha1.Redpanda{
			ObjectMeta: metav1.ObjectMeta{
				Name:      defaultName,
				Namespace: defaultNamespace,
			},
			TypeMeta: metav1.TypeMeta{
				Kind:       "Redpanda",
				APIVersion: v1alpha1.GroupVersion.Group + "/" + v1alpha1.GroupVersion.Version,
			},
			Spec: v1alpha1.RedpandaSpec{
				ChartRef: v1alpha1.ChartRef{
					ChartVersion: version,
				},
			},
		}
	}

	annotations := cluster.Annotations
	if annotations == nil {
		annotations = make(map[string]string, 0)
	}

	delete(annotations, vectorizedv1alpha1.GroupVersion.Group+"/managed")

	annotations[v1alpha1.GroupVersion.Group+"/managed"] = "false"

	return v1alpha1.Redpanda{
		ObjectMeta: metav1.ObjectMeta{
			Name:        cluster.Name,
			Namespace:   cluster.Namespace,
			Labels:      cluster.Labels,
			Annotations: annotations,
		},
		TypeMeta: metav1.TypeMeta{
			Kind:       "Redpanda",
			APIVersion: v1alpha1.GroupVersion.Group + "/" + v1alpha1.GroupVersion.Version,
		},
		Spec: v1alpha1.RedpandaSpec{
			ChartRef: v1alpha1.ChartRef{
				ChartVersion: version,
			},
		},
	}
}
