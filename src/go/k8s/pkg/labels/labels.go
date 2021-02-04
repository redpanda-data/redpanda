// Package labels handles label for cluster resource
package labels

import redpandav1alpha1 "github.com/vectorizedio/redpanda/src/go/k8s/apis/redpanda/v1alpha1"

// https://kubernetes.io/docs/concepts/overview/working-with-objects/common-labels/
// TODO support "app.kubernetes.io/version"
const (
	// The name of a higher level application this one is part of
	NameKey	= "app.kubernetes.io/name"
	// A unique name identifying the instance of an application
	InstanceKey	= "app.kubernetes.io/instance"
	// The component within the architecture
	ComponentKey	= "app.kubernetes.io/component"
	// The name of a higher level application this one is part of
	PartOfKey	= "app.kubernetes.io/part-of"
	// The tool being used to manage the operation of an application
	ManagedByKey	= "app.kubernetes.io/managed-by"
	nameKeyVal	= "redpanda"
)

// ForCluster returns a set of labels that is a union of cluster labels as well as recommended default labels
// recommended by the kubernetes documentation https://kubernetes.io/docs/concepts/overview/working-with-objects/common-labels/
func ForCluster(cluster *redpandav1alpha1.Cluster) map[string]string {
	dl := defaultLabels(cluster)
	labels := merge(cluster.Labels, dl)

	return labels
}

// merge merges two sets of labels
// if label is set in mainLabels, it won't be overwritten by newLabels
func merge(
	mainLabels map[string]string, newLabels map[string]string,
) map[string]string {
	if mainLabels == nil {
		mainLabels = make(map[string]string)
	}

	for k, v := range newLabels {
		if _, ok := mainLabels[k]; !ok {
			mainLabels[k] = v
		}
	}

	return mainLabels
}

func defaultLabels(cluster *redpandav1alpha1.Cluster) map[string]string {
	labels := make(map[string]string)
	labels[NameKey] = nameKeyVal
	labels[InstanceKey] = cluster.Name
	labels[ComponentKey] = "database"
	labels[PartOfKey] = nameKeyVal
	labels[ManagedByKey] = "redpanda-operator"

	return labels
}
