package redpanda

// NamespaceNameRef contains namespace and name to inspect or modify the referred object
// REF https://pkg.go.dev/k8s.io/api/core/v1#ObjectReference
type NamespaceNameRef struct {
	// Name of the referent.
	// More info: https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#names
	Name string `json:"name"`

	// Namespace of the referent.
	// More info: https://kubernetes.io/docs/concepts/overview/working-with-objects/namespaces/
	Namespace string `json:"namespace"`
}
