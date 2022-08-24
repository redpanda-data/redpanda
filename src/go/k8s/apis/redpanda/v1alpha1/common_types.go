package v1alpha1

// SecretRef contains enough information to inspect or modify the referred Secret data
// REF https://pkg.go.dev/k8s.io/api/core/v1#ObjectReference
type SecretRef struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
	Key       string `json:"key,omitempty"`
}
