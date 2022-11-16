package v1alpha1

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// SuperUsersPrefix is a prefix added to SuperUsers created and managed by the operator (i.e. PandaProxy, SchemaRegistry, Console)
// This is useful for identifying and grouping all users managed by the operator
// This is set as a configuration for backwards compatibility
var SuperUsersPrefix string

// SecretKeyRef contains enough information to inspect or modify the referred Secret data
// REF https://pkg.go.dev/k8s.io/api/core/v1#ObjectReference
type SecretKeyRef struct {
	// Name of the referent.
	// More info: https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#names
	Name string `json:"name"`

	// Namespace of the referent.
	// More info: https://kubernetes.io/docs/concepts/overview/working-with-objects/namespaces/
	Namespace string `json:"namespace"`

	// +optional
	// Key in Secret data to get value from
	Key string `json:"key,omitempty"`
}

// GetSecret fetches the referenced Secret
func (s *SecretKeyRef) GetSecret(
	ctx context.Context, cl client.Client,
) (*corev1.Secret, error) {
	secret := &corev1.Secret{}
	if err := cl.Get(ctx, client.ObjectKey{Namespace: s.Namespace, Name: s.Name}, secret); err != nil {
		return nil, fmt.Errorf("getting Secret %s/%s: %w", s.Namespace, s.Name, err)
	}
	return secret, nil
}

// GetValue extracts the value from the specified key or default
func (s *SecretKeyRef) GetValue(
	secret *corev1.Secret, defaultKey string,
) ([]byte, error) {
	key := s.Key
	if key == "" {
		key = defaultKey
	}

	value, ok := secret.Data[key]
	if !ok {
		return nil, fmt.Errorf("getting value from Secret %s/%s: key %s not found", s.Namespace, s.Name, key) //nolint:goerr113 // no need to declare new error type
	}
	return value, nil
}

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

// IngressConfig defines ingress specification
type IngressConfig struct {
	// Indicates if ingress is enabled (true when unspecified).
	Enabled *bool `json:"enabled,omitempty"`
	// Optional annotations for the generated ingress.
	Annotations map[string]string `json:"annotations,omitempty"`
	// If present, it's appended to the subdomain to form the ingress hostname.
	Endpoint string `json:"endpoint,omitempty"`
}
