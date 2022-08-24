package v1alpha1

import corev1 "k8s.io/api/core/v1"

// Enterprise defines configurable fields for features that require license
type Enterprise struct {
	// Console uses role-based access control (RBAC) to restrict system access to authorized users
	RBAC EnterpriseRBAC `json:"rbac"`
}

// EnterpriseRBAC defines configurable fields for specifying RBAC Authorization
type EnterpriseRBAC struct {
	Enabled         bool                        `json:"enabled"`
	RoleBindingsRef corev1.LocalObjectReference `json:"roleBindingsRef"`
}

// EnterpriseLogin defines configurable fields to enable SSO Authentication for supported login providers
type EnterpriseLogin struct {
	Enabled bool `json:"enabled"`

	// jwtSecret is a secret that is used to sign and encrypt the JSON Web tokens that are used by the backend for session management
	JWTSecret SecretRef `json:"jwtSecret"`

	Google *EnterpriseLoginGoogle `json:"google,omitempty"`
}

// EnterpriseLoginGoogle defines configurable fields for Google provider
type EnterpriseLoginGoogle struct {
	Enabled bool `json:"enabled"`

	// If key is not provided in the SecretRef, Secret data should have key "clientId" and "clientSecret"
	ClientCredentials SecretRef `json:"clientCredentials"`

	// Use Google groups in your RBAC role bindings.
	Directory *EnterpriseLoginGoogleDirectory `json:"directory,omitempty" yaml:"directory,omitempty"`
}

type EnterpriseLoginGoogleDirectory struct {
	ServiceAccountRef corev1.LocalObjectReference `json:"serviceAccountRef" yaml:"serviceAccountRef"`
	TargetPrincipal   string                      `json:"targetPrincipal" yaml:"targetPrincipal"`
}
