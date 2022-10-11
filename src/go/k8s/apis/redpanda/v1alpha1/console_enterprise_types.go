package v1alpha1

import corev1 "k8s.io/api/core/v1"

// Enterprise defines configurable fields for features that require license
type Enterprise struct {
	// Console uses role-based access control (RBAC) to restrict system access to authorized users
	RBAC EnterpriseRBAC `json:"rbac"`
}

// EnterpriseRBAC defines configurable fields for specifying RBAC Authorization
type EnterpriseRBAC struct {
	Enabled bool `json:"enabled"`

	// RoleBindingsRef is the ConfigMap that contains the RBAC file
	// The ConfigMap should contain "rbac.yaml" key
	RoleBindingsRef corev1.LocalObjectReference `json:"roleBindingsRef"`
}

// EnterpriseLogin defines configurable fields to enable SSO Authentication for supported login providers
type EnterpriseLogin struct {
	Enabled bool `json:"enabled"`

	// JWTSecret is the Secret that is used to sign and encrypt the JSON Web tokens that are used by the backend for session management
	// If not provided, the default key is "jwt"
	JWTSecretRef SecretKeyRef `json:"jwtSecretRef"`

	Google *EnterpriseLoginGoogle `json:"google,omitempty"`

	RedpandaCloud *EnterpriseLoginRedpandaCloud `json:"redpandaCloud,omitempty"`
}

// EnterpriseLoginRedpandaCloud defines configurable fields for RedpandaCloud SSO provider
type EnterpriseLoginRedpandaCloud struct {
	Enabled bool `json:"enabled" yaml:"enabled"`

	// Domain is the domain of the auth server
	Domain string `json:"domain" yaml:"domain"`

	// Audience is the domain where this auth is intended for
	Audience string `json:"audience" yaml:"audience"`

	// AllowedOrigins indicates if response is allowed from given origin
	AllowedOrigins []string `json:"allowedOrigins,omitempty" yaml:"allowedOrigins,omitempty"`
}

// IsGoogleLoginEnabled returns true if Google SSO provider is enabled
func (c *Console) IsGoogleLoginEnabled() bool {
	login := c.Spec.Login
	return login != nil && login.Google != nil && login.Google.Enabled
}

// EnterpriseLoginGoogle defines configurable fields for Google provider
type EnterpriseLoginGoogle struct {
	Enabled bool `json:"enabled"`

	// ClientCredentials is the Secret that contains SSO credentials
	// The Secret should contain keys "clientId", "clientSecret"
	ClientCredentialsRef NamespaceNameRef `json:"clientCredentialsRef"`

	// Use Google groups in your RBAC role bindings.
	Directory *EnterpriseLoginGoogleDirectory `json:"directory,omitempty"`
}

// EnterpriseLoginGoogleDirectory defines configurable fields for enabling RBAC Google groups sync
type EnterpriseLoginGoogleDirectory struct {
	// ServiceAccountRef is the ConfigMap that contains the Google Service Account json
	// The ConfigMap should contain "sa.json" key
	ServiceAccountRef corev1.LocalObjectReference `json:"serviceAccountRef"`

	// TargetPrincipal is the user that shall be impersonated by the service account
	TargetPrincipal string `json:"targetPrincipal"`
}
