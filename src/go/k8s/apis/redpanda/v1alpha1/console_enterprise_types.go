package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

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

// CloudConfig contains configurations for Redpanda cloud. If you're running a
// self-hosted installation, you can ignore this
type CloudConfig struct {
	PrometheusEndpoint *PrometheusEndpointConfig `json:"prometheusEndpoint"`
}

// PrometheusEndpointConfig configures the Prometheus endpoint that shall be
// exposed in Redpanda Cloud so that users can scrape this URL to
// collect their dataplane's metrics in their own time-series database.
type PrometheusEndpointConfig struct {
	Enabled   bool            `json:"enabled"`
	BasicAuth BasicAuthConfig `json:"basicAuth,omitempty"`
	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:Format=duration
	// +kubebuilder:default="1s"
	ResponseCacheDuration *metav1.Duration  `json:"responseCacheDuration,omitempty"`
	Prometheus            *PrometheusConfig `json:"prometheus"`
}

// BasicAuthConfig are credentials that will be required by the user in order to
// scrape the endpoint
type BasicAuthConfig struct {
	Username    string       `json:"username"`
	PasswordRef SecretKeyRef `json:"passwordRef"`
}

// PrometheusConfig is configuration of prometheus instance
type PrometheusConfig struct {
	// Address to Prometheus endpoint
	Address string `json:"address"`

	// Jobs is the list of Prometheus Jobs that we want to discover so that we
	// can then scrape the discovered targets ourselves.
	Jobs []PrometheusScraperJobConfig `json:"jobs"`

	// +kubebuilder:validation:Type=string
	// +kubebuilder:default="10s"
	TargetRefreshInterval *metav1.Duration `json:"targetRefreshInterval,omitempty"`
}

// PrometheusScraperJobConfig is the configuration object that determines what Prometheus
// targets we should scrape.
type PrometheusScraperJobConfig struct {
	// JobName refers to the Prometheus job name whose discovered targets we want to scrape
	JobName string `json:"jobName"`
	// KeepLabels is a list of label keys that are added by Prometheus when scraping
	// the target and should remain for all metrics as exposed to the Prometheus endpoint.
	KeepLabels []string `json:"keepLabels"`
}
