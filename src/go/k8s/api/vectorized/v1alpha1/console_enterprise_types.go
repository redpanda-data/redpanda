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

// SecretStore contains the configuration for the secret manager that shall
// be used by Console to manage secrets for other components such as
// Kafka connect.
type SecretStore struct {
	Enabled bool `json:"enabled"`
	// SecretNamePrefix is the prefix that shall be used for each secret name
	// that will be stored. The prefix is used for namespacing your secrets,
	// so that one secret store can be used by multiple tenants.
	// For AWS it's common to use a path-like structure whereas GCP does not
	// allow slashes.
	//
	// Examples:
	// AWS: redpanda/analytics/prod/console/
	// GCP: redpanda-analytics-prod-console-
	//
	// Changing this prefix won't let you access secrets created under
	// a different prefix.
	SecretNamePrefix string                   `json:"secretNamePrefix"`
	GCPSecretManager *SecretManagerGCP        `json:"gcpSecretManager,omitempty"`
	AWSSecretManager *SecretManagerAWS        `json:"awsSecretManager,omitempty"`
	KafkaConnect     *SecretStoreKafkaConnect `json:"kafkaConnect,omitempty"`
}

// SecretManagerGCP is the configuration object for using Google Cloud's secret manager.
type SecretManagerGCP struct {
	// Enabled is whether GCP secret manager is enabled. Only one store
	// can be enabled at a time.
	Enabled bool `json:"enabled"`

	// CredentialsSecretRef points to Kubernetes secret where service account
	// will be mounted to Console and used to authenticate again GCP API.
	CredentialsSecretRef *SecretKeyRef `json:"credentialsSecretRef,omitempty"`

	// ServiceAccountNameAnnotation will be included in the Service Account definition.
	// That Kubernetes Service Account will be used in Kubernetes Deployment Spec of Console
	// Ref https://cloud.google.com/kubernetes-engine/docs/how-to/workload-identity
	ServiceAccountNameAnnotation string `json:"serviceAccountNameAnnotation,omitempty"`

	// ProjectID is the GCP project in which to store the secrets.
	ProjectID string `json:"projectId"`

	// Labels help you to organize your project, add arbitrary labels as key/value
	// pairs to your resources. Use labels to indicate different environments,
	// services, teams, and so on. Console may use additional labels for each secret.
	//
	// Use a label with key "owner" to namespace secrets within a secret manager.
	// This label will always be set for the creation and listing of all secrets.
	// If you change the value after secrets have been created, Console will no
	// longer return them and consider them as managed by another application.
	// The owner label is optional but recommended.
	//
	// Labels default to:
	// "owner": "console"
	Labels map[string]string `json:"labels,omitempty"`
}

// SecretManagerAWS is the configuration object for using Amazon's secret manager.
type SecretManagerAWS struct {
	// Enabled is whether AWS secret manager is enabled. Only one store
	// can be enabled at a time.
	Enabled bool `json:"enabled"`

	// Region in which service is deployed so that related resources like
	// secrets are put to the same region
	Region string `json:"region"`

	// KmsKeyID is the ARN, key ID, or alias of the KMS key that Secrets Manager uses to encrypt
	// the secret value in the secret.
	//
	// To use a KMS key in a different account, use the key ARN or the alias ARN.
	//
	// If you don't specify this value, then Secrets Manager uses the key aws/secretsmanager.
	// If that key doesn't yet exist, then Secrets Manager creates it for you automatically
	// the first time it encrypts the secret value.
	//
	// If the secret is in a different Amazon Web Services account from the credentials
	// calling the API, then you can't use aws/secretsmanager to encrypt the secret,
	// and you must create and use a customer managed KMS key.
	KmsKeyID *string `json:"kmsKeyId,omitempty"`

	// AWSCredentialsRef refers to Kubernetes secret where AWS access key id and secret access key
	// is taken and used as environments variable
	AWSCredentialsRef *corev1.LocalObjectReference `json:"AWSCredentialsRef,omitempty"`

	// ServiceAccountRoleARNAnnotation will be included in the Service Account definition.
	// That Kubernetes Service Account will be used in Kubernetes Deployment Spec of Console
	// Ref https://docs.aws.amazon.com/eks/latest/userguide/iam-roles-for-service-accounts.html
	ServiceAccountRoleARNAnnotation string `json:"serviceAccountRoleARNAnnotation,omitempty"`

	// Tags is a list of tags to attach to the secret. Each tag is a key and value a pair
	// of strings in a JSON text string, for example:
	//
	// [{"Key":"CostCenter","Value":"12345"},{"Key":"environment","Value":"production"}]
	//
	// Secrets Manager tag key names are case sensitive. A tag with the key "ABC"
	// is a different tag from one with key "abc".
	//
	// Tags can be used for permissions, so that you can namespace your secrets within a
	// single secret store. Console will also only allow the deletion of secrets that
	// posses the configured tags.
	// Tags default to:
	// "owner": "console"
	Tags map[string]string `json:"tags,omitempty"`
}

// SecretStoreKafkaConnect is a configuration block that specifies
// what configured Kafka connect clusters support loading secrets from
// the configured secret store. The frontend will only store sensitive
// connector configurations in the secret store if the respective
// kafka connect cluster is listed in this configuration.
type SecretStoreKafkaConnect struct {
	Enabled bool `json:"enabled"`
	// Clusters is the list of Kafka connect clusters
	// which the secret store shall be used for.
	Clusters []SecretStoreKafkaConnectCluster `json:"clusters"`
}

// SecretStoreKafkaConnectCluster configures the Kafka connect clusters
// that support loading secrets from the configured secret store.
type SecretStoreKafkaConnectCluster struct {
	// Name refers to the Kafka connect cluster name that has been
	// given in the connect configuration. This name must match some
	// cluster name or the configuration will be rejected.
	Name string `json:"name"`

	// SecretNamePrefixAppend is an optional string that shall be appended
	// to the global secretNamePrefix. This config is helpful if you want
	// to use a specific prefix for secrets belonging to this Kafka connect
	// cluster. You may want to do this if you want to restrict the
	// permissions for the kafka connect workers reading these secrets.
	//
	// Example:
	// secretstore.secretNamePrefix is set to: "redpanda/prod/"
	// secretstore.kafkaConnect.clusters.dwh.secretNamePrefixAppend is set to: "dwh/"
	// => Secrets will be created with the prefix "redpanda/prod/dwh/" so that
	// you can apply special iam permissions in your cloud account.
	SecretNamePrefixAppend string `json:"secretNamePrefixAppend"`
}
