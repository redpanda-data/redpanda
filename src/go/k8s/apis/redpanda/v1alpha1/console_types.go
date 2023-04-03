// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package v1alpha1

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var (
	// AllowConsoleAnyNamespace operator flag to control creating Console in any namespace aside from Redpanda namespace
	// Console needs SchemaRegistry TLS certs Secret, if enabled this flag copies Secrets from Redpanda namespace to Console local namespace
	// Secret syncing across namespaces might not be ideal especially for multi-tenant K8s clusters
	AllowConsoleAnyNamespace = false

	// ErrClusterNotConfigured is error returned if referenced Cluster is not yet configured
	ErrClusterNotConfigured = fmt.Errorf("cluster not configured")
)

// ConsoleSpec defines the desired state of Console
// Most of the fields here are copied from Console config
// REF https://github.com/redpanda-data/console/blob/master/backend/pkg/api/config.go
type ConsoleSpec struct {
	// +optional
	// +kubebuilder:default=console
	// Prefix for all exported prometheus metrics
	MetricsPrefix string `json:"metricsNamespace"`

	// +optional
	// +kubebuilder:default=true
	// Only relevant for developers, who might want to run the frontend separately
	ServeFrontend bool `json:"serveFrontend"`

	// +optional
	Server Server `json:"server"`

	SchemaRegistry Schema `json:"schema"`

	// The referenced Redpanda Cluster
	ClusterRef NamespaceNameRef `json:"clusterRef"`

	Deployment Deployment `json:"deployment"`
	Connect    Connect    `json:"connect"`

	Enterprise *Enterprise `json:"enterprise,omitempty"`

	// If you don't provide an enterprise license, Console ignores configurations for enterprise features
	// REF https://docs.redpanda.com/docs/console/reference/config/
	// If key is not provided in the SecretRef, Secret data should have key "license"
	LicenseRef *SecretKeyRef `json:"licenseRef,omitempty"`

	// Login contains all configurations in order to protect Console with a login screen
	// Configure one or more of the below identity providers in order to support SSO
	// This feature requires an Enterprise license
	// REF https://docs.redpanda.com/docs/console/single-sign-on/identity-providers/google/
	Login *EnterpriseLogin `json:"login,omitempty"`

	// Ingress contains configuration for the Console ingress.
	Ingress *IngressConfig `json:"ingress,omitempty"`

	// Cloud contains configurations for Redpanda cloud. If you're running a
	// self-hosted installation, you can ignore this
	Cloud *CloudConfig `json:"cloud,omitempty"`

	// Redpanda contains configurations that are Redpanda specific
	Redpanda *Redpanda `json:"redpanda,omitempty"`

	// SecretStore contains the configuration for the cloud provider secret manager
	SecretStore *SecretStore `json:"secretStore,omitempty"`
}

// Server is the Console app HTTP server config
// REF https://github.com/cloudhut/common/blob/b601d681e8599cee4255899def813142c0218e8b/rest/config.go
type Server struct {
	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:Format=duration
	// +kubebuilder:default="30s"
	// Timeout for graceful shutdowns
	ServerGracefulShutdownTimeout *metav1.Duration `json:"gracefulShutdownTimeout,omitempty"`

	// HTTP server listen address
	HTTPListenAddress string `json:"listenAddress,omitempty"`

	// +kubebuilder:default=8080
	// HTTP server listen port
	HTTPListenPort int `json:"listenPort,omitempty"`

	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:Format=duration
	// +kubebuilder:default="30s"
	// Read timeout for HTTP server
	HTTPServerReadTimeout *metav1.Duration `json:"readTimeout,omitempty"`

	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:Format=duration
	// +kubebuilder:default="30s"
	// Write timeout for HTTP server
	HTTPServerWriteTimeout *metav1.Duration `json:"writeTimeout,omitempty"`

	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:Format=duration
	// +kubebuilder:default="30s"
	// Idle timeout for HTTP server
	HTTPServerIdleTimeout *metav1.Duration `json:"idleTimeout,omitempty"`

	// +kubebuilder:default=4
	// Compression level applied to all http responses. Valid values are: 0-9 (0=completely disable compression middleware, 1=weakest compression, 9=best compression)
	CompressionLevel int `json:"compressionLevel,omitempty"`

	// Sets the subpath (root prefix) under which Kowl is reachable. If you want to host Kowl under 'your.domain.com/kowl/' you'd set the base path to 'kowl/'. The default is an empty string which makes Kowl reachable under just 'domain.com/'. When using this setting (or letting the 'X-Forwarded-Prefix' header set it for you) remember to either leave 'strip-prefix' enabled, or use a proxy that can strip the base-path/prefix before it reaches Kowl.
	BasePath string `json:"basePath,omitempty"`

	// +kubebuilder:default=true
	// server.set-base-path-from-x-forwarded-prefix", true, "When set to true, Kowl will use the 'X-Forwarded-Prefix' header as the base path. (When enabled the 'base-path' setting won't be used)
	SetBasePathFromXForwardedPrefix bool `json:"setBasePathFromXForwardedPrefix,omitempty"`

	// +kubebuilder:default=true
	// If a base-path is set (either by the 'base-path' setting, or by the 'X-Forwarded-Prefix' header), they will be removed from the request url. You probably want to leave this enabled, unless you are using a proxy that can remove the prefix automatically (like Traefik's 'StripPrefix' option)
	StripPrefix bool `json:"stripPrefix,omitempty"`
}

// Schema defines configurable fields for Schema Registry
type Schema struct {
	Enabled bool `json:"enabled"`
}

// Deployment defines configurable fields for the Console Deployment resource
type Deployment struct {
	Image string `json:"image"`

	// +kubebuilder:default=1
	Replicas int32 `json:"replicas,omitempty"`

	// +kubebuilder:default=0
	MaxUnavailable int32 `json:"maxUnavailable,omitempty"`

	// +kubebuilder:default=1
	MaxSurge int32 `json:"maxSurge,omitempty"`
}

// Connect defines configurable fields for Kafka Connect
type Connect struct {
	// +optional
	Enabled bool `json:"enabled"`

	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:Format=duration
	// +kubebuilder:default="15s"
	ConnectTimeout *metav1.Duration `json:"connectTimeout,omitempty"`

	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:Format=duration
	// +kubebuilder:default="60s"
	ReadTimeout *metav1.Duration `json:"readTimeout,omitempty"`

	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:Format=duration
	// +kubebuilder:default="6s"
	RequestTimeout *metav1.Duration `json:"requestTimeout,omitempty"`

	Clusters []ConnectCluster `json:"clusters,omitempty"`
}

// ConnectCluster defines configurable fields for the Kafka Connect cluster
type ConnectCluster struct {
	Name string `json:"name"`
	URL  string `json:"url"`

	// TLS configures mTLS auth
	TLS *ConnectClusterTLS `json:"tls,omitempty"`

	// BasicAuthRef configures basic auth credentials referenced by Secret
	// Expects to have keys "username", "password"
	BasicAuthRef *corev1.ObjectReference `json:"basicAuthRef,omitempty"`

	// TokenRef configures token header auth referenced by Secret
	// Expects to have key "token"
	TokenRef *corev1.ObjectReference `json:"tokenRef,omitempty"`
}

// ConnectClusterTLS defines TLS certificates for the Kafka Connect cluster
type ConnectClusterTLS struct {
	Enabled bool `json:"enabled,omitempty"`

	// SecretKeyRef configures certificate used for mTLS auth referenced by Secret
	// Expects to have keys "tls.crt", "tls.key", "ca.crt"
	SecretKeyRef *corev1.ObjectReference `json:"secretKeyRef,omitempty"`

	InsecureSkipTLSVerify bool `json:"insecureSkipTlsVerify,omitempty"`
}

// Redpanda defines configurable fields that are Redpanda specific
type Redpanda struct {
	AdminAPI *RedpandaAdmin `json:"adminApi,omitempty"`
}

// RedpandaAdmin defines API configuration that enables additional features that are Redpanda specific
type RedpandaAdmin struct {
	Enabled bool `json:"enabled"`
}

// ConsoleStatus defines the observed state of Console
type ConsoleStatus struct {
	// The ConfigMap used by Console
	// This is used to pass the ConfigMap used to mount in the Deployment Resource since Ensure() only returns error
	ConfigMapRef *corev1.ObjectReference `json:"configMapRef,omitempty"`

	// The generation observed by the controller
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	Connectivity *Connectivity `json:"connectivity,omitempty"`
}

// Connectivity defines internal/external hosts
type Connectivity struct {
	Internal string `json:"internal,omitempty"`
	External string `json:"external,omitempty"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status

// Console is the Schema for the consoles API
type Console struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ConsoleSpec   `json:"spec,omitempty"`
	Status ConsoleStatus `json:"status,omitempty"`
}

// GenerationMatchesObserved returns true if Generation matches ObservedGeneration
func (c *Console) GenerationMatchesObserved() bool {
	return c.GetGeneration() == c.Status.ObservedGeneration
}

// IsAllowedNamespace returns true if Console is valid to be created in current namespace
func (c *Console) IsAllowedNamespace() bool {
	return AllowConsoleAnyNamespace || c.GetNamespace() == c.Spec.ClusterRef.Namespace
}

// GetClusterRef returns the NamespacedName of referenced Cluster object
func (c *Console) GetClusterRef() types.NamespacedName {
	return types.NamespacedName{Name: c.Spec.ClusterRef.Name, Namespace: c.Spec.ClusterRef.Namespace}
}

// GetCluster returns the referenced Cluster object
func (c *Console) GetCluster(
	ctx context.Context, cl client.Client,
) (*Cluster, error) {
	cluster := &Cluster{}
	if err := cl.Get(ctx, c.GetClusterRef(), cluster); err != nil {
		return nil, err
	}
	if cc := cluster.Status.GetCondition(ClusterConfiguredConditionType); cc == nil || cc.Status != corev1.ConditionTrue {
		return nil, ErrClusterNotConfigured
	}
	return cluster, nil
}

//+kubebuilder:object:root=true

// ConsoleList contains a list of Console
type ConsoleList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Console `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Console{}, &ConsoleList{})
}
