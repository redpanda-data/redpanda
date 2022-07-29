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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// ConsoleSpec defines the desired state of Console
type ConsoleSpec struct {
	// +optional
	Server Server `json:"server"`

	Schema        Schema                 `json:"schema"`
	ClusterKeyRef corev1.ObjectReference `json:"clusterKeyRef"`
	Deployment    Deployment             `json:"deployment"`
}

// Server is the Console app HTTP server config
type Server struct {
	// +kubebuilder:default="30s"
	ServerGracefulShutdownTimeout string `json:"gracefulShutdownTimeout,omitempty" yaml:"gracefulShutdownTimeout,omitempty"`

	HTTPListenAddress string `json:"listenAddress,omitempty" yaml:"listenAddress,omitempty"`

	// +kubebuilder:default=8080
	HTTPListenPort int `json:"listenPort,omitempty" yaml:"listenPort,omitempty"`

	// +kubebuilder:default="30s"
	HTTPServerReadTimeout string `json:"readTimeout,omitempty" yaml:"readTimeout,omitempty"`

	// +kubebuilder:default="30s"
	HTTPServerWriteTimeout string `json:"writeTimeout,omitempty" yaml:"writeTimeout,omitempty"`

	// +kubebuilder:default="30s"
	HTTPServerIdleTimeout string `json:"idleTimeout,omitempty" yaml:"idleTimeout,omitempty"`

	// +kubebuilder:default=4
	CompressionLevel int `json:"compressionLevel,omitempty" yaml:"compressionLevel,omitempty"`

	BasePath string `json:"basePath,omitempty" yaml:"basePath,omitempty"`

	// +kubebuilder:default=true
	SetBasePathFromXForwardedPrefix bool `json:"setBasePathFromXForwardedPrefix,omitempty" yaml:"setBasePathFromXForwardedPrefix,omitempty"`

	// +kubebuilder:default=true
	StripPrefix bool `json:"stripPrefix,omitempty" yaml:"stripPrefix,omitempty"`
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

// ConsoleStatus defines the observed state of Console
type ConsoleStatus struct{}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status

// Console is the Schema for the consoles API
type Console struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ConsoleSpec   `json:"spec,omitempty"`
	Status ConsoleStatus `json:"status,omitempty"`
}

// GetClusterRef returns the NamespacedName of referenced Cluster object
func (c *Console) GetClusterRef() types.NamespacedName {
	return types.NamespacedName{Name: c.Spec.ClusterKeyRef.Name, Namespace: c.Spec.ClusterKeyRef.Namespace}
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
