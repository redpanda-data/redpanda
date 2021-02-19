// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// Package v1alpha1 represent Custom Resource definition of the vectorized.io redpanda group
package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ClusterSpec defines the desired state of Cluster
type ClusterSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// Image is the fully qualified name of the Redpanda container
	Image	string	`json:"image,omitempty"`
	// Version is the Redpanda container tag
	Version	string	`json:"version,omitempty"`
	// Replicas determine how big the cluster will be.
	// +kubebuilder:validation:Minimum=0
	Replicas	*int32	`json:"replicas,omitempty"`
	// Resources used by each Redpanda container
	// To calculate overall resource consumption one need to
	// multiply replicas against limits
	Resources	corev1.ResourceRequirements	`json:"resources"`
	// Configuration represent redpanda specific configuration
	Configuration	RedpandaConfig	`json:"configuration,omitempty"`
}

// ClusterStatus defines the observed state of Cluster
type ClusterStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// Replicas show how many nodes are working in the cluster
	// +optional
	Replicas	int32	`json:"replicas"`
	// Nodes of the provisioned redpanda nodes
	// +optional
	Nodes	[]string	`json:"nodes,omitempty"`
	// Indicates cluster is upgrading
	// +optional
	Upgrading	bool	`json:"upgrading"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status

// Cluster is the Schema for the clusters API
type Cluster struct {
	metav1.TypeMeta		`json:",inline"`
	metav1.ObjectMeta	`json:"metadata,omitempty"`

	Spec	ClusterSpec	`json:"spec,omitempty"`
	Status	ClusterStatus	`json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// ClusterList contains a list of Cluster
type ClusterList struct {
	metav1.TypeMeta	`json:",inline"`
	metav1.ListMeta	`json:"metadata,omitempty"`
	Items		[]Cluster	`json:"items"`
}

// RedpandaConfig is the definition of the main configuration
type RedpandaConfig struct {
	RPCServer	SocketAddress	`json:"rpcServer,omitempty"`
	KafkaAPI	SocketAddress	`json:"kafkaApi,omitempty"`
	AdminAPI	SocketAddress	`json:"admin,omitempty"`
	DeveloperMode	bool		`json:"developerMode,omitempty"`
}

// SocketAddress provide the way to configure the port
type SocketAddress struct {
	Port int `json:"port,omitempty"`
}

func init() {
	SchemeBuilder.Register(&Cluster{}, &ClusterList{})
}
