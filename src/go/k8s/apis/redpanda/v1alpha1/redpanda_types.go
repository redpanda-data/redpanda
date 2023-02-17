// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// RedpandaSpec defines the desired state of Redpanda
type RedpandaSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// Foo is an example field of Redpanda. Edit redpanda_types.go to remove/update
	Foo string `json:"foo,omitempty"`
}

// RedpandaStatus defines the observed state of Redpanda
type RedpandaStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status

// Redpanda is the Schema for the redpanda API
type Redpanda struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   RedpandaSpec   `json:"spec,omitempty"`
	Status RedpandaStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// RedpandaList contains a list of Redpanda
type RedpandaList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Redpanda `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Redpanda{}, &RedpandaList{})
}
