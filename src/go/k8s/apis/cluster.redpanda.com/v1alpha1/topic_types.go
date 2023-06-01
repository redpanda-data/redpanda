// Copyright 2021-2023 Redpanda Data, Inc.
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

// TopicSpec defines the desired state of Topic
type TopicSpec struct {
	// Partitions is the number topic shards that is distributed across the nodes in a cluster.
	// This cannot be decreased after topic creation.
	// It can be increased after topic creation, but it is
	// important to understand the consequences that has, especially for
	// topics with semantic partitioning. When absent this will default to
	// the Redpanda cluster configuration `default_topic_partitions`.
	// https://docs.redpanda.com/docs/reference/cluster-properties/#default_topic_partitions
	// https://docs.redpanda.com/docs/get-started/architecture/#partitions
	Partitions *int `json:"partitions,omitempty"`
	// ReplicationFactor is the number of replicas the topic should have. Must be odd value.
	// When absent this will default to the Redpanda cluster configuration `default_topic_replications`.
	// https://docs.redpanda.com/docs/reference/cluster-properties/#default_topic_replications
	ReplicationFactor *int `json:"replicationFactor,omitempty"`
	// OverwriteTopicName will change the topic name from the `metadata.name` to `OverwriteTopicName`
	OverwriteTopicName *string `json:"overwriteTopicName,omitempty"`
	// AdditionalConfig is free form map of any configuration option that topic can have.
	// Examples:
	// cleanup.policy=compact
	// redpanda.remote.write=true
	// redpanda.remote.read=true
	// redpanda.remote.recovery=true
	// redpanda.remote.delete=true
	AdditionalConfig map[string]string `json:"additionalConfig,omitempty"`

	// SynchronizationInterval when the topic controller will schedule next reconciliation
	// Default is 3 seconds
	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:Format=duration
	// +kubebuilder:default="3s"
	SynchronizationInterval *metav1.Duration `json:"interval,omitempty"`
}

// TopicStatus defines the observed state of Topic
type TopicStatus struct {
	// ObservedGeneration is the last observed generation of the Topic.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions holds the conditions for the Topic.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status

// Topic is the Schema for the topics API
type Topic struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   TopicSpec   `json:"spec,omitempty"`
	Status TopicStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// TopicList contains a list of Topic
type TopicList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Topic `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Topic{}, &TopicList{})
}
