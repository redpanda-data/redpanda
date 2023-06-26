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
	"github.com/fluxcd/pkg/apis/meta"
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
	// AdditionalConfigSingleValue is free form map of any configuration option that topic can have with single string value.
	// Examples:
	// cleanup.policy=compact
	// redpanda.remote.write=true
	// redpanda.remote.read=true
	// redpanda.remote.recovery=true
	// redpanda.remote.delete=true
	AdditionalConfigSingleValue map[string]*string `json:"additionalConfigSingleValue,omitempty"`

	KafkaAPISpec *KafkaAPISpec `json:"kafkaApiSpec,omitempty"`

	// ClientID can be set to identify connection performed from Redpanda operator
	// By default, it is set to `redpanda-operator`
	ClientID *string `json:"clientId,omitempty"`

	// RackID should be used if connection should be made to particular rack
	RackID *string `json:"rackId,omitempty"`

	// MetricsNamespace can be used to overwrite fully-qualified
	// name of the Metric. That should be easier to identify if
	// multiple operator runs inside the same Kubernetes cluster.
	// By default, it is set to `redpanda-operator`.
	MetricsNamespace *string `json:"metricsNamespace,omitempty"`
}

// KafkaAPISpec represents definition for connection that used
// Kafka protocol.
type KafkaAPISpec struct {
	Brokers []string   `json:"brokers"`
	TLS     *KafkaTLS  `json:"tls,omitempty"`
	SASL    *KafkaSASL `json:"sasl,omitempty"`
}

// KafkaSASL to connect to Kafka using SASL credentials
type KafkaSASL struct {
	Username     string               `json:"username"`
	Password     string               `json:"password"`
	Mechanism    string               `json:"mechanism"`
	OAUth        KafkaSASLOAuthBearer `json:"oauth"`
	GSSAPIConfig KafkaSASLGSSAPI      `json:"gssapi"`
	AWSMskIam    KafkaSASLAwsMskIam   `json:"awsMskIam"`
}

// KafkaSASLOAuthBearer is the config struct for the SASL OAuthBearer mechanism
type KafkaSASLOAuthBearer struct {
	Token string `json:"token"`
}

// KafkaSASLGSSAPI represents the Kafka Kerberos config.
type KafkaSASLGSSAPI struct {
	AuthType           string `json:"authType"`
	KeyTabPath         string `json:"keyTabPath"`
	KerberosConfigPath string `json:"kerberosConfigPath"`
	ServiceName        string `json:"serviceName"`
	Username           string `json:"username"`
	Password           string `json:"password"`
	Realm              string `json:"realm"`

	// EnableFAST enables FAST, which is a pre-authentication framework for Kerberos.
	// It includes a mechanism for tunneling pre-authentication exchanges using armored KDC messages.
	// FAST provides increased resistance to passive password guessing attacks.
	EnableFast bool `json:"enableFast"`
}

// KafkaSASLAwsMskIam is the config for AWS IAM SASL mechanism,
// see: https://docs.aws.amazon.com/msk/latest/developerguide/iam-access-control.html
type KafkaSASLAwsMskIam struct {
	AccessKey string `json:"accessKey"`
	SecretKey string `json:"secretKey"`

	// SessionToken, if non-empty, is a session / security token to use for authentication.
	// See: https://docs.aws.amazon.com/STS/latest/APIReference/welcome.html
	SessionToken string `json:"sessionToken"`

	// UserAgent is the user agent to for the client to use when connecting
	// to Kafka, overriding the default "franz-go/<runtime.Version()>/<hostname>".
	//
	// Setting a UserAgent allows authorizing based on the aws:UserAgent
	// condition key; see the following link for more details:
	// https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_condition-keys.html#condition-keys-useragent
	UserAgent string `json:"userAgent"`
}

// KafkaTLS to connect to Kafka via TLS
type KafkaTLS struct {
	CaFilepath            string `json:"caFilepath"`
	CertFilepath          string `json:"certFilepath"`
	KeyFilepath           string `json:"keyFilepath"`
	InsecureSkipTLSVerify bool   `json:"insecureSkipTlsVerify"`
}

// TopicStatus defines the observed state of Topic
type TopicStatus struct {
	// ObservedGeneration is the last observed generation.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	meta.ReconcileRequestStatus `json:",inline"`

	// Conditions holds the conditions for the Topic.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// LastAppliedRevision is the revision of the last successfully applied source.
	// +optional
	LastAppliedRevision string `json:"lastAppliedRevision,omitempty"`

	// LastAttemptedRevision is the revision of the last reconciliation attempt.
	// +optional
	LastAttemptedRevision string `json:"lastAttemptedRevision,omitempty"`
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

func (t *Topic) GetName() string {
	topicName := t.Name
	if t.Spec.OverwriteTopicName != nil && *t.Spec.OverwriteTopicName != "" {
		topicName = *t.Spec.OverwriteTopicName
	}
	return topicName
}
