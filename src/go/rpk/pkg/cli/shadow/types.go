// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package shadow

import (
	"time"
)

type ShadowLinkConfig struct {
	// Name is the name of the shadow link
	Name string `json:"name" yaml:"name"`
	// Configurations for Shadow Link in Redpanda Cloud
	CloudOptions *CloudShadowLinkOptions `json:"cloud_options,omitempty" yaml:"cloud_options,omitempty"`
	// Configuration for the internal kafka client
	ClientOptions *ShadowLinkClientOptions `json:"client_options,omitempty" yaml:"client_options,omitempty"`
	// Topic metadata sync options
	TopicMetadataSyncOptions *TopicMetadataSyncOptions `json:"topic_metadata_sync_options,omitempty" yaml:"topic_metadata_sync_options,omitempty"`
	// Consumer offset sync options
	ConsumerOffsetSyncOptions *ConsumerOffsetSyncOptions `json:"consumer_offset_sync_options,omitempty" yaml:"consumer_offset_sync_options,omitempty"`
	// Security settings sync options
	SecuritySyncOptions *SecuritySettingsSyncOptions `json:"security_sync_options,omitempty" yaml:"security_sync_options,omitempty"`
	// Schema Registry sync options
	SchemaRegistrySyncOptions *SchemaRegistrySyncOptions `json:"schema_registry_sync_options,omitempty" yaml:"schema_registry_sync_options,omitempty"`
}

type CloudShadowLinkOptions struct {
	// Source Redpanda cluster ID. This field is optional. If provided, fetches
	// bootstrap server information.
	SourceRedpandaID string `json:"source_redpanda_id,omitempty" yaml:"source_redpanda_id,omitempty"`
	// Shadow Redpanda cluster ID where the shadow link will be created.
	ShadowRedpandaID string `json:"shadow_redpanda_id,omitempty" yaml:"shadow_redpanda_id,omitempty"`
}

type ShadowLinkClientOptions struct {
	// The bootstrap servers to use
	BootstrapServers []string `json:"bootstrap_servers,omitempty" yaml:"bootstrap_servers,omitempty"`
	// If provided, this is the expected ID of the source cluster.  If it does
	// not match then the connection will be rejected.  If provided, this value
	// must match the `ClusterId` field returned in the Kafka Metadata response
	// message
	SourceClusterID string `json:"source_cluster_id,omitempty" yaml:"source_cluster_id,omitempty"`
	// TLS settings
	TLSSettings *TLSSettings `json:"tls_settings,omitempty" yaml:"tls_settings,omitempty"`
	// Authentication settings
	AuthenticationConfiguration *AuthenticationConfiguration `json:"authentication_configuration,omitempty" yaml:"authentication_configuration,omitempty"`
	// Max metadata age
	// If 0 is provided, defaults to 10 seconds
	MetadataMaxAgeMs int32 `json:"metadata_max_age_ms,omitempty" yaml:"metadata_max_age_ms,omitempty"`
	// Connection timeout
	// If 0 is provided, defaults to 1 second
	ConnectionTimeoutMs int32 `json:"connection_timeout_ms,omitempty" yaml:"connection_timeout_ms,omitempty"`
	// Retry base backoff
	// If 0 is provided, defaults to 100ms
	RetryBackoffMs int32 `json:"retry_backoff_ms,omitempty" yaml:"retry_backoff_ms,omitempty"`
	// Fetch request timeout
	// If 0 is provided, defaults to 100ms
	FetchWaitMaxMs int32 `json:"fetch_wait_max_ms,omitempty" yaml:"fetch_wait_max_ms,omitempty"`
	// Fetch min bytes
	// If 0 is provided, defaults to 1 byte
	FetchMinBytes int32 `json:"fetch_min_bytes,omitempty" yaml:"fetch_min_bytes,omitempty"`
	// Fetch max bytes
	// If 0 is provided, defaults to 1MiB
	FetchMaxBytes int32 `json:"fetch_max_bytes,omitempty" yaml:"fetch_max_bytes,omitempty"`
	// Fetch partition max bytes.
	// If 0 is provided, defaults to 1 MiB
	FetchPartitionMaxBytes int32 `json:"fetch_partition_max_bytes,omitempty" yaml:"fetch_partition_max_bytes,omitempty"`
}

// TLSSettings is the union for TLS configuration, supporting file paths or
// PEM content.
type TLSSettings struct {
	// Whether or not TLS is enabled
	Enabled bool `json:"enabled" yaml:"enabled"`
	// If true, the SNI hostname will not be provided when TLS is used
	DoNotSetSniHostname bool `json:"do_not_set_sni_hostname,omitempty" yaml:"do_not_set_sni_hostname,omitempty"`
	// One of the following must be provided:
	TLSFileSettings *TLSFileSettings `json:"tls_file_settings,omitempty" yaml:"tls_file_settings,omitempty"`
	TLSPEMSettings  *TLSPEMSettings  `json:"tls_pem_settings,omitempty" yaml:"tls_pem_settings,omitempty"`
}

type TLSFileSettings struct {
	// Path to the CA
	CAPath string `json:"ca_path,omitempty" yaml:"ca_path,omitempty"`
	// Key and Cert are optional but if one is provided, then both must be
	// Path to the key
	KeyPath string `json:"key_path,omitempty" yaml:"key_path,omitempty"`
	// Path to the cert
	CertPath string `json:"cert_path,omitempty" yaml:"cert_path,omitempty"`
}

type TLSPEMSettings struct {
	// The CA
	CA string `json:"ca,omitempty" yaml:"ca,omitempty"`
	// Key and Cert are optional but if one is provided, then both must be
	// The key
	Key string `json:"key,omitempty" yaml:"key,omitempty"`
	// The cert
	Cert string `json:"cert,omitempty" yaml:"cert,omitempty"`
}

// AuthenticationConfiguration is the union for authentication configurations.
type AuthenticationConfiguration struct {
	ScramConfiguration *ScramConfiguration `json:"scram_configuration,omitempty" yaml:"scram_configuration,omitempty"`
	PlainConfiguration *PlainConfiguration `json:"plain_configuration,omitempty" yaml:"plain_configuration,omitempty"`
}

// PlainConfiguration is an authentication configuration using SASL/PLAIN.
type PlainConfiguration struct {
	// PLAIN username
	Username string `json:"username,omitempty" yaml:"username,omitempty"`
	// Password
	Password string `json:"password,omitempty" yaml:"password,omitempty"`
}

// ScramConfiguration is an authentication configuration using SCRAM.
type ScramConfiguration struct {
	// SCRAM username
	Username string `json:"username,omitempty" yaml:"username,omitempty"`
	// Password
	Password string `json:"password,omitempty" yaml:"password,omitempty"`
	// The SCRAM mechanism to use
	ScramMechanism ScramMechanism `json:"scram_mechanism,omitempty" yaml:"scram_mechanism,omitempty"`
}

// ScramMechanism are valid SCRAM mechanisms.
type ScramMechanism string

const (
	// ScramMechanismScramSha256 represents SCRAM-SHA-256.
	ScramMechanismScramSha256 ScramMechanism = "SCRAM-SHA-256"
	// ScramMechanismScramSha512 represents SCRAM-SHA-512.
	ScramMechanismScramSha512 ScramMechanism = "SCRAM-SHA-512"
)

type TopicMetadataSyncOptions struct {
	// How often to sync metadata
	// If 0 provided, defaults to 30 seconds
	Interval time.Duration `json:"interval,omitempty" yaml:"interval,omitempty"`
	// Allows user to pause the topic sync task.  If paused, then
	// the task will enter the 'paused' state and not sync topics or their
	// properties from the source cluster.
	Paused bool `json:"paused,omitempty" yaml:"paused,omitempty"`
	// The topic filters to use
	AutoCreateShadowTopicFilters []*NameFilter `json:"auto_create_shadow_topic_filters,omitempty" yaml:"auto_create_shadow_topic_filters,omitempty"`
	// Additional topic properties to shadow
	// Partition count, `max.message.bytes`, `cleanup.policy` and
	// `timestamp.type` will always be replicated
	SyncedShadowTopicProperties []string `json:"synced_shadow_topic_properties,omitempty" yaml:"synced_shadow_topic_properties,omitempty"`
	// If false, then the following topic properties will be synced by default:
	// - compression.type
	// - retention.bytes
	// - retention.ms
	// - delete.retention.ms
	// - Replication Factor
	// - min.compaction.lag.ms
	// - max.compaction.lag.ms
	//
	// If this is true, then only the properties listed in
	// `synced_shadow_topic_properties` will be synced.
	ExcludeDefault bool `json:"exclude_default,omitempty" yaml:"exclude_default,omitempty"`
	// One Of the following must be selected
	//
	// - StartAtEarliest: Start syncing from the earliest offset
	// - StartAtLatest: Start syncing from the latest offset
	// - StartAtTimestamp: Start syncing from a specific timestamp
	StartAtEarliest  *StartAtEarliest `json:"start_at_earliest,omitempty" yaml:"start_at_earliest,omitempty"`
	StartAtLatest    *StartAtLatest   `json:"start_at_latest,omitempty" yaml:"start_at_latest,omitempty"`
	StartAtTimestamp *time.Time       `json:"start_at_timestamp,omitempty" yaml:"start_at_timestamp,omitempty"`
}

type (
	StartAtEarliest struct{}
	StartAtLatest   struct{}
)

type ConsumerOffsetSyncOptions struct {
	// Sync interval
	// If 0 provided, defaults to 30 seconds
	Interval time.Duration `json:"interval,omitempty" yaml:"interval,omitempty"`
	// Allows user to pause the consumer offset sync task. If paused, then
	// the task will enter the 'paused' state and not sync consumer offsets from
	// the source cluster
	Paused bool `json:"paused,omitempty" yaml:"paused,omitempty"`
	// The filters
	GroupFilters []*NameFilter `json:"group_filters,omitempty" yaml:"group_filters,omitempty"`
}

type SecuritySettingsSyncOptions struct {
	// Sync interval
	// If 0 provided, defaults to 30 seconds
	Interval time.Duration `json:"interval,omitempty" yaml:"interval,omitempty"`
	// Allows user to pause the security settings sync task. If paused,
	// then the task will enter the 'paused' state and will not sync security
	// settings from the source cluster
	Paused bool `json:"paused,omitempty" yaml:"paused,omitempty"`
	// ACL filters
	ACLFilters []*ACLFilter `json:"acl_filters,omitempty" yaml:"acl_filters,omitempty"`
}

type SchemaRegistrySyncOptions struct {
	// Shadow the entire source cluster's Schema Registry byte-for-byte.
	// If set, the Shadow Link will attempt to add the `_schemas`
	// topic to the list of Shadow Topics as long as:
	// 1. The `_schemas` topic exists on the source cluster
	// 2. The `_schemas` topic does not exist on the shadow cluster, or it is
	// empty.
	// If either of the above conditions are _not_ met, then the `_schemas`
	// topic will _not_ be shadowed by this cluster. Unsetting this flag will
	// _not_ remove the `_schemas` topic from shadowing if it has already been
	// added.  Once made a shadow topic, the
	// `_schemas` topic will be replicated byte-for-byte.  To stop shadowing the
	// `_schemas` topic, unset this field, then either fail-over the topic or
	// delete it.
	ShadowSchemaRegistryTopic *ShadowSchemaRegistryTopic `json:"shadow_schema_registry_topic,omitempty" yaml:"shadow_schema_registry_topic,omitempty"`
}

type ShadowSchemaRegistryTopic struct{}

type NameFilter struct {
	// Literal or prefix
	PatternType PatternType `json:"pattern_type,omitempty" yaml:"pattern_type,omitempty"`
	// Include or exclude
	FilterType FilterType `json:"filter_type,omitempty" yaml:"filter_type,omitempty"`
	// The resource name, or "*"
	// Note if "*", must be the _only_ character
	// and `pattern_type` must be `LITERAL`
	Name string `json:"name,omitempty" yaml:"name,omitempty"`
}

type FilterType string

const (
	// FilterTypeInclude Include the items that match the filter.
	FilterTypeInclude FilterType = "INCLUDE"
	// FilterTypeExclude Exclude the items that match the filter.
	FilterTypeExclude FilterType = "EXCLUDE"
)

type PatternType string

const (
	// PatternTypeLiteral Must match the filter exactly.
	PatternTypeLiteral PatternType = "LITERAL"
	// PatternTypePrefix Will match anything that starts with filter.
	PatternTypePrefix PatternType = "PREFIX"
)

type ACLFilter struct {
	// The resource filter
	ResourceFilter *ACLResourceFilter `json:"resource_filter,omitempty" yaml:"resource_filter,omitempty"`
	// The access filter
	AccessFilter *ACLAccessFilter `json:"access_filter,omitempty" yaml:"access_filter,omitempty"`
}

type ACLResourceFilter struct {
	// The ACL resource type to match
	ResourceType ACLResource `json:"resource_type,omitempty" yaml:"resource_type,omitempty"`
	// The pattern to apply to name
	PatternType ACLPattern `json:"pattern_type,omitempty" yaml:"pattern_type,omitempty"`
	// Name, if not given will default to match all items in `resource_type`.
	// Note that asterisk `*` is literal and matches resource ACLs
	// that are named `*`
	Name string `json:"name,omitempty" yaml:"name,omitempty"`
}

type ACLAccessFilter struct {
	// The name of the principal, if not set will default to match
	// all principals with the specified `operation` and `permission_type`
	Principal string `json:"principal,omitempty" yaml:"principal,omitempty"`
	// The ACL operation to match
	Operation ACLOperation `json:"operation,omitempty" yaml:"operation,omitempty"`
	// The permission type
	PermissionType ACLPermissionType `json:"permission_type,omitempty" yaml:"permission_type,omitempty"`
	// The host to match.  If not set, will default to match all hosts
	// with the specified `operation` and `permission_type`. Note that
	// the asterisk `*` is literal and matches hosts that are set to `*`
	Host string `json:"host,omitempty" yaml:"host,omitempty"`
}

type ACLResource string

const (
	// ACLResourceAny is a wildcard for selecting any ACL resource.
	ACLResourceAny ACLResource = "ANY"
	// ACLResourceCluster is the Cluster wide resource.
	ACLResourceCluster ACLResource = "CLUSTER"
	// ACLResourceGroup is the Consumer group resource.
	ACLResourceGroup ACLResource = "GROUP"
	// ACLResourceTopic is the Topic resource.
	ACLResourceTopic ACLResource = "TOPIC"
	// ACLResourceTXNID is the Transaction ID resource.
	ACLResourceTXNID ACLResource = "TXN_ID"
	// ACLResourceSRSubject is the Schema Registry subject resource.
	ACLResourceSRSubject ACLResource = "SUBJECT"
	// ACLResourceSRRegistry is the Schema Registry wide resource.
	ACLResourceSRRegistry ACLResource = "REGISTRY"
	// ACLResourceSRAny is a wildcard to match any SR ACL resource.
	ACLResourceSRAny ACLResource = "SR_ANY"
)

type ACLPattern string

const (
	// ACLPatternAny is a wildcard to match any pattern.
	ACLPatternAny ACLPattern = "ANY"
	// ACLPatternLiteral Match a literal string.
	ACLPatternLiteral ACLPattern = "LITERAL"
	// ACLPatternPrefixed Match a prefix.
	ACLPatternPrefixed ACLPattern = "PREFIXED"
	// ACLPatternMatch serves as a catch-all for all the names of a topic the
	// principal is authorized to access.
	ACLPatternMatch ACLPattern = "MATCH"
)

type ACLOperation string

const (
	ACLOperationAny             ACLOperation = "ANY"
	ACLOperationRead            ACLOperation = "READ"
	ACLOperationWrite           ACLOperation = "WRITE"
	ACLOperationCreate          ACLOperation = "CREATE"
	ACLOperationRemove          ACLOperation = "REMOVE"
	ACLOperationAlter           ACLOperation = "ALTER"
	ACLOperationDescribe        ACLOperation = "DESCRIBE"
	ACLOperationClusterAction   ACLOperation = "CLUSTER_ACTION"
	ACLOperationDescribeConfigs ACLOperation = "DESCRIBE_CONFIGS"
	ACLOperationAlterConfigs    ACLOperation = "ALTER_CONFIGS"
	ACLOperationIdempotentWrite ACLOperation = "IDEMPOTENT_WRITE"
)

type ACLPermissionType string

const (
	ACLPermissionTypeAny   ACLPermissionType = "ANY"
	ACLPermissionTypeAllow ACLPermissionType = "ALLOW"
	ACLPermissionTypeDeny  ACLPermissionType = "DENY"
)
