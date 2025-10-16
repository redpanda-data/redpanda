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
	"encoding/json"
	"errors"
	"time"
)

type ShadowLinkConfig struct {
	// Name is the name of the shadow link
	Name string
	// Configuration for the internal kafka client
	ClientOptions *ShadowLinkClientOptions `json:"client_options,omitempty" yaml:"client_options,omitempty"`
	// Topic metadata sync options
	TopicMetadataSyncOptions *TopicMetadataSyncOptions `json:"topic_metadata_sync_options,omitempty" yaml:"topic_metadata_sync_options,omitempty"`
	// Consumer offset sync options
	ConsumerOffsetSyncOptions *ConsumerOffsetSyncOptions `json:"consumer_offset_sync_options,omitempty" yaml:"consumer_offset_sync_options,omitempty"`
	// Security settings sync options
	SecuritySyncOptions *SecuritySettingsSyncOptions `json:"security_sync_options,omitempty" yaml:"security_sync_options,omitempty"`
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
	TLSSettings TLSSettings `json:"tls_settings,omitempty" yaml:"tls_settings,omitempty"`
	// Authentication settings
	AuthenticationConfiguration AuthenticationConfiguration `json:"authentication_configuration,omitempty" yaml:"authentication_configuration,omitempty"`
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
}

const (
	tlsSettingsTypeTLSFileSettings = "TLSFileSettings"
	tlsSettingsTypeTLSPEMSettings  = "TLSPEMSettings"
)

// TLSSettings is an interface for TLS configuration, supporting file paths or
// PEM content.
type TLSSettings interface {
	tlsSettingType() string
}

type TLSFileSettings struct {
	// Whether or not TLS is enabled
	Enabled bool `json:"enabled" yaml:"enabled"`
	// Path to the CA
	CAPath string `json:"ca_path,omitempty" yaml:"ca_path,omitempty"`
	// Key and Cert are optional but if one is provided, then both must be
	// Path to the key
	KeyPath string `json:"key_path,omitempty" yaml:"key_path,omitempty"`
	// Path to the cert
	CertPath string `json:"cert_path,omitempty" yaml:"cert_path,omitempty"`
}

func (*TLSFileSettings) tlsSettingType() string {
	return tlsSettingsTypeTLSFileSettings
}

type TLSPEMSettings struct {
	// Whether or not TLS is enabled
	Enabled bool `json:"enabled" yaml:"enabled"`
	// The CA
	CA string `json:"ca,omitempty" yaml:"ca,omitempty"`
	// Key and Cert are optional but if one is provided, then both must be
	// The key
	Key string `json:"key,omitempty" yaml:"key,omitempty"`
	// The cert
	Cert string `json:"cert,omitempty" yaml:"cert,omitempty"`
}

func (*TLSPEMSettings) tlsSettingType() string {
	return tlsSettingsTypeTLSPEMSettings
}

const authenticationTypeScram = "SCRAM"

type AuthenticationConfiguration interface {
	authenticationType() string
}

// ScramMechanism are valid SCRAM mechanisms.
type ScramMechanism string

const (
	// ScramMechanismScramSha256 represents SCRAM-SHA-256.
	ScramMechanismScramSha256 ScramMechanism = "SCRAM-SHA-256"
	// ScramMechanismScramSha512 represents SCRAM-SHA-512.
	ScramMechanismScramSha512 ScramMechanism = "SCRAM-SHA-512"
)

// ScramConfig is an authentication configuration using SCRAM.
type ScramConfig struct {
	// SCRAM username
	Username string `json:"username,omitempty" yaml:"username,omitempty"`
	// Password
	Password string `json:"password,omitempty" yaml:"password,omitempty"`
	// The SCRAM mechanism to use
	ScramMechanism ScramMechanism `json:"scram_mechanism,omitempty" yaml:"scram_mechanism,omitempty"`
}

func (*ScramConfig) authenticationType() string {
	return authenticationTypeScram
}

type TopicMetadataSyncOptions struct {
	// How often to sync metadata
	// If 0 provided, defaults to 30 seconds
	Interval time.Duration `json:"interval,omitempty" yaml:"interval,omitempty"`
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
}

type ConsumerOffsetSyncOptions struct {
	// Sync interval
	// If 0 provided, defaults to 30 seconds
	Interval time.Duration `json:"interval,omitempty" yaml:"interval,omitempty"`
	// Whether it's enabled
	Enabled bool `json:"enabled,omitempty" yaml:"enabled,omitempty"`
	// The filters
	GroupFilters []*NameFilter `json:"group_filters,omitempty" yaml:"group_filters,omitempty"`
}

type SecuritySettingsSyncOptions struct {
	// Sync interval
	// If 0 provided, defaults to 30 seconds
	Interval time.Duration `json:"interval,omitempty" yaml:"interval,omitempty"`
	// Whether or not it's enabled
	Enabled bool `json:"enabled,omitempty" yaml:"enabled,omitempty"`
	// ACL filters
	ACLFilters []*ACLFilter `json:"acl_filters,omitempty" yaml:"acl_filters,omitempty"`
}

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

// checkRawTLSConfig checks the raw map for TLS config and determines if it's
// file-based or PEM-based. It also performs validation to ensure that the
// config is valid.
func checkRawTLSConfig(raw map[string]any) (hasCA, hasCAPath bool, err error) {
	var isEnabled bool
	if val, hasEnabled := raw["enabled"]; hasEnabled {
		if enabledVal, ok := val.(bool); ok {
			isEnabled = enabledVal
		}
	}
	// Determine type based on presence of fields; ca_path is mandatory for
	// file-based and ca is mandatory for PEM-based.
	_, hasCa := raw["ca"]
	_, hasCaPath := raw["ca_path"]
	if hasCaPath && hasCa {
		return false, false, errors.New("both ca_path and ca are set; only one of these can be set")
	}
	if !hasCaPath && !hasCa && !isEnabled {
		return false, false, errors.New("unrecognized 'tls_settings' neither ca_path nor ca are set; one of these must be set")
	}
	return hasCa, hasCaPath, nil
}

// tlsSettingsWrapper is used for YAML unmarshaling of TLSSettings interface.
type tlsSettingsWrapper struct {
	TLSSettings
}

// UnmarshalYAML implements custom YAML unmarshaling for TLSSettings interface.
func (w *tlsSettingsWrapper) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var raw map[string]interface{}
	if err := unmarshal(&raw); err != nil {
		return err
	}

	_, hasCAPath, err := checkRawTLSConfig(raw)
	if err != nil {
		return err
	}

	if hasCAPath {
		var fileSettings TLSFileSettings
		if err := unmarshal(&fileSettings); err != nil {
			return err
		}
		w.TLSSettings = &fileSettings
		return nil
	}

	var pemSettings TLSPEMSettings
	if err := unmarshal(&pemSettings); err != nil {
		return err
	}
	w.TLSSettings = &pemSettings
	return nil
}

// authConfigWrapper is used for YAML unmarshaling of AuthenticationConfiguration interface.
type authConfigWrapper struct {
	AuthenticationConfiguration
}

// UnmarshalYAML implements custom YAML unmarshaling for AuthenticationConfiguration interface.
func (w *authConfigWrapper) UnmarshalYAML(unmarshal func(interface{}) error) error {
	// For now we only have SCRAM authentication, so we can unmarshal directly
	var scramConfig ScramConfig
	if err := unmarshal(&scramConfig); err != nil {
		return err
	}
	w.AuthenticationConfiguration = &scramConfig
	return nil
}

// UnmarshalYAML implements custom YAML unmarshaling for ShadowLinkClientOptions.
func (s *ShadowLinkClientOptions) UnmarshalYAML(unmarshal func(interface{}) error) error {
	// Create an auxiliary struct with all fields explicitly defined
	aux := &struct {
		BootstrapServers            []string            `yaml:"bootstrap_servers"`
		ClientID                    string              `yaml:"client_id"`
		SourceClusterID             string              `yaml:"source_cluster_id"`
		TLSSettings                 *tlsSettingsWrapper `yaml:"tls_settings"`
		AuthenticationConfiguration *authConfigWrapper  `yaml:"authentication_configuration"`
		MetadataMaxAgeMs            int32               `yaml:"metadata_max_age_ms"`
		ConnectionTimeoutMs         int32               `yaml:"connection_timeout_ms"`
		RetryBackoffMs              int32               `yaml:"retry_backoff_ms"`
		FetchWaitMaxMs              int32               `yaml:"fetch_wait_max_ms"`
		FetchMinBytes               int32               `yaml:"fetch_min_bytes"`
		FetchMaxBytes               int32               `yaml:"fetch_max_bytes"`
	}{}

	if err := unmarshal(aux); err != nil {
		return err
	}

	// Copy all the regular fields
	s.BootstrapServers = aux.BootstrapServers
	s.SourceClusterID = aux.SourceClusterID
	s.MetadataMaxAgeMs = aux.MetadataMaxAgeMs
	s.ConnectionTimeoutMs = aux.ConnectionTimeoutMs
	s.RetryBackoffMs = aux.RetryBackoffMs
	s.FetchWaitMaxMs = aux.FetchWaitMaxMs
	s.FetchMinBytes = aux.FetchMinBytes
	s.FetchMaxBytes = aux.FetchMaxBytes

	// Extract the interface values from the wrappers
	if aux.TLSSettings != nil {
		s.TLSSettings = aux.TLSSettings.TLSSettings
	}
	if aux.AuthenticationConfiguration != nil {
		s.AuthenticationConfiguration = aux.AuthenticationConfiguration.AuthenticationConfiguration
	}

	return nil
}

// UnmarshalJSON implements custom JSON unmarshaling for TLSSettings interface.
func (w *tlsSettingsWrapper) UnmarshalJSON(data []byte) error {
	var raw map[string]interface{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	_, hasCAPath, err := checkRawTLSConfig(raw)
	if err != nil {
		return err
	}
	if hasCAPath {
		var fileSettings TLSFileSettings
		if err := json.Unmarshal(data, &fileSettings); err != nil {
			return err
		}
		w.TLSSettings = &fileSettings
		return nil
	}

	var pemSettings TLSPEMSettings
	if err := json.Unmarshal(data, &pemSettings); err != nil {
		return err
	}
	w.TLSSettings = &pemSettings
	return nil
}

// UnmarshalJSON implements custom JSON unmarshaling for AuthenticationConfiguration interface.
func (w *authConfigWrapper) UnmarshalJSON(data []byte) error {
	// For now we only have SCRAM authentication, so we can unmarshal directly
	var scramConfig ScramConfig
	if err := json.Unmarshal(data, &scramConfig); err != nil {
		return err
	}
	w.AuthenticationConfiguration = &scramConfig
	return nil
}

// UnmarshalJSON implements custom JSON unmarshaling for ShadowLinkClientOptions.
func (s *ShadowLinkClientOptions) UnmarshalJSON(data []byte) error {
	// Create an auxiliary struct with all fields explicitly defined
	aux := &struct {
		BootstrapServers            []string            `json:"bootstrap_servers"`
		ClientID                    string              `json:"client_id"`
		SourceClusterID             string              `json:"source_cluster_id"`
		TLSSettings                 *tlsSettingsWrapper `json:"tls_settings"`
		AuthenticationConfiguration *authConfigWrapper  `json:"authentication_configuration"`
		MetadataMaxAgeMs            int32               `json:"metadata_max_age_ms"`
		ConnectionTimeoutMs         int32               `json:"connection_timeout_ms"`
		RetryBackoffMs              int32               `json:"retry_backoff_ms"`
		FetchWaitMaxMs              int32               `json:"fetch_wait_max_ms"`
		FetchMinBytes               int32               `json:"fetch_min_bytes"`
		FetchMaxBytes               int32               `json:"fetch_max_bytes"`
	}{}

	if err := json.Unmarshal(data, aux); err != nil {
		return err
	}

	// Copy all the regular fields
	s.BootstrapServers = aux.BootstrapServers
	s.SourceClusterID = aux.SourceClusterID
	s.MetadataMaxAgeMs = aux.MetadataMaxAgeMs
	s.ConnectionTimeoutMs = aux.ConnectionTimeoutMs
	s.RetryBackoffMs = aux.RetryBackoffMs
	s.FetchWaitMaxMs = aux.FetchWaitMaxMs
	s.FetchMinBytes = aux.FetchMinBytes
	s.FetchMaxBytes = aux.FetchMaxBytes

	// Extract the interface values from the wrappers
	if aux.TLSSettings != nil {
		s.TLSSettings = aux.TLSSettings.TLSSettings
	}
	if aux.AuthenticationConfiguration != nil {
		s.AuthenticationConfiguration = aux.AuthenticationConfiguration.AuthenticationConfiguration
	}

	return nil
}
