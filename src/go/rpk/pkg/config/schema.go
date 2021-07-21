// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package config

import "path"

type Config struct {
	NodeUuid             string                 `yaml:"node_uuid,omitempty" mapstructure:"node_uuid,omitempty" json:"nodeUuid"`
	Organization         string                 `yaml:"organization,omitempty" mapstructure:"organization,omitempty" json:"organization"`
	LicenseKey           string                 `yaml:"license_key,omitempty" mapstructure:"license_key,omitempty" json:"licenseKey"`
	ClusterId            string                 `yaml:"cluster_id,omitempty" mapstructure:"cluster_id,omitempty" json:"clusterId"`
	ConfigFile           string                 `yaml:"config_file" mapstructure:"config_file" json:"configFile"`
	Redpanda             RedpandaConfig         `yaml:"redpanda" mapstructure:"redpanda" json:"redpanda"`
	Rpk                  RpkConfig              `yaml:"rpk" mapstructure:"rpk" json:"rpk"`
	Pandaproxy           *Pandaproxy            `yaml:"pandaproxy,omitempty" mapstructure:"pandaproxy,omitempty" json:"pandaproxy,omitempty"`
	PandaproxyClient     *KafkaClient           `yaml:"pandaproxy_client,omitempty" mapstructure:"pandaproxy_client,omitempty" json:"pandaproxyClient,omitempty"`
	SchemaRegistry       *SchemaRegistry        `yaml:"schema_registry,omitempty" mapstructure:"schema_registry,omitempty" json:"schemaRegistry,omitempty"`
	SchemaRegistryClient *KafkaClient           `yaml:"schema_registry_client,omitempty" mapstructure:"schema_registry_client,omitempty" json:"schemaRegistryClient,omitempty"`
	Other                map[string]interface{} `yaml:",inline" mapstructure:",remain"`
}

type RedpandaConfig struct {
	Directory                            string                 `yaml:"data_directory" mapstructure:"data_directory" json:"dataDirectory"`
	RPCServer                            SocketAddress          `yaml:"rpc_server" mapstructure:"rpc_server" json:"rpcServer"`
	AdvertisedRPCAPI                     *SocketAddress         `yaml:"advertised_rpc_api,omitempty" mapstructure:"advertised_rpc_api,omitempty" json:"advertisedRpcApi,omitempty"`
	KafkaApi                             []NamedSocketAddress   `yaml:"kafka_api" mapstructure:"kafka_api" json:"kafkaApi"`
	AdvertisedKafkaApi                   []NamedSocketAddress   `yaml:"advertised_kafka_api,omitempty" mapstructure:"advertised_kafka_api,omitempty" json:"advertisedKafkaApi,omitempty"`
	KafkaApiTLS                          []ServerTLS            `yaml:"kafka_api_tls,omitempty" mapstructure:"kafka_api_tls,omitempty" json:"kafkaApiTls"`
	AdminApi                             []NamedSocketAddress   `yaml:"admin" mapstructure:"admin" json:"admin"`
	AdminApiTLS                          []ServerTLS            `yaml:"admin_api_tls,omitempty" mapstructure:"admin_api_tls,omitempty" json:"adminApiTls"`
	Id                                   int                    `yaml:"node_id" mapstructure:"node_id" json:"id"`
	SeedServers                          []SeedServer           `yaml:"seed_servers" mapstructure:"seed_servers" json:"seedServers"`
	DeveloperMode                        bool                   `yaml:"developer_mode" mapstructure:"developer_mode" json:"developerMode"`
	CloudStorageApiEndpoint              *string                `yaml:"cloud_storage_api_endpoint,omitempty" mapstructure:"cloud_storage_api_endpoint,omitempty" json:"cloudStorageApiEndpoint,omitempty"`
	CloudStorageEnabled                  *bool                  `yaml:"cloud_storage_enabled,omitempty" mapstructure:"cloud_storage_enabled,omitempty" json:"cloudStorageEnabled,omitempty"`
	CloudStorageAccessKey                *string                `yaml:"cloud_storage_access_key,omitempty" mapstructure:"cloud_storage_access_key,omitempty" json:"cloudStorageAccessKey,omitempty"`
	CloudStorageSecretKey                *string                `yaml:"cloud_storage_secret_key,omitempty" mapstructure:"cloud_storage_secret_key,omitempty" json:"cloudStorageSecretKey,omitempty"`
	CloudStorageRegion                   *string                `yaml:"cloud_storage_region,omitempty" mapstructure:"cloud_storage_region,omitempty" json:"cloudStorageRegion,omitempty"`
	CloudStorageBucket                   *string                `yaml:"cloud_storage_bucket,omitempty" mapstructure:"cloud_storage_bucket,omitempty" json:"cloudStorageBucket,omitempty"`
	CloudStorageReconciliationIntervalMs *int                   `yaml:"cloud_storage_reconciliation_interval_ms,omitempty" mapstructure:"cloud_storage_reconciliation_interval_ms,omitempty" json:"cloudStorageReconciliationIntervalMs,omitempty"`
	CloudStorageMaxConnections           *int                   `yaml:"cloud_storage_max_connections,omitempty" mapstructure:"cloud_storage_max_connections,omitempty" json:"cloudStorageMaxConnections,omitempty"`
	CloudStorageDisableTls               *bool                  `yaml:"cloud_storage_disable_tls,omitempty" mapstructure:"cloud_storage_disable_tls,omitempty" json:"cloudStorageDisableTls,omitempty"`
	CloudStorageApiEndpointPort          *int                   `yaml:"cloud_storage_api_endpoint_port,omitempty" mapstructure:"cloud_storage_api_endpoint_port,omitempty" json:"cloudStorageApiEndpointPort,omitempty"`
	CloudStorageTrustFile                *string                `yaml:"cloud_storage_trust_file,omitempty" mapstructure:"cloud_storage_trust_file,omitempty" json:"cloudStorageTrustFile,omitempty"`
	Superusers                           []string               `yaml:"superusers,omitempty" mapstructure:"superusers,omitempty" json:"superusers,omitempty"`
	EnableSASL                           *bool                  `yaml:"enable_sasl,omitempty" mapstructure:"enable_sasl,omitempty" json:"enableSasl,omitempty"`
	GroupTopicPartitions                 *int                   `yaml:"group_topic_partitions,omitempty" mapstructure:"group_topic_partitions,omitempty" json:"groupTopicPartitions,omitempty"`
	LogSegmentSize                       *int                   `yaml:"log_segment_size,omitempty" mapstructure:"log_segment_size,omitempty" json:"log_segment_size,omitempty"`
	Other                                map[string]interface{} `yaml:",inline" mapstructure:",remain"`
}

type Pandaproxy struct {
	PandaproxyAPI           []NamedSocketAddress   `yaml:"pandaproxy_api,omitempty" mapstructure:"pandaproxy_api,omitempty" json:"pandaproxyApi,omitempty"`
	PandaproxyAPITLS        []ServerTLS            `yaml:"pandaproxy_api_tls,omitempty" mapstructure:"pandaproxy_api_tls,omitempty" json:"pandaproxyApiTls,omitempty"`
	AdvertisedPandaproxyAPI []NamedSocketAddress   `yaml:"advertised_pandaproxy_api,omitempty" mapstructure:"advertised_pandaproxy_api,omitempty" json:"advertisedPandaproxyApi,omitempty"`
	Other                   map[string]interface{} `yaml:",inline" mapstructure:",remain"`
}

type SchemaRegistry struct {
	SchemaRegistryAPI    []NamedSocketAddress `yaml:"schema_registry_api,omitempty" mapstructure:"schema_registry_api,omitempty" json:"schemaRegistryApi,omitempty"`
	SchemaRegistryAPITLS []ServerTLS          `yaml:"schema_registry_api_tls,omitempty" mapstructure:"schema_registry_api_tls,omitempty" json:"schemaRegistryApiTls,omitempty"`
}

type KafkaClient struct {
	Brokers       []SocketAddress        `yaml:"brokers,omitempty" mapstructure:"brokers,omitempty" json:"brokers,omitempty"`
	BrokerTLS     ServerTLS              `yaml:"broker_tls,omitempty" mapstructure:"broker_tls,omitempty" json:"brokerTls,omitempty"`
	SASLMechanism *string                `yaml:"sasl_mechanism,omitempty" mapstructure:"sasl_mechanism,omitempty" json:"saslMechanism,omitempty"`
	SCRAMUsername *string                `yaml:"scram_username,omitempty" mapstructure:"scram_username,omitempty" json:"scramUsername,omitempty"`
	SCRAMPassword *string                `yaml:"scram_password,omitempty" mapstructure:"scram_password,omitempty" json:"scramPassword,omitempty"`
	Other         map[string]interface{} `yaml:",inline" mapstructure:",remain"`
}

type SeedServer struct {
	Host SocketAddress `yaml:"host" mapstructure:"host" json:"host"`
}

type SocketAddress struct {
	Address string `yaml:"address" mapstructure:"address" json:"address"`
	Port    int    `yaml:"port" mapstructure:"port" json:"port"`
}

type NamedSocketAddress struct {
	SocketAddress `yaml:",inline" mapstructure:",squash"`
	Name          string `yaml:"name,omitempty" mapstructure:"name,omitempty" json:"name,omitempty"`
}

type TLS struct {
	KeyFile        string `yaml:"key_file,omitempty" mapstructure:"key_file,omitempty" json:"keyFile"`
	CertFile       string `yaml:"cert_file,omitempty" mapstructure:"cert_file,omitempty" json:"certFile"`
	TruststoreFile string `yaml:"truststore_file,omitempty" mapstructure:"truststore_file,omitempty" json:"truststoreFile"`
}

type ServerTLS struct {
	Name              string `yaml:"name,omitempty" mapstructure:"name,omitempty" json:"name"`
	KeyFile           string `yaml:"key_file,omitempty" mapstructure:"key_file,omitempty" json:"keyFile"`
	CertFile          string `yaml:"cert_file,omitempty" mapstructure:"cert_file,omitempty" json:"certFile"`
	TruststoreFile    string `yaml:"truststore_file,omitempty" mapstructure:"truststore_file,omitempty" json:"truststoreFile"`
	Enabled           bool   `yaml:"enabled,omitempty" mapstructure:"enabled,omitempty" json:"enabled"`
	RequireClientAuth bool   `yaml:"require_client_auth,omitempty" mapstructure:"require_client_auth,omitempty" json:"requireClientAuth"`
}

type RpkConfig struct {
	// Deprecated 2021-07-1
	TLS *TLS `yaml:"tls,omitempty" mapstructure:"tls,omitempty" json:"tls"`
	// Deprecated 2021-07-1
	SASL *SASL `yaml:"sasl,omitempty" mapstructure:"sasl,omitempty" json:"sasl,omitempty"`

	KafkaApi                 RpkKafkaApi `yaml:"kafka_api,omitempty" mapstructure:"kafka_api,omitempty" json:"kafkaApi"`
	AdminApi                 RpkAdminApi `yaml:"admin_api,omitempty" mapstructure:"admin_api,omitempty" json:"adminApi"`
	AdditionalStartFlags     []string    `yaml:"additional_start_flags,omitempty" mapstructure:"additional_start_flags,omitempty" json:"additionalStartFlags"`
	EnableUsageStats         bool        `yaml:"enable_usage_stats" mapstructure:"enable_usage_stats" json:"enableUsageStats"`
	TuneNetwork              bool        `yaml:"tune_network" mapstructure:"tune_network" json:"tuneNetwork"`
	TuneDiskScheduler        bool        `yaml:"tune_disk_scheduler" mapstructure:"tune_disk_scheduler" json:"tuneDiskScheduler"`
	TuneNomerges             bool        `yaml:"tune_disk_nomerges" mapstructure:"tune_disk_nomerges" json:"tuneNomerges"`
	TuneDiskWriteCache       bool        `yaml:"tune_disk_write_cache" mapstructure:"tune_disk_write_cache" json:"tuneDiskWriteCache"`
	TuneDiskIrq              bool        `yaml:"tune_disk_irq" mapstructure:"tune_disk_irq" json:"tuneDiskIrq"`
	TuneFstrim               bool        `yaml:"tune_fstrim" mapstructure:"tune_fstrim" json:"tuneFstrim"`
	TuneCpu                  bool        `yaml:"tune_cpu" mapstructure:"tune_cpu" json:"tuneCpu"`
	TuneAioEvents            bool        `yaml:"tune_aio_events" mapstructure:"tune_aio_events" json:"tuneAioEvents"`
	TuneClocksource          bool        `yaml:"tune_clocksource" mapstructure:"tune_clocksource" json:"tuneClocksource"`
	TuneSwappiness           bool        `yaml:"tune_swappiness" mapstructure:"tune_swappiness" json:"tuneSwappiness"`
	TuneTransparentHugePages bool        `yaml:"tune_transparent_hugepages" mapstructure:"tune_transparent_hugepages" json:"tuneTransparentHugePages"`
	EnableMemoryLocking      bool        `yaml:"enable_memory_locking" mapstructure:"enable_memory_locking" json:"enableMemoryLocking"`
	TuneCoredump             bool        `yaml:"tune_coredump" mapstructure:"tune_coredump" json:"tuneCoredump"`
	CoredumpDir              string      `yaml:"coredump_dir,omitempty" mapstructure:"coredump_dir,omitempty" json:"coredumpDir"`
	WellKnownIo              string      `yaml:"well_known_io,omitempty" mapstructure:"well_known_io,omitempty" json:"wellKnownIo"`
	Overprovisioned          bool        `yaml:"overprovisioned" mapstructure:"overprovisioned" json:"overprovisioned"`
	SMP                      *int        `yaml:"smp,omitempty" mapstructure:"smp,omitempty" json:"smp,omitempty"`
}

type RpkKafkaApi struct {
	Brokers []string `yaml:"brokers,omitempty" mapstructure:"brokers,omitempty" json:"brokers"`
	TLS     *TLS     `yaml:"tls,omitempty" mapstructure:"tls,omitempty" json:"tls"`
	SASL    *SASL    `yaml:"sasl,omitempty" mapstructure:"sasl,omitempty" json:"sasl,omitempty"`
}

type RpkAdminApi struct {
	Addresses []string `yaml:"addresses,omitempty" mapstructure:"addresses,omitempty" json:"addresses"`
	TLS       *TLS     `yaml:"tls,omitempty" mapstructure:"tls,omitempty" json:"tls"`
}

type SASL struct {
	User      string `yaml:"user,omitempty" mapstructure:"user,omitempty" json:"user,omitempty"`
	Password  string `yaml:"password,omitempty" mapstructure:"password,omitempty" json:"password,omitempty"`
	Mechanism string `yaml:"type,omitempty" mapstructure:"type,omitempty" json:"type,omitempty"`
}

func (conf *Config) PIDFile() string {
	return path.Join(conf.Redpanda.Directory, "pid.lock")
}
