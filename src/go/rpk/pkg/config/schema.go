// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package config

import (
	"crypto/tls"
	"path"

	"github.com/spf13/afero"
	"github.com/twmb/tlscfg"
)

type Config struct {
	file *Config

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

// File returns the configuration as read from a file, with no defaults
// pre-deserializing and no overrides applied after. If the return is nil,
// no file was read.
func (c *Config) File() *Config {
	return c.file
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
	Other                                map[string]interface{} `yaml:",inline" mapstructure:",remain"`
}

type Pandaproxy struct {
	PandaproxyAPI           []NamedSocketAddress   `yaml:"pandaproxy_api,omitempty" mapstructure:"pandaproxy_api,omitempty" json:"pandaproxyApi,omitempty"`
	PandaproxyAPITLS        []ServerTLS            `yaml:"pandaproxy_api_tls,omitempty" mapstructure:"pandaproxy_api_tls,omitempty" json:"pandaproxyApiTls,omitempty"`
	AdvertisedPandaproxyAPI []NamedSocketAddress   `yaml:"advertised_pandaproxy_api,omitempty" mapstructure:"advertised_pandaproxy_api,omitempty" json:"advertisedPandaproxyApi,omitempty"`
	Other                   map[string]interface{} `yaml:",inline" mapstructure:",remain"`
}

type SchemaRegistry struct {
	SchemaRegistryAPI               []NamedSocketAddress `yaml:"schema_registry_api,omitempty" mapstructure:"schema_registry_api,omitempty" json:"schemaRegistryApi,omitempty"`
	SchemaRegistryAPITLS            []ServerTLS          `yaml:"schema_registry_api_tls,omitempty" mapstructure:"schema_registry_api_tls,omitempty" json:"schemaRegistryApiTls,omitempty"`
	SchemaRegistryReplicationFactor *int                 `yaml:"schema_registry_replication_factor,omitempty" mapstructure:"schema_registry_replication_factor,omitempty" json:"schemaRegistryReplicationFactor,omitempty"`
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

func (t *TLS) Config(fs afero.Fs) (*tls.Config, error) {
	if t == nil {
		return nil, nil
	}
	return tlscfg.New(
		tlscfg.WithFS(
			tlscfg.FuncFS(func(path string) ([]byte, error) {
				return afero.ReadFile(fs, path)
			}),
		),
		tlscfg.MaybeWithDiskCA(
			t.TruststoreFile,
			tlscfg.ForClient,
		),
		tlscfg.MaybeWithDiskKeyPair(
			t.CertFile,
			t.KeyFile,
		),
	)
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
	TuneBallastFile          bool        `yaml:"tune_ballast_file" mapstructure:"tune_ballast_file" json:"tuneBallastFile"`
	BallastFilePath          string      `yaml:"ballast_file_path,omitempty" mapstructure:"ballast_file_path,omitempty" json:"ballastFilePath"`
	BallastFileSize          string      `yaml:"ballast_file_size,omitempty" mapstructure:"ballast_file_size,omitempty" json:"ballastFileSize"`
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
