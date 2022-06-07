// Copyright 2020 Redpanda Data, Inc.
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
	file       *Config
	loadedPath string

	NodeUUID             string          `yaml:"node_uuid,omitempty" json:"nodeUuid"`
	Organization         string          `yaml:"organization,omitempty" json:"organization"`
	LicenseKey           string          `yaml:"license_key,omitempty" json:"licenseKey"`
	ClusterID            string          `yaml:"cluster_id,omitempty" json:"clusterId"`
	ConfigFile           string          `yaml:"config_file" json:"configFile"`
	Redpanda             RedpandaConfig  `yaml:"redpanda" json:"redpanda"`
	Rpk                  RpkConfig       `yaml:"rpk" json:"rpk"`
	Pandaproxy           *Pandaproxy     `yaml:"pandaproxy,omitempty" json:"pandaproxy,omitempty"`
	PandaproxyClient     *KafkaClient    `yaml:"pandaproxy_client,omitempty" json:"pandaproxyClient,omitempty"`
	SchemaRegistry       *SchemaRegistry `yaml:"schema_registry,omitempty" json:"schemaRegistry,omitempty"`
	SchemaRegistryClient *KafkaClient    `yaml:"schema_registry_client,omitempty" json:"schemaRegistryClient,omitempty"`

	Other map[string]interface{} `yaml:",inline"`
}

// File returns the configuration as read from a file, with no defaults
// pre-deserializing and no overrides applied after. If the return is nil,
// no file was read.
func (c *Config) File() *Config {
	return c.file
}

type RedpandaConfig struct {
	Directory                  string                 `yaml:"data_directory" json:"dataDirectory"`
	ID                         int                    `yaml:"node_id" json:"id"`
	Rack                       string                 `yaml:"rack,omitempty" json:"rack"`
	SeedServers                []SeedServer           `yaml:"seed_servers" json:"seedServers"`
	RPCServer                  SocketAddress          `yaml:"rpc_server" json:"rpcServer"`
	RPCServerTLS               []ServerTLS            `yaml:"rpc_server_tls,omitempty" json:"rpcServerTls"`
	KafkaAPI                   []NamedSocketAddress   `yaml:"kafka_api" json:"kafkaApi"`
	KafkaAPITLS                []ServerTLS            `yaml:"kafka_api_tls,omitempty" json:"kafkaApiTls"`
	AdminAPI                   []NamedSocketAddress   `yaml:"admin" json:"admin"`
	AdminAPITLS                []ServerTLS            `yaml:"admin_api_tls,omitempty" json:"adminApiTls"`
	CoprocSupervisorServer     SocketAddress          `yaml:"coproc_supervisor_server,omitempty" json:"coprocSupervisorServer"`
	AdminAPIDocDir             string                 `yaml:"admin_api_doc_dir,omitempty" json:"adminApiDocDir"`
	DashboardDir               string                 `yaml:"dashboard_dir,omitempty" json:"dashboardDir"`
	CloudStorageCacheDirectory string                 `yaml:"cloud_storage_cache_directory,omitempty" json:"CloudStorageCacheDirectory"`
	AdvertisedRPCAPI           *SocketAddress         `yaml:"advertised_rpc_api,omitempty" json:"advertisedRpcApi,omitempty"`
	AdvertisedKafkaAPI         []NamedSocketAddress   `yaml:"advertised_kafka_api,omitempty" json:"advertisedKafkaApi,omitempty"`
	DeveloperMode              bool                   `yaml:"developer_mode" json:"developerMode"`
	Other                      map[string]interface{} `yaml:",inline"`
}

type Pandaproxy struct {
	PandaproxyAPI           []NamedSocketAddress   `yaml:"pandaproxy_api,omitempty" json:"pandaproxyApi,omitempty"`
	PandaproxyAPITLS        []ServerTLS            `yaml:"pandaproxy_api_tls,omitempty" json:"pandaproxyApiTls,omitempty"`
	AdvertisedPandaproxyAPI []NamedSocketAddress   `yaml:"advertised_pandaproxy_api,omitempty" json:"advertisedPandaproxyApi,omitempty"`
	Other                   map[string]interface{} `yaml:",inline"`
}

type SchemaRegistry struct {
	SchemaRegistryAPI               []NamedSocketAddress `yaml:"schema_registry_api,omitempty" json:"schemaRegistryApi,omitempty"`
	SchemaRegistryAPITLS            []ServerTLS          `yaml:"schema_registry_api_tls,omitempty" json:"schemaRegistryApiTls,omitempty"`
	SchemaRegistryReplicationFactor *int                 `yaml:"schema_registry_replication_factor,omitempty" json:"schemaRegistryReplicationFactor,omitempty"`
}

type KafkaClient struct {
	Brokers       []SocketAddress        `yaml:"brokers,omitempty" json:"brokers,omitempty"`
	BrokerTLS     ServerTLS              `yaml:"broker_tls,omitempty" json:"brokerTls,omitempty"`
	SASLMechanism *string                `yaml:"sasl_mechanism,omitempty" json:"saslMechanism,omitempty"`
	SCRAMUsername *string                `yaml:"scram_username,omitempty" json:"scramUsername,omitempty"`
	SCRAMPassword *string                `yaml:"scram_password,omitempty" json:"scramPassword,omitempty"`
	Other         map[string]interface{} `yaml:",inline"`
}

type SeedServer struct {
	Host SocketAddress `yaml:"host" json:"host"`
}

type SocketAddress struct {
	Address string `yaml:"address" json:"address"`
	Port    int    `yaml:"port" json:"port"`
}

type NamedSocketAddress struct {
	Address string `yaml:"address" json:"address"`
	Port    int    `yaml:"port" json:"port"`
	Name    string `yaml:"name,omitempty" json:"name,omitempty"`
}

type TLS struct {
	KeyFile        string `yaml:"key_file,omitempty" json:"keyFile"`
	CertFile       string `yaml:"cert_file,omitempty" json:"certFile"`
	TruststoreFile string `yaml:"truststore_file,omitempty" json:"truststoreFile"`
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
	Name              string                 `yaml:"name,omitempty" json:"name"`
	KeyFile           string                 `yaml:"key_file,omitempty" json:"keyFile"`
	CertFile          string                 `yaml:"cert_file,omitempty" json:"certFile"`
	TruststoreFile    string                 `yaml:"truststore_file,omitempty" json:"truststoreFile"`
	Enabled           bool                   `yaml:"enabled,omitempty" json:"enabled"`
	RequireClientAuth bool                   `yaml:"require_client_auth,omitempty" json:"requireClientAuth"`
	Other             map[string]interface{} `yaml:",inline" `
}

type RpkConfig struct {
	// Deprecated 2021-07-1
	TLS *TLS `yaml:"tls,omitempty" json:"tls"`
	// Deprecated 2021-07-1
	SASL *SASL `yaml:"sasl,omitempty" json:"sasl,omitempty"`

	KafkaAPI                 RpkKafkaAPI `yaml:"kafka_api,omitempty" json:"kafkaApi"`
	AdminAPI                 RpkAdminAPI `yaml:"admin_api,omitempty" json:"adminApi"`
	AdditionalStartFlags     []string    `yaml:"additional_start_flags,omitempty"  json:"additionalStartFlags"`
	EnableUsageStats         bool        `yaml:"enable_usage_stats" json:"enableUsageStats"`
	TuneNetwork              bool        `yaml:"tune_network" json:"tuneNetwork"`
	TuneDiskScheduler        bool        `yaml:"tune_disk_scheduler" json:"tuneDiskScheduler"`
	TuneNomerges             bool        `yaml:"tune_disk_nomerges" json:"tuneNomerges"`
	TuneDiskWriteCache       bool        `yaml:"tune_disk_write_cache" json:"tuneDiskWriteCache"`
	TuneDiskIrq              bool        `yaml:"tune_disk_irq" json:"tuneDiskIrq"`
	TuneFstrim               bool        `yaml:"tune_fstrim" json:"tuneFstrim"`
	TuneCPU                  bool        `yaml:"tune_cpu" json:"tuneCpu"`
	TuneAioEvents            bool        `yaml:"tune_aio_events" json:"tuneAioEvents"`
	TuneClocksource          bool        `yaml:"tune_clocksource" json:"tuneClocksource"`
	TuneSwappiness           bool        `yaml:"tune_swappiness" json:"tuneSwappiness"`
	TuneTransparentHugePages bool        `yaml:"tune_transparent_hugepages" json:"tuneTransparentHugePages"`
	EnableMemoryLocking      bool        `yaml:"enable_memory_locking" json:"enableMemoryLocking"`
	TuneCoredump             bool        `yaml:"tune_coredump" json:"tuneCoredump"`
	CoredumpDir              string      `yaml:"coredump_dir,omitempty" json:"coredumpDir"`
	TuneBallastFile          bool        `yaml:"tune_ballast_file" json:"tuneBallastFile"`
	BallastFilePath          string      `yaml:"ballast_file_path,omitempty" json:"ballastFilePath"`
	BallastFileSize          string      `yaml:"ballast_file_size,omitempty" json:"ballastFileSize"`
	WellKnownIo              string      `yaml:"well_known_io,omitempty" json:"wellKnownIo"`
	Overprovisioned          bool        `yaml:"overprovisioned" json:"overprovisioned"`
	SMP                      *int        `yaml:"smp,omitempty" json:"smp,omitempty"`
}

type RpkKafkaAPI struct {
	Brokers []string `yaml:"brokers,omitempty" json:"brokers"`
	TLS     *TLS     `yaml:"tls,omitempty" json:"tls"`
	SASL    *SASL    `yaml:"sasl,omitempty" json:"sasl,omitempty"`
}

type RpkAdminAPI struct {
	Addresses []string `yaml:"addresses,omitempty" json:"addresses"`
	TLS       *TLS     `yaml:"tls,omitempty" json:"tls"`
}

type SASL struct {
	User      string `yaml:"user,omitempty" json:"user,omitempty"`
	Password  string `yaml:"password,omitempty" json:"password,omitempty"`
	Mechanism string `yaml:"type,omitempty" json:"type,omitempty"`
}

func (c *Config) PIDFile() string {
	return path.Join(c.Redpanda.Directory, "pid.lock")
}
