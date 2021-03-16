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
	NodeUuid     string                 `yaml:"node_uuid,omitempty" mapstructure:"node_uuid,omitempty" json:"nodeUuid"`
	Organization string                 `yaml:"organization,omitempty" mapstructure:"organization,omitempty" json:"organization"`
	LicenseKey   string                 `yaml:"license_key,omitempty" mapstructure:"license_key,omitempty" json:"licenseKey"`
	ClusterId    string                 `yaml:"cluster_id,omitempty" mapstructure:"cluster_id,omitempty" json:"clusterId"`
	ConfigFile   string                 `yaml:"config_file" mapstructure:"config_file" json:"configFile"`
	Redpanda     RedpandaConfig         `yaml:"redpanda" mapstructure:"redpanda" json:"redpanda"`
	Rpk          RpkConfig              `yaml:"rpk" mapstructure:"rpk" json:"rpk"`
	Other        map[string]interface{} `yaml:",inline" mapstructure:",remain"`
}

type RedpandaConfig struct {
	Directory          string                 `yaml:"data_directory" mapstructure:"data_directory" json:"dataDirectory"`
	RPCServer          SocketAddress          `yaml:"rpc_server" mapstructure:"rpc_server" json:"rpcServer"`
	AdvertisedRPCAPI   *SocketAddress         `yaml:"advertised_rpc_api,omitempty" mapstructure:"advertised_rpc_api,omitempty" json:"advertisedRpcApi,omitempty"`
	KafkaApi           []NamedSocketAddress   `yaml:"kafka_api" mapstructure:"kafka_api" json:"kafkaApi"`
	AdvertisedKafkaApi []NamedSocketAddress   `yaml:"advertised_kafka_api,omitempty" mapstructure:"advertised_kafka_api,omitempty" json:"advertisedKafkaApi,omitempty"`
	KafkaApiTLS        ServerTLS              `yaml:"kafka_api_tls,omitempty" mapstructure:"kafka_api_tls,omitempty" json:"kafkaApiTls"`
	AdminApi           SocketAddress          `yaml:"admin" mapstructure:"admin" json:"admin"`
	Id                 int                    `yaml:"node_id" mapstructure:"node_id" json:"id"`
	SeedServers        []SeedServer           `yaml:"seed_servers" mapstructure:"seed_servers" json:"seedServers"`
	DeveloperMode      bool                   `yaml:"developer_mode" mapstructure:"developer_mode" json:"developerMode"`
	Other              map[string]interface{} `yaml:",inline" mapstructure:",remain"`
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
	KeyFile           string `yaml:"key_file,omitempty" mapstructure:"key_file,omitempty" json:"keyFile"`
	CertFile          string `yaml:"cert_file,omitempty" mapstructure:"cert_file,omitempty" json:"certFile"`
	TruststoreFile    string `yaml:"truststore_file,omitempty" mapstructure:"truststore_file,omitempty" json:"truststoreFile"`
	Enabled           bool   `yaml:"enabled,omitempty" mapstructure:"enabled,omitempty" json:"enabled"`
	RequireClientAuth bool   `yaml:"require_client_auth,omitempty" mapstructure:"require_client_auth,omitempty" json:"requireClientAuth"`
}

type RpkConfig struct {
	TLS                      TLS      `yaml:"tls,omitempty" mapstructure:"tls,omitempty" json:"tls"`
	AdditionalStartFlags     []string `yaml:"additional_start_flags,omitempty" mapstructure:"additional_start_flags,omitempty" json:"additionalStartFlags"`
	EnableUsageStats         bool     `yaml:"enable_usage_stats" mapstructure:"enable_usage_stats" json:"enableUsageStats"`
	TuneNetwork              bool     `yaml:"tune_network" mapstructure:"tune_network" json:"tuneNetwork"`
	TuneDiskScheduler        bool     `yaml:"tune_disk_scheduler" mapstructure:"tune_disk_scheduler" json:"tuneDiskScheduler"`
	TuneNomerges             bool     `yaml:"tune_disk_nomerges" mapstructure:"tune_disk_nomerges" json:"tuneNomerges"`
	TuneDiskWriteCache       bool     `yaml:"tune_disk_write_cache" mapstructure:"tune_disk_write_cache" json:"tuneDiskWriteCache"`
	TuneDiskIrq              bool     `yaml:"tune_disk_irq" mapstructure:"tune_disk_irq" json:"tuneDiskIrq"`
	TuneFstrim               bool     `yaml:"tune_fstrim" mapstructure:"tune_fstrim" json:"tuneFstrim"`
	TuneCpu                  bool     `yaml:"tune_cpu" mapstructure:"tune_cpu" json:"tuneCpu"`
	TuneAioEvents            bool     `yaml:"tune_aio_events" mapstructure:"tune_aio_events" json:"tuneAioEvents"`
	TuneClocksource          bool     `yaml:"tune_clocksource" mapstructure:"tune_clocksource" json:"tuneClocksource"`
	TuneSwappiness           bool     `yaml:"tune_swappiness" mapstructure:"tune_swappiness" json:"tuneSwappiness"`
	TuneTransparentHugePages bool     `yaml:"tune_transparent_hugepages" mapstructure:"tune_transparent_hugepages" json:"tuneTransparentHugePages"`
	EnableMemoryLocking      bool     `yaml:"enable_memory_locking" mapstructure:"enable_memory_locking" json:"enableMemoryLocking"`
	TuneCoredump             bool     `yaml:"tune_coredump" mapstructure:"tune_coredump" json:"tuneCoredump"`
	CoredumpDir              string   `yaml:"coredump_dir,omitempty" mapstructure:"coredump_dir,omitempty" json:"coredumpDir"`
	WellKnownIo              string   `yaml:"well_known_io,omitempty" mapstructure:"well_known_io,omitempty" json:"wellKnownIo"`
	Overprovisioned          bool     `yaml:"overprovisioned" mapstructure:"overprovisioned" json:"overprovisioned"`
	SMP                      *int     `yaml:"smp,omitempty" mapstructure:"smp,omitempty" json:"smp,omitempty"`
}

func (conf *Config) PIDFile() string {
	return path.Join(conf.Redpanda.Directory, "pid.lock")
}
