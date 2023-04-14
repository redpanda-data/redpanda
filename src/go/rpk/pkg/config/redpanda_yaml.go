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

type (
	Config struct {
		file         *Config
		fileLocation string
		rawFile      []byte

		Redpanda             RedpandaNodeConfig `yaml:"redpanda,omitempty" json:"redpanda"`
		Rpk                  RpkNodeConfig      `yaml:"rpk,omitempty" json:"rpk"`
		Pandaproxy           *Pandaproxy        `yaml:"pandaproxy,omitempty" json:"pandaproxy,omitempty"`
		PandaproxyClient     *KafkaClient       `yaml:"pandaproxy_client,omitempty" json:"pandaproxy_client,omitempty"`
		SchemaRegistry       *SchemaRegistry    `yaml:"schema_registry,omitempty" json:"schema_registry,omitempty"`
		SchemaRegistryClient *KafkaClient       `yaml:"schema_registry_client,omitempty" json:"schema_registry_client,omitempty"`

		Other map[string]interface{} `yaml:",inline"`
	}

	// RedpandaNodeConfig is the source of truth for Redpanda node configuration.
	//
	// Cluster properties must NOT be enlisted in this struct. Adding a cluster
	// property here would cause the dependent libraries (e.g. operator) to wrongly
	// consider it a node property.
	RedpandaNodeConfig struct {
		Directory                  string                    `yaml:"data_directory,omitempty" json:"data_directory"`
		ID                         *int                      `yaml:"node_id,omitempty" json:"node_id,omitempty"`
		Rack                       string                    `yaml:"rack,omitempty" json:"rack"`
		EmptySeedStartsCluster     *bool                     `yaml:"empty_seed_starts_cluster,omitempty" json:"empty_seed_starts_cluster,omitempty"`
		SeedServers                []SeedServer              `yaml:"seed_servers" json:"seed_servers"`
		RPCServer                  SocketAddress             `yaml:"rpc_server,omitempty" json:"rpc_server"`
		KafkaAPI                   []NamedAuthNSocketAddress `yaml:"kafka_api,omitempty" json:"kafka_api"`
		KafkaAPITLS                []ServerTLS               `yaml:"kafka_api_tls,omitempty" json:"kafka_api_tls"`
		AdminAPI                   []NamedSocketAddress      `yaml:"admin,omitempty" json:"admin"`
		AdminAPITLS                []ServerTLS               `yaml:"admin_api_tls,omitempty" json:"admin_api_tls"`
		CoprocSupervisorServer     SocketAddress             `yaml:"coproc_supervisor_server,omitempty" json:"coproc_supervisor_server"`
		AdminAPIDocDir             string                    `yaml:"admin_api_doc_dir,omitempty" json:"admin_api_doc_dir"`
		DashboardDir               string                    `yaml:"dashboard_dir,omitempty" json:"dashboard_dir"`
		CloudStorageCacheDirectory string                    `yaml:"cloud_storage_cache_directory,omitempty" json:"cloud_storage_cache_directory"`
		AdvertisedRPCAPI           *SocketAddress            `yaml:"advertised_rpc_api,omitempty" json:"advertised_rpc_api,omitempty"`
		AdvertisedKafkaAPI         []NamedSocketAddress      `yaml:"advertised_kafka_api,omitempty" json:"advertised_kafka_api,omitempty"`
		DeveloperMode              bool                      `yaml:"developer_mode,omitempty" json:"developer_mode"`
		CrashLoopLimit             *int                      `yaml:"crash_loop_limit,omitempty" json:"crash_loop_limit"`
		Other                      map[string]interface{}    `yaml:",inline"`
	}

	Pandaproxy struct {
		PandaproxyAPI           []NamedAuthNSocketAddress `yaml:"pandaproxy_api,omitempty" json:"pandaproxy_api,omitempty"`
		PandaproxyAPITLS        []ServerTLS               `yaml:"pandaproxy_api_tls,omitempty" json:"pandaproxy_api_tls,omitempty"`
		AdvertisedPandaproxyAPI []NamedSocketAddress      `yaml:"advertised_pandaproxy_api,omitempty" json:"advertised_pandaproxy_api,omitempty"`
		Other                   map[string]interface{}    `yaml:",inline"`
	}

	SchemaRegistry struct {
		SchemaRegistryAPI               []NamedAuthNSocketAddress `yaml:"schema_registry_api,omitempty" json:"schema_registry_api,omitempty"`
		SchemaRegistryAPITLS            []ServerTLS               `yaml:"schema_registry_api_tls,omitempty" json:"schema_registry_api_tls,omitempty"`
		SchemaRegistryReplicationFactor *int                      `yaml:"schema_registry_replication_factor,omitempty" json:"schema_registry_replication_factor,omitempty"`
	}

	KafkaClient struct {
		Brokers       []SocketAddress        `yaml:"brokers,omitempty" json:"brokers,omitempty"`
		BrokerTLS     ServerTLS              `yaml:"broker_tls,omitempty" json:"broker_tls,omitempty"`
		SASLMechanism *string                `yaml:"sasl_mechanism,omitempty" json:"sasl_mechanism,omitempty"`
		SCRAMUsername *string                `yaml:"scram_username,omitempty" json:"scram_username,omitempty"`
		SCRAMPassword *string                `yaml:"scram_password,omitempty" json:"scram_password,omitempty"`
		Other         map[string]interface{} `yaml:",inline"`
	}

	SeedServer struct {
		Host SocketAddress `yaml:"host,omitempty" json:"host"`

		// The SeedServer in older versions of redpanda was untabbed, but we support
		// these older versions using a custom unmarshaller. We track whether the
		// SeedServer field has been modified from the older version using this
		// unexported field.
		//
		// See see github.com/redpanda-data/redpanda/issues/8915.
		untabbed bool
	}

	SocketAddress struct {
		Address string `yaml:"address" json:"address"`
		Port    int    `yaml:"port,omitempty" json:"port"`
	}

	NamedSocketAddress struct {
		Address string `yaml:"address" json:"address"`
		Port    int    `yaml:"port,omitempty" json:"port"`
		Name    string `yaml:"name,omitempty" json:"name,omitempty"`
	}

	NamedAuthNSocketAddress struct {
		Address string  `yaml:"address,omitempty" json:"address"`
		Port    int     `yaml:"port,omitempty" json:"port"`
		Name    string  `yaml:"name,omitempty" json:"name,omitempty"`
		AuthN   *string `yaml:"authentication_method,omitempty" json:"authentication_method,omitempty"`
	}

	TLS struct {
		KeyFile        string `yaml:"key_file,omitempty" json:"key_file"`
		CertFile       string `yaml:"cert_file,omitempty" json:"cert_file"`
		TruststoreFile string `yaml:"truststore_file,omitempty" json:"truststore_file"`
	}

	ServerTLS struct {
		Name              string                 `yaml:"name,omitempty" json:"name"`
		KeyFile           string                 `yaml:"key_file,omitempty" json:"key_file"`
		CertFile          string                 `yaml:"cert_file,omitempty" json:"cert_file"`
		TruststoreFile    string                 `yaml:"truststore_file,omitempty" json:"truststore_file"`
		Enabled           bool                   `yaml:"enabled,omitempty" json:"enabled"`
		RequireClientAuth bool                   `yaml:"require_client_auth,omitempty" json:"require_client_auth"`
		Other             map[string]interface{} `yaml:",inline" `
	}

	RpkNodeConfig struct {
		// Deprecated 2021-07-1
		TLS *TLS `yaml:"tls,omitempty" json:"tls"`
		// Deprecated 2021-07-1
		SASL *SASL `yaml:"sasl,omitempty" json:"sasl,omitempty"`

		KafkaAPI             RpkKafkaAPI `yaml:"kafka_api,omitempty" json:"kafka_api"`
		AdminAPI             RpkAdminAPI `yaml:"admin_api,omitempty" json:"admin_api"`
		AdditionalStartFlags []string    `yaml:"additional_start_flags,omitempty"  json:"additional_start_flags"`

		Tuners RpkNodeTuners `yaml:",inline"`
	}

	RpkNodeTuners struct {
		TuneNetwork              bool   `yaml:"tune_network,omitempty" json:"tune_network"`
		TuneDiskScheduler        bool   `yaml:"tune_disk_scheduler,omitempty" json:"tune_disk_scheduler"`
		TuneNomerges             bool   `yaml:"tune_disk_nomerges,omitempty" json:"tune_disk_nomerges"`
		TuneDiskWriteCache       bool   `yaml:"tune_disk_write_cache,omitempty" json:"tune_disk_write_cache"`
		TuneDiskIrq              bool   `yaml:"tune_disk_irq,omitempty" json:"tune_disk_irq"`
		TuneFstrim               bool   `yaml:"tune_fstrim,omitempty" json:"tune_fstrim"`
		TuneCPU                  bool   `yaml:"tune_cpu,omitempty" json:"tune_cpu"`
		TuneAioEvents            bool   `yaml:"tune_aio_events,omitempty" json:"tune_aio_events"`
		TuneClocksource          bool   `yaml:"tune_clocksource,omitempty" json:"tune_clocksource"`
		TuneSwappiness           bool   `yaml:"tune_swappiness,omitempty" json:"tune_swappiness"`
		TuneTransparentHugePages bool   `yaml:"tune_transparent_hugepages,omitempty" json:"tune_transparent_hugepages"`
		EnableMemoryLocking      bool   `yaml:"enable_memory_locking,omitempty" json:"enable_memory_locking"`
		TuneCoredump             bool   `yaml:"tune_coredump,omitempty" json:"tune_coredump"`
		CoredumpDir              string `yaml:"coredump_dir,omitempty" json:"coredump_dir"`
		TuneBallastFile          bool   `yaml:"tune_ballast_file,omitempty" json:"tune_ballast_file"`
		BallastFilePath          string `yaml:"ballast_file_path,omitempty" json:"ballast_file_path"`
		BallastFileSize          string `yaml:"ballast_file_size,omitempty" json:"ballast_file_size"`
		WellKnownIo              string `yaml:"well_known_io,omitempty" json:"well_known_io"`
		Overprovisioned          bool   `yaml:"overprovisioned,omitempty" json:"overprovisioned"`
		SMP                      *int   `yaml:"smp,omitempty" json:"smp,omitempty"`
	}

	RpkKafkaAPI struct {
		Brokers []string `yaml:"brokers,omitempty" json:"brokers"`
		TLS     *TLS     `yaml:"tls,omitempty" json:"tls"`
		SASL    *SASL    `yaml:"sasl,omitempty" json:"sasl,omitempty"`
	}

	RpkAdminAPI struct {
		Addresses []string `yaml:"addresses,omitempty" json:"addresses"`
		TLS       *TLS     `yaml:"tls,omitempty" json:"tls"`
	}

	SASL struct {
		User      string `yaml:"user,omitempty" json:"user,omitempty"`
		Password  string `yaml:"password,omitempty" json:"password,omitempty"`
		Mechanism string `yaml:"type,omitempty" json:"type,omitempty"`
	}
)

// File returns the configuration as read from a file, with no defaults
// pre-deserializing and no overrides applied after. If the return is nil,
// no file was read.
func (c *Config) File() *Config {
	return c.file
}

// RawFile returns the bytes of the actual file on disk, if there was one.
// The raw bytes must be valid yaml.
func (c *Config) RawFile() []byte {
	return c.rawFile
}

// FileLocation returns the loaded file location; this is the path that
// rpk uses for write operations.
func (c *Config) FileLocation() string {
	return c.fileLocation
}

func (c *Config) PIDFile() string {
	return path.Join(c.Redpanda.Directory, "pid.lock")
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

func namedAuthnToNamed(src []NamedAuthNSocketAddress) []NamedSocketAddress {
	dst := make([]NamedSocketAddress, 0, len(src))
	for _, a := range src {
		dst = append(dst, NamedSocketAddress{
			Address: a.Address,
			Port:    a.Port,
			Name:    a.Name,
		})
	}
	return dst
}
