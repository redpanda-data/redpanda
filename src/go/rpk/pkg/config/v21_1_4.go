// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package config

import (
	"fmt"
	"strconv"
)

type v2114Config struct {
	NodeUuid	string			`yaml:"node_uuid,omitempty" mapstructure:"node_uuid,omitempty" json:"nodeUuid"`
	Organization	string			`yaml:"organization,omitempty" mapstructure:"organization,omitempty" json:"organization"`
	LicenseKey	string			`yaml:"license_key,omitempty" mapstructure:"license_key,omitempty" json:"licenseKey"`
	ClusterId	string			`yaml:"cluster_id,omitempty" mapstructure:"cluster_id,omitempty" json:"clusterId"`
	ConfigFile	string			`yaml:"config_file" mapstructure:"config_file" json:"configFile"`
	Redpanda	v2114RedpandaConfig	`yaml:"redpanda" mapstructure:"redpanda" json:"redpanda"`
	Rpk		v2114RpkConfig		`yaml:"rpk" mapstructure:"rpk" json:"rpk"`
}

type v2114RedpandaConfig struct {
	Directory		string			`yaml:"data_directory" mapstructure:"data_directory" json:"dataDirectory"`
	RPCServer		v2114SocketAddress	`yaml:"rpc_server" mapstructure:"rpc_server" json:"rpcServer"`
	AdvertisedRPCAPI	*v2114SocketAddress	`yaml:"advertised_rpc_api,omitempty" mapstructure:"advertised_rpc_api,omitempty" json:"advertisedRpcApi,omitempty"`
	KafkaApi		v2114SocketAddress	`yaml:"kafka_api" mapstructure:"kafka_api" json:"kafkaApi"`
	AdvertisedKafkaApi	interface{}		`yaml:"advertised_kafka_api,omitempty" mapstructure:"advertised_kafka_api,omitempty" json:"advertisedKafkaApi,omitempty"`
	KafkaApiTLS		v2114ServerTLS		`yaml:"kafka_api_tls,omitempty" mapstructure:"kafka_api_tls,omitempty" json:"kafkaApiTls"`
	AdminApi		v2114SocketAddress	`yaml:"admin" mapstructure:"admin" json:"admin"`
	Id			int			`yaml:"node_id" mapstructure:"node_id" json:"id"`
	SeedServers		[]v2114SeedServer	`yaml:"seed_servers" mapstructure:"seed_servers" json:"seedServers"`
	DeveloperMode		bool			`yaml:"developer_mode" mapstructure:"developer_mode" json:"developerMode"`
}

type v2114SeedServer struct {
	Host v2114SocketAddress `yaml:"host" mapstructure:"host" json:"host"`
}

type v2114SocketAddress struct {
	Address	string	`yaml:"address" mapstructure:"address" json:"address"`
	Port	int	`yaml:"port" mapstructure:"port" json:"port"`
}

type v2114TLS struct {
	KeyFile		string	`yaml:"key_file,omitempty" mapstructure:"key_file,omitempty" json:"keyFile"`
	CertFile	string	`yaml:"cert_file,omitempty" mapstructure:"cert_file,omitempty" json:"certFile"`
	TruststoreFile	string	`yaml:"truststore_file,omitempty" mapstructure:"truststore_file,omitempty" json:"truststoreFile"`
}

type v2114ServerTLS struct {
	KeyFile		string	`yaml:"key_file,omitempty" mapstructure:"key_file,omitempty" json:"keyFile"`
	CertFile	string	`yaml:"cert_file,omitempty" mapstructure:"cert_file,omitempty" json:"certFile"`
	TruststoreFile	string	`yaml:"truststore_file,omitempty" mapstructure:"truststore_file,omitempty" json:"truststoreFile"`
	Enabled		bool	`yaml:"enabled,omitempty" mapstructure:"enabled,omitempty" json:"enabled"`
}

type v2114RpkConfig struct {
	TLS				v2114TLS	`yaml:"tls,omitempty" mapstructure:"tls,omitempty" json:"tls"`
	AdditionalStartFlags		[]string	`yaml:"additional_start_flags,omitempty" mapstructure:"additional_start_flags,omitempty" json:"additionalStartFlags"`
	EnableUsageStats		bool		`yaml:"enable_usage_stats" mapstructure:"enable_usage_stats" json:"enableUsageStats"`
	TuneNetwork			bool		`yaml:"tune_network" mapstructure:"tune_network" json:"tuneNetwork"`
	TuneDiskScheduler		bool		`yaml:"tune_disk_scheduler" mapstructure:"tune_disk_scheduler" json:"tuneDiskScheduler"`
	TuneNomerges			bool		`yaml:"tune_disk_nomerges" mapstructure:"tune_disk_nomerges" json:"tuneNomerges"`
	TuneDiskWriteCache		bool		`yaml:"tune_disk_write_cache" mapstructure:"tune_disk_write_cache" json:"tuneDiskWriteCache"`
	TuneDiskIrq			bool		`yaml:"tune_disk_irq" mapstructure:"tune_disk_irq" json:"tuneDiskIrq"`
	TuneFstrim			bool		`yaml:"tune_fstrim" mapstructure:"tune_fstrim" json:"tuneFstrim"`
	TuneCpu				bool		`yaml:"tune_cpu" mapstructure:"tune_cpu" json:"tuneCpu"`
	TuneAioEvents			bool		`yaml:"tune_aio_events" mapstructure:"tune_aio_events" json:"tuneAioEvents"`
	TuneClocksource			bool		`yaml:"tune_clocksource" mapstructure:"tune_clocksource" json:"tuneClocksource"`
	TuneSwappiness			bool		`yaml:"tune_swappiness" mapstructure:"tune_swappiness" json:"tuneSwappiness"`
	TuneTransparentHugePages	bool		`yaml:"tune_transparent_hugepages" mapstructure:"tune_transparent_hugepages" json:"tuneTransparentHugePages"`
	EnableMemoryLocking		bool		`yaml:"enable_memory_locking" mapstructure:"enable_memory_locking" json:"enableMemoryLocking"`
	TuneCoredump			bool		`yaml:"tune_coredump" mapstructure:"tune_coredump" json:"tuneCoredump"`
	CoredumpDir			string		`yaml:"coredump_dir,omitempty" mapstructure:"coredump_dir,omitempty" json:"coredumpDir"`
	WellKnownIo			string		`yaml:"well_known_io,omitempty" mapstructure:"well_known_io,omitempty" json:"wellKnownIo"`
	Overprovisioned			bool		`yaml:"overprovisioned" mapstructure:"overprovisioned" json:"overprovisioned"`
	SMP				*int		`yaml:"smp,omitempty" mapstructure:"smp,omitempty" json:"smp,omitempty"`
}

func (c *v2114Config) ToGeneric() (*Config, error) {
	redpandaConf, err := c.Redpanda.toGeneric()
	if err != nil {
		return nil, err
	}
	conf := &Config{
		NodeUuid:	c.NodeUuid,
		Organization:	c.Organization,
		LicenseKey:	c.LicenseKey,
		ClusterId:	c.ClusterId,
		ConfigFile:	c.ConfigFile,
		Redpanda:	*redpandaConf,
		Rpk:		c.Rpk.toGeneric(),
	}
	return conf, nil
}

func (rc v2114RedpandaConfig) toGeneric() (*RedpandaConfig, error) {
	rpcServer := rc.RPCServer.toGeneric()

	var advRpcApi *SocketAddress
	if rc.AdvertisedRPCAPI != nil {
		advRpcApi = rc.AdvertisedRPCAPI.toGeneric()
	}

	kafkaApi := rc.KafkaApi.toGeneric()

	var advKafkaApi *SocketAddress
	if rc.AdvertisedKafkaApi != nil {
		var err error
		advKafkaApi, err = v2114ParsePolymorphicSocketAddress(rc.AdvertisedKafkaApi)
		if err != nil {
			return nil, err
		}
	}

	kafkaApiTls := rc.KafkaApiTLS.toGeneric()

	adminApi := rc.AdminApi.toGeneric()

	var seeds []SeedServer
	if rc.SeedServers != nil {
		for _, s := range rc.SeedServers {
			seeds = append(seeds, s.toGeneric())
		}
	}

	return &RedpandaConfig{
		Directory:		rc.Directory,
		RPCServer:		*rpcServer,
		AdvertisedRPCAPI:	advRpcApi,
		KafkaApi:		*kafkaApi,
		AdvertisedKafkaApi:	advKafkaApi,
		KafkaApiTLS:		kafkaApiTls,
		AdminApi:		*adminApi,
		Id:			rc.Id,
		SeedServers:		seeds,
		DeveloperMode:		rc.DeveloperMode,
	}, nil
}

func (rpk v2114RpkConfig) toGeneric() RpkConfig {
	return RpkConfig{
		TLS:				rpk.TLS.toGeneric(),
		AdditionalStartFlags:		rpk.AdditionalStartFlags,
		EnableUsageStats:		rpk.EnableUsageStats,
		TuneNetwork:			rpk.TuneNetwork,
		TuneDiskScheduler:		rpk.TuneDiskScheduler,
		TuneNomerges:			rpk.TuneNomerges,
		TuneDiskWriteCache:		rpk.TuneDiskWriteCache,
		TuneDiskIrq:			rpk.TuneDiskIrq,
		TuneFstrim:			rpk.TuneFstrim,
		TuneCpu:			rpk.TuneCpu,
		TuneAioEvents:			rpk.TuneAioEvents,
		TuneClocksource:		rpk.TuneClocksource,
		TuneSwappiness:			rpk.TuneSwappiness,
		TuneTransparentHugePages:	rpk.TuneTransparentHugePages,
		EnableMemoryLocking:		rpk.EnableMemoryLocking,
		TuneCoredump:			rpk.TuneCoredump,
		CoredumpDir:			rpk.CoredumpDir,
		WellKnownIo:			rpk.WellKnownIo,
		Overprovisioned:		rpk.Overprovisioned,
		SMP:				rpk.SMP,
	}
}

func (sa *v2114SocketAddress) toGeneric() *SocketAddress {
	return &SocketAddress{
		Address:	sa.Address,
		Port:		sa.Port,
	}
}

func (stls v2114ServerTLS) toGeneric() ServerTLS {
	return ServerTLS{
		KeyFile:	stls.KeyFile,
		CertFile:	stls.CertFile,
		TruststoreFile:	stls.TruststoreFile,
		Enabled:	stls.Enabled,
	}
}

func (tls v2114TLS) toGeneric() TLS {
	return TLS{
		KeyFile:	tls.KeyFile,
		CertFile:	tls.CertFile,
		TruststoreFile:	tls.TruststoreFile,
	}
}

func (s v2114SeedServer) toGeneric() SeedServer {
	host := s.Host.toGeneric()
	return SeedServer{Host: *host}
}

func v2114ParsePolymorphicSocketAddress(v interface{}) (*SocketAddress, error) {
	if v == nil {
		return nil, nil
	}
	basePath := "redpanda.advertised_kafka_api"
	if m, ok := v.(map[string]interface{}); ok {
		return v2114ParseSocketAddressMap(m)
	} else if _, ok := v.([]interface{}); ok {
		// TODO: Support lists
	}
	return nil, fmt.Errorf(
		"couldn't parse '%s'. Its value doesn't match any of"+
			" the supported structures for v21.1.4",
		basePath,
	)
}

func v2114ParseSocketAddressMap(
	m map[string]interface{},
) (*SocketAddress, error) {
	address := ""
	port := 0
	basePath := "redpanda.advertised_kafka_api"
	for k, v := range m {
		switch k {
		case "address":
			addr, ok := v.(string)
			if !ok {
				return nil, mismatchError(
					basePath+".address",
					"a string",
					v,
				)
			}
			address = addr
		case "port":
			var ok bool
			field := basePath + ".port"
			port, ok = v.(int)
			if ok {
				continue
			}
			portStr, ok := v.(string)
			if !ok {
				return nil, mismatchError(
					field,
					"a parseable int",
					v,
				)
			}
			var err error
			port, err = strconv.Atoi(portStr)
			if err != nil {
				return nil, mismatchError(
					field,
					"a parseable int",
					v,
				)
			}
		}
	}
	return &SocketAddress{Address: address, Port: port}, nil
}

func mismatchError(
	field string, expectedType string, actual interface{},
) error {
	return fmt.Errorf(
		"expected value for '%s' to be %s, but found %v instead.",
		field,
		expectedType,
		actual,
	)
}
