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
	"errors"
	"fmt"
	"reflect"
	"strconv"

	"gopkg.in/yaml.v3"
)

// This file contains weak params type, including basic types support (bool,
// int, and string) and one_or_many support for different types.
//
// The use of this file is to support our transition to a strongly typed
// config file and our migration away from viper and mapstructure.
// TODO: Print deprecation warning when using weak types https://github.com/redpanda-data/redpanda/issues/5262

// weakBool is an intermediary boolean type to be used during our transition
// to strictly typed configuration parameters. This will allow us to support
// weakly typed parsing:
//
//   - int to bool (true if value != 0)
//   - string to bool (accepts: 1, t, T, TRUE, true, True, 0, f, F, FALSE,
//     false, False. Anything else is an error)
type weakBool bool

func (wb *weakBool) UnmarshalYAML(n *yaml.Node) error {
	switch n.Tag {
	case "!!bool":
		b, err := strconv.ParseBool(n.Value)
		if err != nil {
			return err
		}
		*wb = weakBool(b)
		return nil
	case "!!int":
		ni, err := strconv.Atoi(n.Value)
		if err != nil {
			return fmt.Errorf("cannot parse '%s' as bool: %s", n.Value, err)
		}
		*wb = ni != 0
		return nil
	case "!!str":
		// it accepts 1, t, T, TRUE, true, True, 0, f, F
		nb, err := strconv.ParseBool(n.Value)
		if err == nil {
			*wb = weakBool(nb)
			return nil
		} else if n.Value == "" {
			*wb = false
			return nil
		} else {
			return fmt.Errorf("cannot parse '%s' as bool: %s", n.Value, err)
		}
	default:
		return fmt.Errorf("type %s not supported as a boolean", n.Tag)
	}
}

// weakInt is an intermediary integer type to be used during our transition to
// strictly typed configuration parameters. This will allow us to support
// weakly typed parsing:
//
//   - strings to int/uint (base implied by prefix)
//   - bools to int/uint (true = 1, false = 0)
type weakInt int

func (wi *weakInt) UnmarshalYAML(n *yaml.Node) error {
	switch n.Tag {
	case "!!int":
		ni, err := strconv.Atoi(n.Value)
		if err != nil {
			return err
		}
		*wi = weakInt(ni)
		return nil
	case "!!str":
		str := n.Value
		if str == "" {
			str = "0"
		}
		ni, err := strconv.Atoi(str)
		if err != nil {
			return fmt.Errorf("cannot parse '%s' as an integer: %s", str, err)
		}
		*wi = weakInt(ni)
		return nil
	case "!!bool":
		nb, err := strconv.ParseBool(n.Value)
		if err != nil {
			return fmt.Errorf("cannot parse '%s' as an integer: %s", n.Value, err)
		}
		if nb {
			*wi = 1
			return nil
		}
		*wi = 0
		return nil
	default:
		return fmt.Errorf("type %s not supported as an integer", n.Tag)
	}
}

// weakString is an intermediary string type to be used during our transition to
// strictly typed configuration parameters. This will allow us to support
// weakly typed parsing:
//
//   - bools to string (true = "1", false = "0")
//   - numbers to string (base 10)
type weakString string

func (ws *weakString) UnmarshalYAML(n *yaml.Node) error {
	switch n.Tag {
	case "!!str":
		*ws = weakString(n.Value)
		return nil
	case "!!bool":
		nb, err := strconv.ParseBool(n.Value)
		if err != nil {
			return fmt.Errorf("cannot parse '%s' as a boolean: %s", n.Value, err)
		}
		if nb {
			*ws = "1"
			return nil
		}
		*ws = "0"
		return nil
	case "!!int", "!!float":
		*ws = weakString(n.Value)
		return nil
	default:
		return fmt.Errorf("type %s not supported as a string", n.Tag)
	}
}

// weakStringArray is an intermediary one_or_many type to be used
// during our transition to strictly typed configuration parameters.
// This type will:
//   - parse an array of strings
//   - parse a single string to an array.
type weakStringArray []string

func (wsa *weakStringArray) UnmarshalYAML(n *yaml.Node) error {
	var multi []weakString
	err := n.Decode(&multi)
	if err == nil {
		s := make([]string, len(multi))
		for i, v := range multi {
			s[i] = string(v)
		}
		*wsa = s
		return nil
	}

	var single weakString
	err = n.Decode(&single)
	if err != nil {
		return err
	}
	*wsa = []string{string(single)}
	return nil
}

// socketAddresses is an intermediary one_or_many type to be used
// during our transition to strictly typed configuration parameters.
// This type will:
//   - parse an array of SocketAddress
//   - parse a single SocketAddress to an array.
type socketAddresses []SocketAddress

func (s *socketAddresses) UnmarshalYAML(n *yaml.Node) error {
	var multi []SocketAddress
	err := n.Decode(&multi)
	if err == nil {
		*s = multi
		return nil
	}

	var single SocketAddress
	err = n.Decode(&single)
	if err != nil {
		return err
	}
	*s = []SocketAddress{single}
	return nil
}

// namedSocketAddresses is an intermediary one_or_many type to be used
// during our transition to strictly typed configuration parameters.
// This type will:
//   - parse an array of NamedSocketAddress
//   - parse a single NamedSocketAddress to an array.
type namedSocketAddresses []NamedSocketAddress

func (nsa *namedSocketAddresses) UnmarshalYAML(n *yaml.Node) error {
	var multi []NamedSocketAddress
	err := n.Decode(&multi)
	if err == nil {
		*nsa = multi
		return nil
	}

	var single NamedSocketAddress
	err = n.Decode(&single)
	if err != nil {
		return err
	}
	*nsa = []NamedSocketAddress{single}
	return nil
}

// namedAuthNSocketAddresses is an intermediary one_or_many type to be used
// during our transition to strictly typed configuration parameters.
// This type will:
//   - parse an array of NamedAuthNSocketAddress
//   - parse a single NamedAuthNSocketAddress to an array.
type namedAuthNSocketAddresses []NamedAuthNSocketAddress

func (nsa *namedAuthNSocketAddresses) UnmarshalYAML(n *yaml.Node) error {
	var multi []NamedAuthNSocketAddress
	err := n.Decode(&multi)
	if err == nil {
		*nsa = multi
		return nil
	}

	var single NamedAuthNSocketAddress
	err = n.Decode(&single)
	if err != nil {
		return err
	}
	*nsa = []NamedAuthNSocketAddress{single}
	return nil
}

// serverTLSArray is an intermediary one_or_many type to be used during our
// transition to strictly typed configuration parameters. This type will:
//   - parse an array of ServerTLS
//   - parse a single ServerTLS to an array.
type serverTLSArray []ServerTLS

func (s *serverTLSArray) UnmarshalYAML(n *yaml.Node) error {
	var multi []ServerTLS
	err := n.Decode(&multi)
	if err == nil {
		*s = multi
		return nil
	}

	var single ServerTLS
	err = n.Decode(&single)
	if err != nil {
		return err
	}
	// do not log serverTLS because the Other field may contain a secret
	*s = []ServerTLS{single}
	return nil
}

// seedServers is an intermediary one_or_many type to be used during our
// transition to strictly typed configuration parameters. This type will:
//   - parse an array of SeedServer
//   - parse a single SeedServer to an array.
type seedServers []SeedServer

func (ss *seedServers) UnmarshalYAML(n *yaml.Node) error {
	var multi []SeedServer
	err := n.Decode(&multi)
	if err == nil {
		*ss = multi
		return nil
	}

	var single SeedServer
	err = n.Decode(&single)
	if err != nil {
		return err
	}
	*ss = []SeedServer{single}
	return nil
}

// Custom unmarshallers for all the config related types.

func (c *Config) UnmarshalYAML(n *yaml.Node) error {
	var internal struct {
		NodeUUID             weakString         `yaml:"node_uuid"`
		Organization         weakString         `yaml:"organization"`
		LicenseKey           weakString         `yaml:"license_key"`
		ClusterID            weakString         `yaml:"cluster_id"`
		Redpanda             RedpandaNodeConfig `yaml:"redpanda"`
		Rpk                  RpkConfig          `yaml:"rpk"`
		Pandaproxy           *Pandaproxy        `yaml:"pandaproxy"`
		PandaproxyClient     *KafkaClient       `yaml:"pandaproxy_client"`
		SchemaRegistry       *SchemaRegistry    `yaml:"schema_registry"`
		SchemaRegistryClient *KafkaClient       `yaml:"schema_registry_client"`

		Other map[string]interface{} `yaml:",inline"`
	}
	if err := n.Decode(&internal); err != nil {
		return err
	}
	c.NodeUUID = string(internal.NodeUUID)
	c.Organization = string(internal.Organization)
	c.LicenseKey = string(internal.LicenseKey)
	c.ClusterID = string(internal.ClusterID)
	c.Redpanda = internal.Redpanda
	c.Rpk = internal.Rpk
	c.Pandaproxy = internal.Pandaproxy
	c.PandaproxyClient = internal.PandaproxyClient
	c.SchemaRegistry = internal.SchemaRegistry
	c.SchemaRegistryClient = internal.SchemaRegistryClient
	c.Other = internal.Other

	return nil
}

func (rpc *RedpandaNodeConfig) UnmarshalYAML(n *yaml.Node) error {
	var internal struct {
		Directory                  weakString                `yaml:"data_directory"`
		ID                         *weakInt                  `yaml:"node_id"`
		Rack                       weakString                `yaml:"rack"`
		EmptySeedStartsCluster     *weakBool                 `yaml:"empty_seed_starts_cluster"`
		SeedServers                seedServers               `yaml:"seed_servers"`
		RPCServer                  SocketAddress             `yaml:"rpc_server"`
		RPCServerTLS               serverTLSArray            `yaml:"rpc_server_tls"`
		KafkaAPI                   namedAuthNSocketAddresses `yaml:"kafka_api"`
		KafkaAPITLS                serverTLSArray            `yaml:"kafka_api_tls"`
		AdminAPI                   namedSocketAddresses      `yaml:"admin"`
		AdminAPITLS                serverTLSArray            `yaml:"admin_api_tls"`
		CoprocSupervisorServer     SocketAddress             `yaml:"coproc_supervisor_server"`
		AdminAPIDocDir             weakString                `yaml:"admin_api_doc_dir"`
		DashboardDir               weakString                `yaml:"dashboard_dir"`
		CloudStorageCacheDirectory weakString                `yaml:"cloud_storage_cache_directory"`
		AdvertisedRPCAPI           *SocketAddress            `yaml:"advertised_rpc_api"`
		AdvertisedKafkaAPI         namedSocketAddresses      `yaml:"advertised_kafka_api"`
		DeveloperMode              weakBool                  `yaml:"developer_mode"`
		CrashLoopLimit             *weakInt                  `yaml:"crash_loop_limit"`
		Other                      map[string]interface{}    `yaml:",inline"`
	}

	if err := n.Decode(&internal); err != nil {
		return err
	}
	rpc.Directory = string(internal.Directory)
	rpc.ID = (*int)(internal.ID)
	rpc.Rack = string(internal.Rack)
	rpc.EmptySeedStartsCluster = (*bool)(internal.EmptySeedStartsCluster)
	rpc.SeedServers = internal.SeedServers
	rpc.RPCServer = internal.RPCServer
	rpc.RPCServerTLS = internal.RPCServerTLS
	rpc.KafkaAPI = internal.KafkaAPI
	rpc.KafkaAPITLS = internal.KafkaAPITLS
	rpc.AdminAPI = internal.AdminAPI
	rpc.AdminAPITLS = internal.AdminAPITLS
	rpc.CoprocSupervisorServer = internal.CoprocSupervisorServer
	rpc.AdminAPIDocDir = string(internal.AdminAPIDocDir)
	rpc.DashboardDir = string(internal.DashboardDir)
	rpc.CloudStorageCacheDirectory = string(internal.CloudStorageCacheDirectory)
	rpc.AdvertisedRPCAPI = internal.AdvertisedRPCAPI
	rpc.AdvertisedKafkaAPI = internal.AdvertisedKafkaAPI
	rpc.DeveloperMode = bool(internal.DeveloperMode)
	rpc.CrashLoopLimit = (*int)(internal.CrashLoopLimit)
	rpc.Other = internal.Other
	return nil
}

func (rpkc *RpkConfig) UnmarshalYAML(n *yaml.Node) error {
	var internal struct {
		// Deprecated 2021-07-1
		TLS *TLS `yaml:"tls"`
		// Deprecated 2021-07-1
		SASL *SASL `yaml:"sasl"`

		KafkaAPI                 RpkKafkaAPI     `yaml:"kafka_api"`
		AdminAPI                 RpkAdminAPI     `yaml:"admin_api"`
		AdditionalStartFlags     weakStringArray `yaml:"additional_start_flags"`
		TuneNetwork              weakBool        `yaml:"tune_network"`
		TuneDiskScheduler        weakBool        `yaml:"tune_disk_scheduler"`
		TuneNomerges             weakBool        `yaml:"tune_disk_nomerges"`
		TuneDiskWriteCache       weakBool        `yaml:"tune_disk_write_cache"`
		TuneDiskIrq              weakBool        `yaml:"tune_disk_irq"`
		TuneFstrim               weakBool        `yaml:"tune_fstrim"`
		TuneCPU                  weakBool        `yaml:"tune_cpu"`
		TuneAioEvents            weakBool        `yaml:"tune_aio_events"`
		TuneClocksource          weakBool        `yaml:"tune_clocksource"`
		TuneSwappiness           weakBool        `yaml:"tune_swappiness"`
		TuneTransparentHugePages weakBool        `yaml:"tune_transparent_hugepages"`
		EnableMemoryLocking      weakBool        `yaml:"enable_memory_locking"`
		TuneCoredump             weakBool        `yaml:"tune_coredump"`
		CoredumpDir              weakString      `yaml:"coredump_dir"`
		TuneBallastFile          weakBool        `yaml:"tune_ballast_file"`
		BallastFilePath          weakString      `yaml:"ballast_file_path"`
		BallastFileSize          weakString      `yaml:"ballast_file_size"`
		WellKnownIo              weakString      `yaml:"well_known_io"`
		Overprovisioned          weakBool        `yaml:"overprovisioned"`
		SMP                      *weakInt        `yaml:"smp"`
	}
	if err := n.Decode(&internal); err != nil {
		return err
	}

	rpkc.TLS = internal.TLS
	rpkc.SASL = internal.SASL
	rpkc.KafkaAPI = internal.KafkaAPI
	rpkc.AdminAPI = internal.AdminAPI
	rpkc.AdditionalStartFlags = internal.AdditionalStartFlags
	rpkc.TuneNetwork = bool(internal.TuneNetwork)
	rpkc.TuneDiskScheduler = bool(internal.TuneDiskScheduler)
	rpkc.TuneNomerges = bool(internal.TuneNomerges)
	rpkc.TuneDiskWriteCache = bool(internal.TuneDiskWriteCache)
	rpkc.TuneDiskIrq = bool(internal.TuneDiskIrq)
	rpkc.TuneFstrim = bool(internal.TuneFstrim)
	rpkc.TuneCPU = bool(internal.TuneCPU)
	rpkc.TuneAioEvents = bool(internal.TuneAioEvents)
	rpkc.TuneClocksource = bool(internal.TuneClocksource)
	rpkc.TuneSwappiness = bool(internal.TuneSwappiness)
	rpkc.TuneTransparentHugePages = bool(internal.TuneTransparentHugePages)
	rpkc.EnableMemoryLocking = bool(internal.EnableMemoryLocking)
	rpkc.TuneCoredump = bool(internal.TuneCoredump)
	rpkc.CoredumpDir = string(internal.CoredumpDir)
	rpkc.TuneBallastFile = bool(internal.TuneBallastFile)
	rpkc.BallastFilePath = string(internal.BallastFilePath)
	rpkc.BallastFileSize = string(internal.BallastFileSize)
	rpkc.WellKnownIo = string(internal.WellKnownIo)
	rpkc.Overprovisioned = bool(internal.Overprovisioned)
	rpkc.SMP = (*int)(internal.SMP)
	return nil
}

func (r *RpkKafkaAPI) UnmarshalYAML(n *yaml.Node) error {
	var internal struct {
		Brokers weakStringArray `yaml:"brokers"`
		TLS     *TLS            `yaml:"tls"`
		SASL    *SASL           `yaml:"sasl"`
	}
	if err := n.Decode(&internal); err != nil {
		return err
	}
	r.Brokers = internal.Brokers
	r.TLS = internal.TLS
	r.SASL = internal.SASL
	return nil
}

func (r *RpkAdminAPI) UnmarshalYAML(n *yaml.Node) error {
	var internal struct {
		Addresses weakStringArray `yaml:"addresses"`
		TLS       *TLS            `yaml:"tls"`
	}
	if err := n.Decode(&internal); err != nil {
		return err
	}
	r.Addresses = internal.Addresses
	r.TLS = internal.TLS
	return nil
}

func (p *Pandaproxy) UnmarshalYAML(n *yaml.Node) error {
	var internal struct {
		PandaproxyAPI           namedAuthNSocketAddresses `yaml:"pandaproxy_api"`
		PandaproxyAPITLS        serverTLSArray            `yaml:"pandaproxy_api_tls"`
		AdvertisedPandaproxyAPI namedSocketAddresses      `yaml:"advertised_pandaproxy_api"`
		Other                   map[string]interface{}    `yaml:",inline"`
	}
	if err := n.Decode(&internal); err != nil {
		return err
	}
	p.PandaproxyAPI = internal.PandaproxyAPI
	p.PandaproxyAPITLS = internal.PandaproxyAPITLS
	p.AdvertisedPandaproxyAPI = internal.AdvertisedPandaproxyAPI
	p.Other = internal.Other
	return nil
}

func (k *KafkaClient) UnmarshalYAML(n *yaml.Node) error {
	var internal struct {
		Brokers       socketAddresses        `yaml:"brokers"`
		BrokerTLS     ServerTLS              `yaml:"broker_tls"`
		SASLMechanism *weakString            `yaml:"sasl_mechanism"`
		SCRAMUsername *weakString            `yaml:"scram_username"`
		SCRAMPassword *weakString            `yaml:"scram_password"`
		Other         map[string]interface{} `yaml:",inline"`
	}
	if err := n.Decode(&internal); err != nil {
		return err
	}
	k.Brokers = internal.Brokers
	k.BrokerTLS = internal.BrokerTLS
	k.SASLMechanism = (*string)(internal.SASLMechanism)
	k.SCRAMUsername = (*string)(internal.SCRAMUsername)
	k.SCRAMPassword = (*string)(internal.SCRAMPassword)
	k.Other = internal.Other
	return nil
}

func (s *SchemaRegistry) UnmarshalYAML(n *yaml.Node) error {
	var internal struct {
		SchemaRegistryAPI               namedAuthNSocketAddresses `yaml:"schema_registry_api"`
		SchemaRegistryAPITLS            serverTLSArray            `yaml:"schema_registry_api_tls"`
		SchemaRegistryReplicationFactor *weakInt                  `yaml:"schema_registry_replication_factor"`
	}

	if err := n.Decode(&internal); err != nil {
		return err
	}
	s.SchemaRegistryAPI = internal.SchemaRegistryAPI
	s.SchemaRegistryAPITLS = internal.SchemaRegistryAPITLS
	s.SchemaRegistryReplicationFactor = (*int)(internal.SchemaRegistryReplicationFactor)
	return nil
}

func (s *ServerTLS) UnmarshalYAML(n *yaml.Node) error {
	var internal struct {
		Name              weakString             `yaml:"name"`
		KeyFile           weakString             `yaml:"key_file"`
		CertFile          weakString             `yaml:"cert_file"`
		TruststoreFile    weakString             `yaml:"truststore_file"`
		Enabled           weakBool               `yaml:"enabled"`
		RequireClientAuth weakBool               `yaml:"require_client_auth"`
		Other             map[string]interface{} `yaml:",inline"`
	}
	if err := n.Decode(&internal); err != nil {
		return err
	}
	s.Name = string(internal.Name)
	s.KeyFile = string(internal.KeyFile)
	s.CertFile = string(internal.CertFile)
	s.TruststoreFile = string(internal.TruststoreFile)
	s.Enabled = bool(internal.Enabled)
	s.RequireClientAuth = bool(internal.RequireClientAuth)
	s.Other = internal.Other
	return nil
}

func (ss *SeedServer) UnmarshalYAML(n *yaml.Node) error {
	var internal struct {
		// New schema should only contain Address and Port, but we will
		// support this under Host also.
		Address weakString    `yaml:"address"`
		Port    weakInt       `yaml:"port"`
		Host    SocketAddress `yaml:"host"`
		// deprecated
		NodeID *weakInt `yaml:"node_id"`
	}
	if err := n.Decode(&internal); err != nil {
		return err
	}
	if internal.NodeID != nil {
		fmt.Println("redpanda yaml: redpanda.seed_server.node_id is deprecated and unused")
	}

	if internal.Address != "" || internal.Port != 0 {
		embedded := SocketAddress{string(internal.Address), int(internal.Port)}
		nested := internal.Host

		embeddedZero := reflect.DeepEqual(embedded, SocketAddress{})
		nestedZero := reflect.DeepEqual(nested, SocketAddress{})

		if !embeddedZero && !nestedZero && !reflect.DeepEqual(embedded, nested) {
			return errors.New("redpanda.yaml redpanda.seed_server: nested host differs from address and port fields; only one must be set")
		}

		ss.untabbed = true // This means that we are unmarshalling an older version.

		ss.Host = embedded
		if embeddedZero {
			ss.Host = nested
		}
		return nil
	}

	ss.Host = internal.Host
	return nil
}

func (sa *SocketAddress) UnmarshalYAML(n *yaml.Node) error {
	var internal struct {
		Address weakString `yaml:"address"`
		Port    weakInt    `yaml:"port"`
	}
	if err := n.Decode(&internal); err != nil {
		return err
	}
	sa.Address = string(internal.Address)
	sa.Port = int(internal.Port)
	return nil
}

func (nsa *NamedSocketAddress) UnmarshalYAML(n *yaml.Node) error {
	var internal struct {
		Name    weakString `yaml:"name"`
		Address weakString `yaml:"address"`
		Port    weakInt    `yaml:"port"`
	}

	if err := n.Decode(&internal); err != nil {
		return err
	}

	nsa.Name = string(internal.Name)
	nsa.Address = string(internal.Address)
	nsa.Port = int(internal.Port)
	return nil
}

func (nsa *NamedAuthNSocketAddress) UnmarshalYAML(n *yaml.Node) error {
	var internal struct {
		Name    weakString  `yaml:"name"`
		Address weakString  `yaml:"address" mapstructure:"address"`
		Port    weakInt     `yaml:"port" mapstructure:"port"`
		AuthN   *weakString `yaml:"authentication_method" mapstructure:"authentication_method"`
	}

	if err := n.Decode(&internal); err != nil {
		return err
	}

	nsa.Name = string(internal.Name)
	nsa.Address = string(internal.Address)
	nsa.Port = int(internal.Port)
	nsa.AuthN = (*string)(internal.AuthN)
	return nil
}

func (t *TLS) UnmarshalYAML(n *yaml.Node) error {
	var internal struct {
		KeyFile        weakString `yaml:"key_file"`
		CertFile       weakString `yaml:"cert_file"`
		TruststoreFile weakString `yaml:"truststore_file"`
	}

	if err := n.Decode(&internal); err != nil {
		return err
	}
	t.KeyFile = string(internal.KeyFile)
	t.CertFile = string(internal.CertFile)
	t.TruststoreFile = string(internal.TruststoreFile)
	return nil
}

func (s *SASL) UnmarshalYAML(n *yaml.Node) error {
	var internal struct {
		User      weakString `yaml:"user"`
		Password  weakString `yaml:"password"`
		Mechanism weakString `yaml:"type"`
	}
	if err := n.Decode(&internal); err != nil {
		return err
	}
	s.User = string(internal.User)
	s.Password = string(internal.Password)
	s.Mechanism = string(internal.Mechanism)

	return nil
}
