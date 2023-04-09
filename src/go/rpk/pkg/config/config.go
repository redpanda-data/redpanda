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
	"fmt"
	"strings"
)

const (
	ModeDev  = "dev"
	ModeProd = "prod"

	DefaultKafkaPort     = 9092
	DefaultSchemaRegPort = 8081
	DefaultProxyPort     = 8082
	DefaultAdminPort     = 9644
	DefaultRPCPort       = 33145
	DefaultListenAddress = "0.0.0.0"

	DefaultBallastFilePath = "/var/lib/redpanda/data/ballast"
	DefaultBallastFileSize = "1GiB"
)

func DevDefault() *Config {
	return &Config{
		fileLocation: DefaultPath,
		Redpanda: RedpandaNodeConfig{
			Directory: "/var/lib/redpanda/data",
			RPCServer: SocketAddress{
				Address: DefaultListenAddress,
				Port:    DefaultRPCPort,
			},
			KafkaAPI: []NamedAuthNSocketAddress{{
				Address: DefaultListenAddress,
				Port:    DefaultKafkaPort,
			}},
			AdminAPI: []NamedSocketAddress{{
				Address: DefaultListenAddress,
				Port:    DefaultAdminPort,
			}},
			SeedServers:   []SeedServer{},
			DeveloperMode: true,
		},
		Rpk: RpkNodeConfig{
			Tuners: RpkNodeTuners{
				CoredumpDir:     "/var/lib/redpanda/coredump",
				Overprovisioned: true,
			},
		},
		// enable pandaproxy and schema_registry by default
		Pandaproxy:     &Pandaproxy{},
		SchemaRegistry: &SchemaRegistry{},
	}
}

func ProdDefault() *Config {
	cfg := DevDefault()
	return setProduction(cfg)
}

// FileOrDefaults return the configuration as read from the file or
// the default configuration if there is no file loaded.
func (c *Config) FileOrDefaults() *Config {
	if c.File() != nil {
		return c.File()
	} else {
		cfg := DevDefault()
		// --config set but the file doesn't exist yet:
		if c.fileLocation != "" {
			cfg.fileLocation = c.fileLocation
		}
		return cfg // no file, write the defaults
	}
}

///////////
// MODES //
///////////

func AvailableModes() []string {
	return []string{
		ModeDev,
		"development",
		ModeProd,
		"production",
	}
}

func SetMode(mode string, conf *Config) (*Config, error) {
	m, err := normalizeMode(mode)
	if err != nil {
		return nil, err
	}
	switch m {
	case ModeDev:
		return setDevelopment(conf), nil

	case ModeProd:
		return setProduction(conf), nil

	default:
		err := fmt.Errorf(
			"'%s' is not a supported mode. Available modes: %s",
			mode,
			strings.Join(AvailableModes(), ", "),
		)
		return nil, err
	}
}

func setDevelopment(conf *Config) *Config {
	conf.Redpanda.DeveloperMode = true
	// Defaults to setting all tuners to false
	conf.Rpk = RpkNodeConfig{
		TLS:                  conf.Rpk.TLS,
		SASL:                 conf.Rpk.SASL,
		KafkaAPI:             conf.Rpk.KafkaAPI,
		AdminAPI:             conf.Rpk.AdminAPI,
		AdditionalStartFlags: conf.Rpk.AdditionalStartFlags,
		Tuners: RpkNodeTuners{
			CoredumpDir:     conf.Rpk.Tuners.CoredumpDir,
			SMP:             DevDefault().Rpk.Tuners.SMP,
			BallastFilePath: conf.Rpk.Tuners.BallastFilePath,
			BallastFileSize: conf.Rpk.Tuners.BallastFileSize,
			Overprovisioned: true,
		},
	}
	return conf
}

func setProduction(conf *Config) *Config {
	conf.Redpanda.DeveloperMode = false
	conf.Rpk.Tuners.TuneNetwork = true
	conf.Rpk.Tuners.TuneDiskScheduler = true
	conf.Rpk.Tuners.TuneNomerges = true
	conf.Rpk.Tuners.TuneDiskIrq = true
	conf.Rpk.Tuners.TuneFstrim = false
	conf.Rpk.Tuners.TuneCPU = true
	conf.Rpk.Tuners.TuneAioEvents = true
	conf.Rpk.Tuners.TuneClocksource = true
	conf.Rpk.Tuners.TuneSwappiness = true
	conf.Rpk.Tuners.Overprovisioned = false
	conf.Rpk.Tuners.TuneDiskWriteCache = true
	conf.Rpk.Tuners.TuneBallastFile = true
	return conf
}

func normalizeMode(mode string) (string, error) {
	switch mode {
	case "":
		fallthrough
	case "development", ModeDev:
		return ModeDev, nil

	case "production", ModeProd:
		return ModeProd, nil

	default:
		err := fmt.Errorf(
			"'%s' is not a supported mode. Available modes: %s",
			mode,
			strings.Join(AvailableModes(), ", "),
		)
		return "", err
	}
}

////////////////
// VALIDATION // -- this is only used in redpanda_checkers, and could be stronger -- this is essentially just a config validation
////////////////

// Check checks if the redpanda and rpk configuration is valid before running
// the tuners. See: redpanda_checkers.
func (c *Config) Check() (bool, []error) {
	errs := checkRedpandaConfig(c)
	errs = append(
		errs,
		checkRpkNodeConfig(c)...,
	)
	ok := len(errs) == 0
	return ok, errs
}

func checkRedpandaConfig(cfg *Config) []error {
	var errs []error
	rp := cfg.Redpanda
	// top level check
	if rp.Directory == "" {
		errs = append(errs, fmt.Errorf("redpanda.data_directory can't be empty"))
	}
	if rp.ID != nil && *rp.ID < 0 {
		errs = append(errs, fmt.Errorf("redpanda.node_id can't be a negative integer"))
	}

	// rpc server
	if rp.RPCServer == (SocketAddress{}) {
		errs = append(errs, fmt.Errorf("redpanda.rpc_server missing"))
	} else {
		saErrs := checkSocketAddress(rp.RPCServer, "redpanda.rpc_server")
		if len(saErrs) > 0 {
			errs = append(errs, saErrs...)
		}
	}

	// kafka api
	if len(rp.KafkaAPI) == 0 {
		errs = append(errs, fmt.Errorf("redpanda.kafka_api missing"))
	} else {
		for i, addr := range rp.KafkaAPI {
			configPath := fmt.Sprintf("redpanda.kafka_api[%d]", i)
			saErrs := checkSocketAddress(SocketAddress{addr.Address, addr.Port}, configPath)
			if len(saErrs) > 0 {
				errs = append(errs, saErrs...)
			}
		}
	}

	// seed servers
	if len(rp.SeedServers) > 0 {
		for i, seed := range rp.SeedServers {
			configPath := fmt.Sprintf("redpanda.seed_servers[%d].host", i)
			saErrs := checkSocketAddress(seed.Host, configPath)
			if len(saErrs) > 0 {
				errs = append(errs, saErrs...)
			}
		}
	}
	return errs
}

func checkRpkNodeConfig(cfg *Config) []error {
	var errs []error
	if cfg.Rpk.Tuners.TuneCoredump && cfg.Rpk.Tuners.CoredumpDir == "" {
		errs = append(errs, fmt.Errorf("if rpk.tune_coredump is set to true, rpk.coredump_dir can't be empty"))
	}
	return errs
}

func checkSocketAddress(s SocketAddress, configPath string) []error {
	var errs []error
	if s.Port == 0 {
		errs = append(errs, fmt.Errorf("%s.port can't be 0", configPath))
	}
	if s.Address == "" {
		errs = append(errs, fmt.Errorf("%s.address can't be empty", configPath))
	}
	return errs
}
