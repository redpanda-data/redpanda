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
	"errors"
	"fmt"
	fp "path/filepath"
	"strings"

	"github.com/mitchellh/mapstructure"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"github.com/spf13/viper"
	"gopkg.in/yaml.v2"
)

const (
	ModeDev  = "dev"
	ModeProd = "prod"

	DefaultKafkaPort     = 9092
	DefaultSchemaRegPort = 8081
	DefaultProxyPort     = 8082
	DefaultAdminPort     = 9644

	DefaultBallastFilePath = "/var/lib/redpanda/data/ballast"
	DefaultBallastFileSize = "1GiB"
)

func InitViper(fs afero.Fs) *viper.Viper {
	v := viper.New()
	v.SetFs(fs)
	v.SetConfigName("redpanda")
	v.SetConfigType("yaml")

	// Viper does not take into account our explicit SetConfigType when
	// calling ReadInConfig, instead it internally uses SupportedExts.
	// Since we only ever want to load yaml, setting this global disables
	// ReadInConfig from using any existing json files.
	viper.SupportedExts = []string{"yaml"}

	setDefaults(v)
	return v
}

func addConfigPaths(v *viper.Viper) {
	v.AddConfigPath("$HOME")
	v.AddConfigPath(fp.Join("etc", "redpanda"))
	v.AddConfigPath(".")
}

func setDefaults(v *viper.Viper) {
	var traverse func(tree map[string]interface{}, path ...string)
	traverse = func(tree map[string]interface{}, path ...string) {
		for key, val := range tree {
			if subtree, ok := val.(map[string]interface{}); ok {
				traverse(subtree, append(path, key)...)
			} else {
				v.SetDefault(
					strings.Join(append(path, key), "."),
					val,
				)
			}
		}
	}
	traverse(defaultMap())
}

func Default() *Config {
	conf := &Config{}
	err := mapstructure.Decode(defaultMap(), conf)
	if err != nil {
		panic(err)
	}
	return conf
}

func defaultMap() map[string]interface{} {
	var defaultListener interface{} = map[string]interface{}{
		"address": "0.0.0.0",
		"port":    9092,
	}
	var defaultListeners []interface{} = []interface{}{defaultListener}
	var defaultAdminListener interface{} = map[string]interface{}{
		"address": "0.0.0.0",
		"port":    9644,
	}
	var defaultAdminListeners []interface{} = []interface{}{defaultAdminListener}
	return map[string]interface{}{
		"config_file":     "/etc/redpanda/redpanda.yaml",
		"pandaproxy":      Pandaproxy{},
		"schema_registry": SchemaRegistry{},
		"redpanda": map[string]interface{}{
			"data_directory": "/var/lib/redpanda/data",
			"rpc_server": map[string]interface{}{
				"address": "0.0.0.0",
				"port":    33145,
			},
			"kafka_api":      defaultListeners,
			"admin":          defaultAdminListeners,
			"node_id":        0,
			"seed_servers":   []interface{}{},
			"developer_mode": true,
		},
		"rpk": map[string]interface{}{
			"coredump_dir": "/var/lib/redpanda/coredump",
		},
	}
}

func findBackup(fs afero.Fs, dir string) (string, error) {
	exists, err := afero.Exists(fs, dir)
	if err != nil {
		return "", err
	}
	if !exists {
		return "", nil
	}
	files, err := afero.ReadDir(fs, dir)
	if err != nil {
		return "", err
	}
	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".bk") {
			return fmt.Sprintf("%s/%s", dir, f.Name()), nil
		}
	}
	return "", nil
}

func SetMode(mode string, conf *Config) (*Config, error) {
	m, err := NormalizeMode(mode)
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
	conf.Redpanda.Other["developer_mode"] = true
	// Defaults to setting all tuners to false
	conf.Rpk = RpkConfig{
		TLS:                  conf.Rpk.TLS,
		SASL:                 conf.Rpk.SASL,
		KafkaApi:             conf.Rpk.KafkaApi,
		AdminApi:             conf.Rpk.AdminApi,
		AdditionalStartFlags: conf.Rpk.AdditionalStartFlags,
		EnableUsageStats:     conf.Rpk.EnableUsageStats,
		CoredumpDir:          conf.Rpk.CoredumpDir,
		SMP:                  Default().Rpk.SMP,
		BallastFilePath:      conf.Rpk.BallastFilePath,
		BallastFileSize:      conf.Rpk.BallastFileSize,
		Overprovisioned:      true,
	}
	return conf
}

func setProduction(conf *Config) *Config {
	conf.Redpanda.Other["developer_mode"] = false
	conf.Rpk.TuneNetwork = true
	conf.Rpk.TuneDiskScheduler = true
	conf.Rpk.TuneNomerges = true
	conf.Rpk.TuneDiskIrq = true
	conf.Rpk.TuneFstrim = false
	conf.Rpk.TuneCpu = true
	conf.Rpk.TuneAioEvents = true
	conf.Rpk.TuneClocksource = true
	conf.Rpk.TuneSwappiness = true
	conf.Rpk.Overprovisioned = false
	conf.Rpk.TuneDiskWriteCache = true
	conf.Rpk.TuneBallastFile = true
	return conf
}

func NormalizeMode(mode string) (string, error) {
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

func AvailableModes() []string {
	return []string{
		ModeDev,
		"development",
		ModeProd,
		"production",
	}
}

func Check(conf *Config) (bool, []error) {
	configMap, err := toMap(conf)
	if err != nil {
		return false, []error{err}
	}

	v := viper.New()
	err = v.MergeConfigMap(configMap)
	if err != nil {
		return false, []error{err}
	}
	return check(v)
}

func check(v *viper.Viper) (bool, []error) {
	errs := checkRedpandaConfig(v)
	errs = append(
		errs,
		checkRpkConfig(v)...,
	)
	ok := len(errs) == 0
	return ok, errs
}

func checkRedpandaConfig(v *viper.Viper) []error {
	errs := []error{}
	if v.GetString("redpanda.data_directory") == "" {
		errs = append(errs, fmt.Errorf("redpanda.data_directory can't be empty"))
	}
	if v.GetInt("redpanda.node_id") < 0 {
		errs = append(errs, fmt.Errorf("redpanda.node_id can't be a negative integer"))
	}

	rpcServerKey := "redpanda.rpc_server"
	exists := v.Sub(rpcServerKey) != nil
	if !exists {
		errs = append(
			errs,
			fmt.Errorf("%s missing", rpcServerKey),
		)
	} else {
		socket := &SocketAddress{}
		err := unmarshalKey(v, rpcServerKey, socket)
		if err != nil {
			errs = append(
				errs,
				fmt.Errorf("invalid structure for %s", rpcServerKey),
			)
		} else {
			errs = append(
				errs,
				checkSocketAddress(*socket, rpcServerKey)...,
			)
		}
	}

	kafkaApiKey := "redpanda.kafka_api"
	exists = v.Get(kafkaApiKey) != nil
	if !exists {
		errs = append(
			errs,
			fmt.Errorf("%s missing", kafkaApiKey),
		)
	} else {
		var kafkaListeners []NamedSocketAddress
		err := unmarshalKey(v, "redpanda.kafka_api", &kafkaListeners)
		if err != nil {
			log.Error(err)
			err = fmt.Errorf(
				"%s doesn't have the expected structure",
				kafkaApiKey,
			)
			return append(
				errs,
				err,
			)
		}
		for i, addr := range kafkaListeners {
			configPath := fmt.Sprintf(
				"%s.%d",
				kafkaApiKey,
				i,
			)
			errs = append(
				errs,
				checkSocketAddress(
					addr.SocketAddress,
					configPath,
				)...,
			)
		}
	}

	var seedServersSlice []*SeedServer //map[string]interface{}
	err := unmarshalKey(v, "redpanda.seed_servers", &seedServersSlice)
	if err != nil {
		log.Error(err)
		msg := "redpanda.seed_servers doesn't have the expected structure"
		return append(
			errs,
			errors.New(msg),
		)
	}
	if len(seedServersSlice) > 0 {
		seedServersPath := "redpanda.seed_servers"
		for i, seed := range seedServersSlice {
			configPath := fmt.Sprintf(
				"%s.%d.host",
				seedServersPath,
				i,
			)
			errs = append(
				errs,
				checkSocketAddress(
					seed.Host,
					configPath,
				)...,
			)
		}
	}
	return errs
}

func checkSocketAddress(s SocketAddress, configPath string) []error {
	errs := []error{}
	if s.Port == 0 {
		errs = append(errs, fmt.Errorf("%s.port can't be 0", configPath))
	}
	if s.Address == "" {
		errs = append(errs, fmt.Errorf("%s.address can't be empty", configPath))
	}
	return errs
}

func checkRpkConfig(v *viper.Viper) []error {
	errs := []error{}
	if v.GetBool("rpk.tune_coredump") && v.GetString("rpk.coredump_dir") == "" {
		msg := "if rpk.tune_coredump is set to true," +
			"rpk.coredump_dir can't be empty"
		errs = append(errs, errors.New(msg))
	}
	return errs
}

func decoderConfig() mapstructure.DecoderConfig {
	return mapstructure.DecoderConfig{
		// Sometimes viper will save int values as strings (i.e.
		// through BindPFlag) so we have to allow mapstructure
		// to cast them.
		WeaklyTypedInput: true,
		DecodeHook: mapstructure.ComposeDecodeHookFunc(
			// These 2 hooks are viper's default hooks.
			// https://github.com/spf13/viper/blob/fb4eafdd9775508c450b90b1b72affeef4a68cf5/viper.go#L1004-L1005
			// They're set here because when decoderConfigOptions' resulting
			// viper.DecoderConfigOption is used, viper's hooks are overriden.
			mapstructure.StringToTimeDurationHookFunc(),
			mapstructure.StringToSliceHookFunc(","),
			// This hook translates the pre-21.1.4 configuration format to the
			// latest one (see schema.go)
			v21_1_4MapToNamedSocketAddressSlice,
			// This hook translates the pre-21.4.1 TLS configuration format to the
			// latest one (see schema.go)
			v21_4_1TlsMapToNamedTlsSlice,
		),
	}
}

func decoderConfigOptions() viper.DecoderConfigOption {
	return func(c *mapstructure.DecoderConfig) {
		cfg := decoderConfig()
		c.DecodeHook = cfg.DecodeHook
		c.WeaklyTypedInput = cfg.WeaklyTypedInput
	}
}

func unmarshalKey(v *viper.Viper, key string, val interface{}) error {
	return v.UnmarshalKey(
		key,
		val,
		decoderConfigOptions(),
	)
}

func toMap(conf *Config) (map[string]interface{}, error) {
	mapConf := make(map[string]interface{})
	bs, err := yaml.Marshal(conf)
	if err != nil {
		return nil, err
	}
	err = yaml.Unmarshal(bs, &mapConf)
	return mapConf, err
}
