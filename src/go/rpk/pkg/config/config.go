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
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"vectorized/pkg/utils"
	vyaml "vectorized/pkg/yaml"

	"github.com/google/uuid"
	"github.com/mitchellh/mapstructure"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"github.com/spf13/viper"
	"gopkg.in/yaml.v2"
)

const (
	ModeDev  = "dev"
	ModeProd = "prod"
)

type Config struct {
	NodeUuid     string          `yaml:"node_uuid,omitempty" json:"nodeUuid"`
	Organization string          `yaml:"organization,omitempty" json:"organization"`
	LicenseKey   string          `yaml:"license_key,omitempty" json:"licenseKey"`
	ClusterId    string          `yaml:"cluster_id,omitempty" json:"clusterId"`
	ConfigFile   string          `yaml:"config_file,omitempty" json:"configFile"`
	Redpanda     *RedpandaConfig `json:"redpanda"`
	Rpk          *RpkConfig      `yaml:"rpk,omitempty" json:"rpk"`
}

type RedpandaConfig struct {
	Directory          string         `yaml:"data_directory" json:"dataDirectory"`
	RPCServer          SocketAddress  `yaml:"rpc_server" json:"rpcServer"`
	AdvertisedRPCAPI   *SocketAddress `yaml:"advertised_rpc_api,omitempty" json:"advertisedRpcApi"`
	KafkaApi           SocketAddress  `yaml:"kafka_api" json:"kafkaApi"`
	AdvertisedKafkaApi *SocketAddress `yaml:"advertised_kafka_api,omitempty" json:"advertisedKafkaApi"`
	KafkaApiTLS        ServerTLS      `yaml:"kafka_api_tls" json:"kafkaApiTls"`
	AdminApi           SocketAddress  `yaml:"admin" json:"admin"`
	Id                 int            `yaml:"node_id" json:"id"`
	SeedServers        []*SeedServer  `yaml:"seed_servers" json:"seedServers"`
	DeveloperMode      bool           `yaml:"developer_mode" json:"developerMode"`
}

type SeedServer struct {
	Host SocketAddress `yaml:"host" json:"host"`
	Id   int           `yaml:"node_id" json:"id"`
}

type SocketAddress struct {
	Address string `yaml:"address" json:"address"`
	Port    int    `yaml:"port" json:"port"`
}

type TLS struct {
	KeyFile        string `yaml:"key_file" json:"keyFile"`
	CertFile       string `yaml:"cert_file" json:"certFile"`
	TruststoreFile string `yaml:"truststore_file" json:"truststoreFile"`
}

type ServerTLS struct {
	KeyFile        string `yaml:"key_file" json:"keyFile"`
	CertFile       string `yaml:"cert_file" json:"certFile"`
	TruststoreFile string `yaml:"truststore_file" json:"truststoreFile"`
	Enabled        bool   `yaml:"enabled" json:"enabled"`
}

type RpkConfig struct {
	TLS                      TLS      `yaml:"tls" json:"tls"`
	AdditionalStartFlags     []string `yaml:"additional_start_flags,omitempty" json:"additionalStartFlags"`
	EnableUsageStats         bool     `yaml:"enable_usage_stats" json:"enableUsageStats"`
	TuneNetwork              bool     `yaml:"tune_network" json:"tuneNetwork"`
	TuneDiskScheduler        bool     `yaml:"tune_disk_scheduler" json:"tuneDiskScheduler"`
	TuneNomerges             bool     `yaml:"tune_disk_nomerges" json:"tuneNomerges"`
	TuneDiskIrq              bool     `yaml:"tune_disk_irq" json:"tuneDiskIrq"`
	TuneFstrim               bool     `yaml:"tune_fstrim" json:"tuneFstrim"`
	TuneCpu                  bool     `yaml:"tune_cpu" json:"tuneCpu"`
	TuneAioEvents            bool     `yaml:"tune_aio_events" json:"tuneAioEvents"`
	TuneClocksource          bool     `yaml:"tune_clocksource" json:"tuneClocksource"`
	TuneSwappiness           bool     `yaml:"tune_swappiness" json:"tuneSwappiness"`
	TuneTransparentHugePages bool     `yaml:"tune_transparent_hugepages" json:"tuneTransparentHugePages"`
	EnableMemoryLocking      bool     `yaml:"enable_memory_locking" json:"enableMemoryLocking"`
	TuneCoredump             bool     `yaml:"tune_coredump" json:"tuneCoredump"`
	CoredumpDir              string   `yaml:"coredump_dir" json:"coredumpDir"`
	WellKnownIo              string   `yaml:"well_known_io,omitempty" json:"wellKnownIo"`
	Overprovisioned          bool     `yaml:"overprovisioned", json:"overprovisioned"`
	SMP                      *int     `yaml:"smp,omitempty", json:"smp,omitempty"`
}

func (conf *Config) PIDFile() string {
	return path.Join(conf.Redpanda.Directory, "pid.lock")
}

func DefaultConfig() Config {
	return Config{
		ConfigFile: "/etc/redpanda/redpanda.yaml",
		Redpanda: &RedpandaConfig{
			Directory:   "/var/lib/redpanda/data",
			RPCServer:   SocketAddress{"0.0.0.0", 33145},
			KafkaApi:    SocketAddress{"0.0.0.0", 9092},
			AdminApi:    SocketAddress{"0.0.0.0", 9644},
			Id:          0,
			SeedServers: []*SeedServer{},
		},
		Rpk: &RpkConfig{
			CoredumpDir: "/var/lib/redpanda/coredump",
		},
	}
}

func Set(fs afero.Fs, key, value, format, path string) error {
	confMap, err := read(fs, path)
	if err != nil {
		return err
	}
	v := viper.New()
	v.MergeConfigMap(confMap)
	var newConfValue interface{}
	switch strings.ToLower(format) {
	case "single":
		v.Set(key, parse(value))
		err = checkAndWrite(fs, v.AllSettings(), path)
		if err == nil {
			checkAndPrintRestartWarning(key)
		}
		return err
	case "yaml":
		err := yaml.Unmarshal([]byte(value), &newConfValue)
		if err != nil {
			return err
		}
	case "json":
		err := json.Unmarshal([]byte(value), &newConfValue)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported format %s", format)
	}

	newV := viper.New()
	newV.Set(key, newConfValue)
	v.MergeConfigMap(newV.AllSettings())
	err = checkAndWrite(fs, v.AllSettings(), path)
	if err == nil {
		checkAndPrintRestartWarning(key)
	}
	return err
}

func parse(val string) interface{} {
	if i, err := strconv.Atoi(val); err == nil {
		return i
	}
	if f, err := strconv.ParseFloat(val, 64); err == nil {
		return f
	}
	if b, err := strconv.ParseBool(val); err == nil {
		return b
	}
	return val
}

// Checks config and writes it to the given path.
func WriteConfig(fs afero.Fs, config *Config, path string) error {
	confMap, err := toMap(config)
	if err != nil {
		return err
	}
	return checkAndWrite(fs, confMap, path)
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

func ReadConfigFromPath(fs afero.Fs, path string) (*Config, error) {
	log.Debugf("Reading Redpanda config file from '%s'", path)
	config := &Config{}
	err := vyaml.Read(fs, config, path)
	if err != nil {
		return nil, err
	}
	config.ConfigFile = path
	return config, nil
}

// Tries reading a config file at the given path, or generates a default config
// and writes it to the path.
func ReadOrGenerate(fs afero.Fs, configFile string) (*Config, error) {
	conf, err := ReadConfigFromPath(fs, configFile)
	if err == nil {
		// The config file's there, there's nothing to do.
		return conf, nil
	}
	if os.IsNotExist(err) {
		log.Debug(err)
		log.Infof(
			"Couldn't find config file at %s. Generating it.",
			configFile,
		)
		conf := DefaultConfig()
		conf.ConfigFile = configFile
		err = WriteConfig(fs, &conf, configFile)
		if err != nil {
			return nil, fmt.Errorf(
				"Couldn't write config to %s: %v",
				configFile,
				err,
			)
		}
		return &conf, nil
	}
	return nil, fmt.Errorf(
		"An error happened while trying to read %s: %v",
		configFile,
		err,
	)
}

func ReadOrFind(fs afero.Fs, configFile string) (*Config, error) {
	var err error
	if configFile == "" {
		configFile, err = FindConfigFile(fs)
		if err != nil {
			return nil, err
		}
	}
	return ReadConfigFromPath(fs, configFile)
}

// If configFile is empty, tries to find the file in the default locations.
// Otherwise, it tries to read the file and load it. If the file doesn't
// exist, it tries to create it with the default configuration.
func FindOrGenerate(fs afero.Fs, configFile string) (*Config, error) {
	var err error
	if configFile == "" {
		configFile, err = FindConfigFile(fs)
		if err != nil {
			return nil, err
		}
	}
	return ReadOrGenerate(fs, configFile)
}
func CheckConfig(config *Config) (bool, []error) {
	configMap, err := toMap(config)
	if err != nil {
		return false, []error{err}
	}
	return check(configMap)
}

func recover(fs afero.Fs, backup, path string, err error) error {
	log.Infof("Recovering the previous confing from %s", backup)
	recErr := utils.CopyFile(fs, backup, path)
	if recErr != nil {
		msg := "couldn't persist the new config due to '%v'," +
			" nor recover the backup due to '%v"
		return fmt.Errorf(msg, err, recErr)
	}
	return fmt.Errorf("couldn't persist the new config due to '%v'", err)
}

func checkAndWrite(
	fs afero.Fs, conf map[string]interface{}, path string,
) error {
	ok, errs := check(conf)
	if !ok {
		reasons := []string{}
		for _, err := range errs {
			reasons = append(reasons, err.Error())
		}
		return errors.New(strings.Join(reasons, ", "))
	}
	lastBackupFile, err := findBackup(fs, filepath.Dir(path))
	if err != nil {
		return err
	}
	exists, err := afero.Exists(fs, path)
	if err != nil {
		return err
	}
	if !exists {
		// If the config doesn't exist, just write it.
		return write(fs, conf, path)
	}
	// Otherwise, backup the current config file, write the new one, and
	// try to recover if there's an error.
	log.Debug("Backing up the current config")
	backup, err := utils.BackupFile(fs, path)
	if err != nil {
		return err
	}
	log.Debugf("Backed up the current config to %s", backup)
	if lastBackupFile != "" && lastBackupFile != backup {
		log.Debug("Removing previous backup file")
		err = fs.Remove(lastBackupFile)
		if err != nil {
			return err
		}
	}
	currentConf, err := read(fs, path)
	if err != nil {
		return recover(fs, backup, path, err)
	}
	log.Debugf("Writing the new redpanda config to '%s'", path)
	if err != nil {
		return recover(fs, backup, path, err)
	}
	merged := merge(currentConf, conf)
	err = write(fs, merged, path)
	if err != nil {
		return recover(fs, backup, path, err)
	}
	return nil
}

func ReadFlat(fs afero.Fs, path string) (map[string]string, error) {
	v, err := readViper(fs, path)
	if err != nil {
		return nil, err
	}
	keys := v.AllKeys()
	flatMap := map[string]string{}
	compactAddrFields := []string{
		"redpanda.kafka_api",
		"redpanda.rpc_server",
		"redpanda.admin",
	}
	unmarshalKey := func(key string, val interface{}) error {
		return v.UnmarshalKey(
			key,
			val,
			func(c *mapstructure.DecoderConfig) {
				c.TagName = "yaml"
			},
		)
	}
	for _, k := range keys {
		if k == "redpanda.seed_servers" {
			seeds := &[]SeedServer{}
			err := unmarshalKey(k, seeds)
			if err != nil {
				return nil, err
			}
			for _, s := range *seeds {
				key := fmt.Sprintf("%s.%d", k, s.Id)
				flatMap[key] = fmt.Sprintf("%s:%d", s.Host.Address, s.Host.Port)
			}
			continue
		}
		// These fields are added later on as <address>:<port>
		// instead of
		// field.address <address>
		// field.port    <port>
		if strings.HasSuffix(k, ".port") || strings.HasSuffix(k, ".address") {
			continue
		}

		s := v.GetString(k)
		flatMap[k] = s
	}
	for _, k := range compactAddrFields {
		sa := &SocketAddress{}
		err := unmarshalKey(k, sa)
		if err != nil {
			return nil, err
		}
		flatMap[k] = fmt.Sprintf("%s:%d", sa.Address, sa.Port)
	}
	return flatMap, nil
}

func ReadAsJSON(fs afero.Fs, path string) (string, error) {
	confMap, err := read(fs, path)
	if err != nil {
		return "", err
	}
	confJSON, err := json.Marshal(confMap)
	if err != nil {
		return "", err
	}
	return string(confJSON), nil
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
	conf.Redpanda.DeveloperMode = true
	// Defaults to setting all tuners to false
	conf.Rpk = &RpkConfig{
		EnableUsageStats: conf.Rpk.EnableUsageStats,
		CoredumpDir:      conf.Rpk.CoredumpDir,
		SMP:              DefaultConfig().Rpk.SMP,
		Overprovisioned:  true,
	}
	return conf
}

func setProduction(conf *Config) *Config {
	conf.Redpanda.DeveloperMode = false
	rpk := conf.Rpk
	rpk.TuneNetwork = true
	rpk.TuneDiskScheduler = true
	rpk.TuneNomerges = true
	rpk.TuneDiskIrq = true
	rpk.TuneFstrim = true
	rpk.TuneCpu = true
	rpk.TuneAioEvents = true
	rpk.TuneClocksource = true
	rpk.TuneSwappiness = true
	rpk.Overprovisioned = false
	return conf
}

func AvailableModes() []string {
	return []string{
		ModeDev,
		"development",
		ModeProd,
		"production",
	}
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

func write(fs afero.Fs, conf map[string]interface{}, path string) error {
	v := viper.New()
	v.SetFs(fs)
	v.MergeConfigMap(conf)
	err := v.WriteConfigAs(path)
	if err != nil {
		return err
	}
	log.Debugf(
		"Configuration written to %s.",
		path,
	)
	return nil
}

func merge(current, new map[string]interface{}) map[string]interface{} {
	v := viper.New()
	v.MergeConfigMap(current)
	v.MergeConfigMap(new)
	return v.AllSettings()
}

func read(fs afero.Fs, path string) (map[string]interface{}, error) {
	v, err := readViper(fs, path)
	if err != nil {
		return nil, err
	}
	return v.AllSettings(), nil
}

func readViper(fs afero.Fs, path string) (*viper.Viper, error) {
	v := viper.New()
	v.SetFs(fs)
	v.SetConfigFile(path)
	v.SetConfigType("yaml")
	err := v.ReadInConfig()
	return v, err
}

func toMap(conf *Config) (map[string]interface{}, error) {
	mapConf := make(map[string]interface{})
	bs, err := yaml.Marshal(conf)
	if err != nil {
		return mapConf, err
	}
	err = yaml.Unmarshal(bs, &mapConf)
	return mapConf, err
}

func check(conf map[string]interface{}) (bool, []error) {
	v := viper.New()
	v.MergeConfigMap(conf)
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
	rp := v.Sub("redpanda")
	if rp == nil {
		return []error{errors.New("the redpanda config is missing")}
	}
	if rp.GetString("data_directory") == "" {
		errs = append(errs, fmt.Errorf("redpanda.data_directory can't be empty"))
	}
	if rp.GetInt("node_id") < 0 {
		errs = append(errs, fmt.Errorf("redpanda.node_id can't be a negative integer"))
	}
	errs = append(
		errs,
		checkSocketAddress(rp.Sub("rpc_server"), "redpanda.rpc_server")...,
	)
	errs = append(
		errs,
		checkSocketAddress(rp.Sub("kafka_api"), "redpanda.kafka_api")...,
	)
	var seedServersSlice []map[string]interface{}
	err := rp.UnmarshalKey("seed_servers", &seedServersSlice)
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
			s := viper.New()
			s.MergeConfigMap(seed)
			host := s.Sub("host")
			if host == nil {
				err := fmt.Errorf(
					"%s.%d.host can't be empty",
					seedServersPath,
					i,
				)
				errs = append(errs, err)
				continue
			}
			configPath := fmt.Sprintf(
				"%s.%d.host",
				seedServersPath,
				i,
			)
			errs = append(
				errs,
				checkSocketAddress(
					host,
					configPath,
				)...,
			)
		}
	}
	return errs
}

func checkSocketAddress(v *viper.Viper, configPath string) []error {
	errs := []error{}
	if v.GetInt("port") == 0 {
		errs = append(errs, fmt.Errorf("%s.port can't be 0", configPath))
	}
	if v.GetString("address") == "" {
		errs = append(errs, fmt.Errorf("%s.address can't be empty", configPath))
	}
	return errs
}

func checkRpkConfig(v *viper.Viper) []error {
	errs := []error{}
	rpk := v.Sub("rpk")
	if rpk == nil {
		return errs
	}
	if rpk.GetBool("tune_coredump") && rpk.GetString("coredump_dir") == "" {
		msg := "if rpk.tune_coredump is set to true," +
			"rpk.coredump_dir can't be empty"
		errs = append(errs, errors.New(msg))
	}
	return errs
}

func GenerateAndWriteNodeUuid(fs afero.Fs, conf *Config) (*Config, error) {
	id, err := uuid.NewUUID()
	if err != nil {
		return nil, err
	}
	conf.NodeUuid = base58Encode(id.String())
	err = WriteConfig(fs, conf, conf.ConfigFile)
	if err != nil {
		return nil, err
	}
	return conf, nil
}

func stringifyMap(m map[interface{}]interface{}) map[string]interface{} {
	new := map[string]interface{}{}
	for k, v := range m {
		sk := fmt.Sprintf("%v", k)
		sub, ok := v.(map[interface{}]interface{})
		if ok {
			newSub := stringifyMap(sub)
			new[sk] = newSub
			continue
		}
		new[sk] = v
	}
	return new
}

func base58Encode(s string) string {
	b := []byte(s)

	alphabet := "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
	alphabetIdx0 := byte('1')
	bigRadix := big.NewInt(58)
	bigZero := big.NewInt(0)
	x := new(big.Int)
	x.SetBytes(b)

	answer := make([]byte, 0, len(b)*136/100)
	for x.Cmp(bigZero) > 0 {
		mod := new(big.Int)
		x.DivMod(x, bigRadix, mod)
		answer = append(answer, alphabet[mod.Int64()])
	}

	// leading zero bytes
	for _, i := range b {
		if i != 0 {
			break
		}
		answer = append(answer, alphabetIdx0)
	}

	// reverse
	alen := len(answer)
	for i := 0; i < alen/2; i++ {
		answer[i], answer[alen-1-i] = answer[alen-1-i], answer[i]
	}

	return string(answer)
}

func checkAndPrintRestartWarning(fieldKey string) {
	if strings.HasPrefix(fieldKey, "redpanda") {
		log.Info(
			"If redpanda is running, please restart it for the" +
				" changes to take effect.",
		)
	}
}
