package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"vectorized/pkg/utils"
	vyaml "vectorized/pkg/yaml"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"github.com/spf13/viper"
	"gopkg.in/yaml.v2"
)

type Config struct {
	NodeUuid     string          `yaml:"node_uuid,omitempty" json:"nodeUuid"`
	Organization string          `yaml:"organization,omitempty" json:"organization"`
	ClusterId    string          `yaml:"cluster_id,omitempty" json:"clusterId"`
	ConfigFile   string          `yaml:"config_file,omitempty" json:"configFile"`
	PidFile      string          `yaml:"pid_file" json:"pidFile"`
	Redpanda     *RedpandaConfig `json:"redpanda"`
	Rpk          *RpkConfig      `yaml:"rpk,omitempty" json:"rpk"`
}

type RedpandaConfig struct {
	Directory   string        `yaml:"data_directory" json:"directory"`
	RPCServer   SocketAddress `yaml:"rpc_server" json:"rpcServer"`
	KafkaApi    SocketAddress `yaml:"kafka_api" json:"kafkaApi"`
	AdminApi    SocketAddress `yaml:"admin" json:"admin"`
	Id          int           `yaml:"node_id" json:"id"`
	SeedServers []*SeedServer `yaml:"seed_servers" json:"seedServers"`
}

type SeedServer struct {
	Host SocketAddress `yaml:"host" json:"host"`
	Id   int           `yaml:"node_id" json:"id"`
}

type SocketAddress struct {
	Address string `yaml:"address" json:"address"`
	Port    int    `yaml:"port" json:"port"`
}

type RpkConfig struct {
	AdditionalStartFlags []string `yaml:"additional_start_flags,omitempty" json:"additionalStartFlags"`
	EnableUsageStats     bool     `yaml:"enable_usage_stats" json:"enableUsageStats"`
	TuneNetwork          bool     `yaml:"tune_network" json:"tuneNetwork"`
	TuneDiskScheduler    bool     `yaml:"tune_disk_scheduler" json:"tuneDiskScheduler"`
	TuneNomerges         bool     `yaml:"tune_disk_nomerges" json:"tuneNomerges"`
	TuneDiskIrq          bool     `yaml:"tune_disk_irq" json:"tuneDiskIrq"`
	TuneCpu              bool     `yaml:"tune_cpu" json:"tuneCpu"`
	TuneAioEvents        bool     `yaml:"tune_aio_events" json:"tuneAioEvents"`
	TuneClocksource      bool     `yaml:"tune_clocksource" json:"tuneClocksource"`
	TuneSwappiness       bool     `yaml:"tune_swappiness" json:"tuneSwappiness"`
	EnableMemoryLocking  bool     `yaml:"enable_memory_locking" json:"enableMemoryLocking"`
	TuneCoredump         bool     `yaml:"tune_coredump" json:"tuneCoredump"`
	CoredumpDir          string   `yaml:"coredump_dir" json:"coredumpDir"`
	WellKnownIo          string   `yaml:"well_known_io,omitempty" json:"wellKnownIo"`
}

func DefaultConfig() Config {
	return Config{
		ConfigFile: "/etc/redpanda/redpanda.yaml",
		PidFile:    "/var/lib/redpanda/pid",
		Redpanda: &RedpandaConfig{
			Directory:   "/var/lib/redpanda/data",
			RPCServer:   SocketAddress{"0.0.0.0", 33145},
			KafkaApi:    SocketAddress{"0.0.0.0", 9092},
			AdminApi:    SocketAddress{"0.0.0.0", 9644},
			Id:          0,
			SeedServers: []*SeedServer{},
		},
		Rpk: &RpkConfig{
			EnableUsageStats:    true,
			TuneNetwork:         true,
			TuneDiskScheduler:   true,
			TuneNomerges:        true,
			TuneDiskIrq:         true,
			TuneCpu:             true,
			TuneAioEvents:       true,
			TuneClocksource:     true,
			TuneSwappiness:      true,
			EnableMemoryLocking: false,
			TuneCoredump:        false,
			CoredumpDir:         "/var/lib/redpanda/coredump",
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
		return checkAndWrite(fs, v.AllSettings(), path)
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
	return checkAndWrite(fs, v.AllSettings(), path)
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
	for _, k := range keys {
		v := v.GetString(k)
		flatMap[k] = v
	}
	return flatMap, nil
}

func write(fs afero.Fs, conf map[string]interface{}, path string) error {
	v := viper.New()
	v.SetFs(fs)
	v.MergeConfigMap(conf)
	err := v.WriteConfigAs(path)
	if err != nil {
		return err
	}
	log.Infof(
		"Configuration written to %s. If redpanda is running, please"+
			" restart it for the changes to take effect.",
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
