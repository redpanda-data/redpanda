package config

import (
	"errors"
	"fmt"
	"math/big"
	"strings"
	"vectorized/pkg/utils"
	"vectorized/pkg/yaml"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
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

// Checks config and writes it to the given path.
func WriteConfig(fs afero.Fs, config *Config, path string) error {
	ok, errs := CheckConfig(config)
	if !ok {
		reasons := []string{}
		for _, err := range errs {
			reasons = append(reasons, err.Error())
		}
		return errors.New(strings.Join(reasons, ", "))
	}
	backup := fmt.Sprintf("%s.bk", path)
	exists, err := afero.Exists(fs, backup)
	if err != nil {
		return err
	}
	if exists {
		log.Debug("Removing current backup file")
		err = fs.Remove(backup)
		if err != nil {
			return err
		}
	}
	log.Debugf("Backing up the current configuration to '%s'", backup)
	err = fs.Rename(path, backup)
	if err != nil {
		return err
	}
	log.Debugf("Writing the new redpanda config to '%s'", path)
	err = yaml.Persist(fs, config, path)
	if err != nil {
		log.Debugf("Recovering the previous confing from %s", backup)
		recErr := utils.CopyFile(fs, backup, path)
		if recErr != nil {
			msg := "couldn't persist the new config due to '%v', nor recover the backup due to '%v"
			return fmt.Errorf(msg, err, recErr)
		}
		return err
	}
	return nil
}

func ReadConfigFromPath(fs afero.Fs, path string) (*Config, error) {
	log.Debugf("Reading Redpanda config file from '%s'", path)
	config := &Config{}
	err := yaml.Read(fs, config, path)
	if err != nil {
		return nil, err
	}
	config.ConfigFile = path
	return config, nil
}

func CheckConfig(config *Config) (bool, []error) {
	errs := checkRedpandaConfig(config.Redpanda)
	errs = append(
		errs,
		checkRpkConfig(config.Rpk)...,
	)
	ok := len(errs) == 0
	return ok, errs
}

func checkRedpandaConfig(config *RedpandaConfig) []error {
	errs := []error{}
	if config == nil {
		return []error{errors.New("the redpanda config is missing")}
	}
	if config.Directory == "" {
		errs = append(errs, fmt.Errorf("redpanda.data_directory can't be empty"))
	}
	if config.Id < 0 {
		errs = append(errs, fmt.Errorf("redpanda.id can't be a negative integer"))
	}
	errs = append(
		errs,
		checkSocketAddress(config.RPCServer, "redpanda.rpc_server")...,
	)
	errs = append(
		errs,
		checkSocketAddress(config.KafkaApi, "redpanda.kafka_api")...,
	)
	seedServersPath := "redpanda.seed_servers"
	if len(config.SeedServers) == 0 {
		errs = append(errs, fmt.Errorf(seedServersPath+" can't be empty"))
	} else {
		for i, seed := range config.SeedServers {
			errs = append(
				errs,
				checkSocketAddress(
					seed.Host,
					fmt.Sprintf("%s.%d.host", seedServersPath, i),
				)...,
			)
		}
	}
	return errs
}

func checkSocketAddress(socketAddr SocketAddress, configPath string) []error {
	errs := []error{}
	if socketAddr.Port == 0 {
		errs = append(errs, fmt.Errorf("%s.port can't be 0", configPath))
	}
	if socketAddr.Address == "" {
		errs = append(errs, fmt.Errorf("%s.address can't be empty", configPath))
	}
	return errs
}

func checkRpkConfig(rpk *RpkConfig) []error {
	errs := []error{}
	if rpk == nil {
		return errs
	}
	if rpk.TuneCoredump && rpk.CoredumpDir == "" {
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
