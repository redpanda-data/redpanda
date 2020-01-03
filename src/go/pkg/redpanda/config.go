package redpanda

import (
	"errors"
	"fmt"
	"strings"
	"vectorized/pkg/yaml"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
)

type Config struct {
	Redpanda *RedpandaConfig
	Rpk      *RpkConfig `yaml:"rpk,omitempty"`
}

type RedpandaConfig struct {
	Directory   string        `yaml:"data_directory"`
	RPCServer   SocketAddress `yaml:"rpc_server"`
	KafkaApi    SocketAddress `yaml:"kafka_api"`
	Id          int           `yaml:"node_id"`
	SeedServers []*SeedServer `yaml:"seed_servers"`
}

type SeedServer struct {
	Host SocketAddress `yaml:"host"`
	Id   int           `yaml:"node_id"`
}

type SocketAddress struct {
	Address string `yaml:"address"`
	Port    int    `yaml:"port"`
}

type RpkConfig struct {
	TuneNetwork         bool   `yaml:"tune_network"`
	TuneDiskScheduler   bool   `yaml:"tune_disk_scheduler"`
	TuneNomerges        bool   `yaml:"tune_disk_nomerges"`
	TuneDiskIrq         bool   `yaml:"tune_disk_irq"`
	TuneCpu             bool   `yaml:"tune_cpu"`
	TuneAioEvents       bool   `yaml:"tune_aio_events"`
	TuneClocksource     bool   `yaml:"tune_clocksource"`
	TuneSwappiness      bool   `yaml:"tune_swappiness"`
	EnableMemoryLocking bool   `yaml:"enable_memory_locking"`
	TuneCoredump        bool   `yaml:"tune_coredump"`
	CoredumpDir         string `yaml:"coredump_dir"`
	WellKnownIo         string `yaml:"well_known_io,omitempty"`
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
	log.Debugf("Writing redpanda config file to '%s'", path)
	return yaml.Persist(fs, config, path)
}

func ReadConfigFromPath(fs afero.Fs, path string) (*Config, error) {
	log.Debugf("Reading Redpanda config file from '%s'", path)
	config := &Config{}
	err := yaml.Read(fs, config, path)
	if err != nil {
		return nil, err
	}
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
