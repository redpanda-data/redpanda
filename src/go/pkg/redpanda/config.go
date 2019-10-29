package redpanda

import (
	"fmt"
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
	TuneNetwork         bool `yaml:"tune_network"`
	TuneDiskScheduler   bool `yaml:"tune_disk_scheduler"`
	TuneNomerges        bool `yaml:"tune_disk_nomerges"`
	TuneDiskIrq         bool `yaml:"tune_disk_irq"`
	TuneCpu             bool `yaml:"tune_cpu"`
	TuneAioEvents       bool `yaml:"tune_aio_events"`
	TuneClocksource     bool `yaml:"tune_clocksource"`
	EnableMemoryLocking bool `yaml:"enable_memory_locking"`
}

func WriteConfig(config *Config, fs afero.Fs, path string) error {
	log.Debugf("Writing Redpanda config file to '%s'", path)
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
	ok := len(errs) == 0
	return ok, errs
}

func checkRedpandaConfig(config *RedpandaConfig) []error {
	errs := []error{}
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
