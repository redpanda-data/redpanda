package redpanda

import (
	"vectorized/pkg/yaml"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
)

type Config struct {
	Redpanda *RedpandaConfig
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

func CheckConfig(config *Config) bool {
	if config.Redpanda.Directory == "" ||
		config.Redpanda.RPCServer.Port == 0 ||
		config.Redpanda.RPCServer.Address == "" ||
		config.Redpanda.Id < 0 ||
		config.Redpanda.KafkaApi.Port == 0 ||
		config.Redpanda.KafkaApi.Address == "" ||
		len(config.Redpanda.SeedServers) == 0 {
		return false
	}
	return true
}
