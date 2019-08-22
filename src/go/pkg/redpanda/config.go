package redpanda

import (
	"vectorized/pkg/yaml"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
)

type Config struct {
	Directory   string        `yaml:"data_directory"`
	RPCServer   SocketAddress `yaml:"rpc_server"`
	KafkaApi    SocketAddress `yaml:"kafka_api"`
	Id          int           `yaml:"node_id"`
	SeedServers []*SeedServer `yaml:"seed_servers"`
}

type SocketAddress struct {
	Address string
	Port    int
}

type SeedServer struct {
	Host SocketAddress `yaml:"host"`
	Id   int           `yaml:"node_id"`
}

type configRoot struct {
	Redpanda *Config
}

func WriteConfig(config *Config, fs afero.Fs, path string) error {
	log.Debugf("Writing Redpanda config file to '%s'", path)
	configRoot := configRoot{
		Redpanda: config,
	}
	return yaml.Persist(fs, configRoot, path)
}

func ReadConfigFromPath(fs afero.Fs, path string) (*Config, error) {
	log.Debugf("Reading Redpanda config file from '%s'", path)
	configRoot := configRoot{}
	err := yaml.Read(fs, &configRoot, path)
	if err != nil {
		return nil, err
	}
	return configRoot.Redpanda, nil
}

func CheckConfig(config *Config) bool {
	if config.Directory == "" ||
		config.RPCServer.Port == 0 ||
		config.RPCServer.Address == "" ||
		config.Id < 0 ||
		config.KafkaApi.Port == 0 ||
		config.KafkaApi.Address == "" ||
		len(config.SeedServers) == 0 {
		return false
	}
	return true
}
