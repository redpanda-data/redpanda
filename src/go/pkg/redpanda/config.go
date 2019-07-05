package redpanda

import (
	"vectorized/yaml"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
)

type Config struct {
	Directory   string
	Port        int
	Ip          string
	Id          int
	SeedServers []*SeedServer `yaml:"seed_servers"`
}

type SeedServer struct {
	Address string `yaml:"addr"`
	Id      int
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
