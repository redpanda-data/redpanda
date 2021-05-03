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
	"os"
	"path/filepath"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"gopkg.in/yaml.v2"
)

const (
	defaultConfigLocation = ".vcloud/config.yaml"
	configVersion         = "v1"
)

var (
	ErrConfigFileDoesNotExist = errors.New("config file does not exist")
)

// ConfigReaderWriter manages configuration file for rpk which is currently used
// to store login tokens.  Default config location is in
// $HOME/.vcloud/config.yaml
type ConfigReaderWriter interface {
	WriteToken(token string) error
	ReadToken() (string, error)
}

type vCloudReaderWriter struct {
	fs afero.Fs
}

type VCloudConfig struct {
	Version string `yaml:"version"`
	Token   string `yaml:"token,omitempty"`
}

func NewV1Config() *VCloudConfig {
	return &VCloudConfig{
		Version: configVersion,
	}
}

func NewVCloudConfigReaderWriter(fs afero.Fs) ConfigReaderWriter {
	return &vCloudReaderWriter{
		fs: fs,
	}
}

func (rw *vCloudReaderWriter) WriteToken(token string) error {
	configFile, err := rw.getOrCreateConfigFile()
	if err != nil {
		return err
	}
	configFile.Token = token
	return rw.writeConfig(configFile)
}

func (rw *vCloudReaderWriter) ReadToken() (string, error) {
	configFile, err := rw.getConfigFile()
	if err != nil {
		return "", err
	}
	return configFile.Token, nil
}

func (rw *vCloudReaderWriter) getConfigFile() (*VCloudConfig, error) {
	configLocation, err := configLocation()
	if err != nil {
		return nil, err
	}
	file, err := rw.fs.Open(configLocation)
	if os.IsNotExist(err) {
		return nil, ErrConfigFileDoesNotExist
	}
	if err != nil {
		return nil, err
	}
	b, err := afero.ReadAll(file)
	if err != nil {
		return nil, err
	}
	var config VCloudConfig
	yaml.Unmarshal(b, &config)
	return &config, nil
}

func (rw *vCloudReaderWriter) getOrCreateConfigFile() (*VCloudConfig, error) {
	config, err := rw.getConfigFile()
	if err == ErrConfigFileDoesNotExist {
		configLocation, err := configLocation()
		if err != nil {
			return nil, err
		}
		log.Debugf("config file '%s' does not exist, going to create it", configLocation)
		config := NewV1Config()
		err = rw.writeConfig(config)
		return config, err
	}
	if err != nil {
		return nil, fmt.Errorf("error reading config file. %w", err)
	}
	return config, nil
}

func configLocation() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	configLocation := filepath.Join(home, defaultConfigLocation)
	return configLocation, err
}

func (rw *vCloudReaderWriter) writeConfig(config *VCloudConfig) error {
	configLocation, err := configLocation()
	if err != nil {
		return err
	}
	dir, _ := filepath.Split(configLocation)
	err = rw.fs.MkdirAll(dir, 0700)
	if err != nil {
		return err
	}
	yaml, err := yaml.Marshal(config)
	if err != nil {
		return err
	}
	err = afero.WriteFile(rw.fs, configLocation, yaml, 0600)
	if err != nil {
		return fmt.Errorf("error writing config file. %w", err)
	}
	return nil
}
