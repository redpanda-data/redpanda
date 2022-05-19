// Copyright 2020 Redpanda Data, Inc.
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
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/icza/dyno"
	"github.com/mitchellh/mapstructure"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/utils"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"github.com/spf13/viper"
	"gopkg.in/yaml.v2"
)

type Manager interface {
	// Reads the config from the given path
	Read(path string) (*Config, error)
	// Writes the config to Config.ConfigFile
	Write(conf *Config) error
	// Writes the currently-loaded config to redpanda.config_file
	WriteLoaded() error
	// Get the currently-loaded config
	Get() (*Config, error)
	// Sets key to the given value (parsing it according to the format)
	Set(key, value, format string) error
	// If path is empty, tries to find the file in the default locations.
	// Otherwise, it tries to read the file and load it. If the file doesn't
	// exist, it tries to create it with the default configuration.
	FindOrGenerate(path string) (*Config, error)
	// Tries reading a config file at the given path, or tries to find it in
	// the default locations if it doesn't exist.
	ReadOrFind(path string) (*Config, error)
	// Reads the configuration as JSON
	ReadAsJSON(path string) (string, error)
	// Generates and writes the node's UUID
	WriteNodeUUID(conf *Config) error
	// Merges an input config to the currently-loaded map
	Merge(conf *Config) error
}

type manager struct {
	fs afero.Fs
	v  *viper.Viper
}

func NewManager(fs afero.Fs) Manager {
	return &manager{fs, InitViper(fs)}
}

func (m *manager) FindOrGenerate(path string) (*Config, error) {
	if path == "" {
		addConfigPaths(m.v)
		err := m.v.ReadInConfig()
		if err == nil {
			conf, err := unmarshal(m.v)
			if err != nil {
				return nil, err
			}
			conf.ConfigFile, err = absPath(m.v.ConfigFileUsed())
			return conf, err
		}
		_, notFound := err.(viper.ConfigFileNotFoundError) //nolint:errorlint // Viper returns a non-pointer error https://github.com/spf13/viper/issues/1139
		if !notFound {
			return nil, err
		}
		path = Default().ConfigFile
	}
	return readOrGenerate(m.fs, m.v, path)
}

// Tries reading a config file at the given path, or generates a default config
// and writes it to the path.
func readOrGenerate(fs afero.Fs, v *viper.Viper, path string) (*Config, error) {
	abs, err := absPath(path)
	if err != nil {
		return nil, err
	}
	v.SetConfigFile(abs)
	err = v.ReadInConfig()
	if err == nil {
		// The config file's there, there's nothing to do.
		return unmarshal(v)
	}
	_, notFound := err.(viper.ConfigFileNotFoundError) //nolint:errorlint // Viper returns a non-pointer error https://github.com/spf13/viper/issues/1139
	notExist := os.IsNotExist(err)
	if err != nil && !notFound && !notExist {
		return nil, fmt.Errorf(
			"An error happened while trying to read %s: %v",
			abs,
			err,
		)
	}
	log.Debug(err)
	log.Infof(
		"Couldn't find config file at %s. Generating it.",
		abs,
	)
	v.Set("config_file", abs)
	err = createConfigDir(fs, abs)
	if err != nil {
		return nil, err
	}
	err = v.WriteConfigAs(abs)
	if err != nil {
		return nil, fmt.Errorf(
			"Couldn't write config to %s: %v",
			abs,
			err,
		)
	}
	return unmarshal(v)
}

func (m *manager) ReadOrFind(path string) (*Config, error) {
	var err error
	if path == "" {
		path, err = FindConfigFile(m.fs)
		if err != nil {
			return nil, err
		}
	}
	return m.Read(path)
}

func (m *manager) ReadAsJSON(path string) (string, error) {
	confMap, err := m.readMap(path)
	if err != nil {
		return "", err
	}
	confJSON, err := json.Marshal(confMap)
	if err != nil {
		return "", err
	}
	return string(confJSON), nil
}

func (m *manager) Read(path string) (*Config, error) {
	// If the path was set, try reading only from there.
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	m.v.SetConfigFile(abs)
	err = m.v.ReadInConfig()
	if err != nil {
		return nil, err
	}
	conf, err := unmarshal(m.v)
	if err != nil {
		return nil, err
	}
	conf.ConfigFile, err = absPath(m.v.ConfigFileUsed())
	return conf, err
}

func (m *manager) readMap(path string) (map[string]interface{}, error) {
	m.v.SetConfigFile(path)
	err := m.v.ReadInConfig()
	if err != nil {
		return nil, err
	}
	strMap := dyno.ConvertMapI2MapS(m.v.AllSettings())
	return strMap.(map[string]interface{}), nil
}

func (m *manager) WriteNodeUUID(conf *Config) error {
	id, err := uuid.NewUUID()
	if err != nil {
		return err
	}
	conf.NodeUUID = id.String()
	return m.Write(conf)
}

func (m *manager) Get() (*Config, error) {
	return unmarshal(m.v)
}

// Checks config and writes it to the given path.
func (m *manager) Write(conf *Config) error {
	confMap, err := toMap(conf)
	if err != nil {
		return err
	}
	// Merge the config into a new viper.Viper instance to prevent
	// concurrent writes to the underlying config map.
	v := InitViper(m.fs)
	current, err := unmarshal(m.v)
	if err != nil {
		return err
	}
	currentMap, err := toMap(current)
	if err != nil {
		return err
	}
	v.MergeConfigMap(currentMap)
	v.MergeConfigMap(confMap)
	return checkAndWrite(m.fs, v, conf.ConfigFile)
}

// Writes the currently loaded config.
func (m *manager) WriteLoaded() error {
	return checkAndWrite(m.fs, m.v, m.v.GetString("config_file"))
}

func write(fs afero.Fs, v *viper.Viper, path string) error {
	err := createConfigDir(fs, path)
	if err != nil {
		return err
	}
	err = v.WriteConfigAs(path)
	if err != nil {
		return err
	}
	log.Debugf(
		"Configuration written to %s.",
		path,
	)
	return nil
}

func (m *manager) setDeduceFormat(key, value string) error {
	replace := func(key string, newValue interface{}) error {
		newV := viper.New()
		newV.Set(key, newValue)
		return m.v.MergeConfigMap(newV.AllSettings())
	}

	var newVal interface{}
	switch {
	case json.Unmarshal([]byte(value), &newVal) == nil: // Try JSON
		return replace(key, newVal)

	case yaml.Unmarshal([]byte(value), &newVal) == nil: // Try YAML
		return replace(key, newVal)

	default: // Treat the value as a "single"
		m.v.Set(key, parse(value))
		return nil
	}
}

func (m *manager) Set(key, value, format string) error {
	if key == "" {
		return errors.New("empty config field key")
	}
	if format == "" {
		return m.setDeduceFormat(key, value)
	}
	var newConfValue interface{}
	switch strings.ToLower(format) {
	case "single":
		m.v.Set(key, parse(value))
		return nil
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
	return m.v.MergeConfigMap(newV.AllSettings())
}

func (m *manager) Merge(conf *Config) error {
	confMap, err := toMap(conf)
	if err != nil {
		return err
	}
	return m.v.MergeConfigMap(confMap)
}

func checkAndWrite(fs afero.Fs, v *viper.Viper, path string) error {
	ok, errs := check(v)
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
		return write(fs, v, path)
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
	log.Debugf("Writing the new redpanda config to '%s'", path)
	err = write(fs, v, path)
	if err != nil {
		return recover(fs, backup, path, err) //nolint:revive // false positive: this recover function is different from built-in recover
	}
	return nil
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

func unmarshal(v *viper.Viper) (*Config, error) {
	result := &Config{}
	decoderConfig := decoderConfig()
	decoderConfig.Result = result
	decoder, err := mapstructure.NewDecoder(&decoderConfig)
	if err != nil {
		return nil, err
	}
	err = decoder.Decode(v.AllSettings())
	if err != nil {
		return nil, err
	}
	if result.ConfigFile == "" {
		result.ConfigFile, err = absPath(v.ConfigFileUsed())
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

// Redpanda version < 21.1.4 only supported a single anonymous listener and a
// single anonymous advertised address. This custom decode function translates
// a single SocketAddress-equivalent map[string]interface{} into a
// []NamedSocketAddress.
func v21_1_4MapToNamedSocketAddressSlice(
	from, to reflect.Type, data interface{},
) (interface{}, error) {
	if to == reflect.TypeOf([]NamedSocketAddress{}) {
		if from.Kind() == reflect.Map {
			sa := NamedSocketAddress{}
			err := mapstructure.Decode(data, &sa)
			if err != nil {
				return nil, err
			}
			return []NamedSocketAddress{sa}, nil
		}
	}
	return data, nil
}

// Redpanda version <= 21.4.1 only supported a single TLS config. This custom
// decode function translates a single TLS config-equivalent
// map[string]interface{} into a []ServerTLS.
//nolint:revive // using underscore here is intended
func v21_4_1TlsMapToNamedTlsSlice(
	from, to reflect.Type, data interface{},
) (interface{}, error) {
	if to == reflect.TypeOf([]ServerTLS{}) {
		if from.Kind() == reflect.Map {
			tls := ServerTLS{}
			err := mapstructure.Decode(data, &tls)
			if err != nil {
				return nil, err
			}
			return []ServerTLS{tls}, nil
		}
	}
	return data, nil
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

func absPath(path string) (string, error) {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf(
			"couldn't convert the used config file path to"+
				" absolute: %s",
			path,
		)
	}
	return absPath, nil
}

func createConfigDir(fs afero.Fs, configFile string) error {
	dir := filepath.Dir(configFile)
	err := fs.MkdirAll(dir, 0o755)
	if err != nil {
		return fmt.Errorf(
			"couldn't create config dir %s: %v",
			dir,
			err,
		)
	}
	return nil
}
