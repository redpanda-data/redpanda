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
	fp "path/filepath"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/mitchellh/mapstructure"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/utils"
	"gopkg.in/yaml.v2"
)

type configConverter interface {
	ToGeneric() (*Config, error)
}

type Manager interface {
	// Reads the config from the given path
	Read(path string) (*Config, error)
	// Writes the config to Config.ConfigFile
	Write(conf *Config) error
	// Reads the config from path, sets key to the given value (parsing it
	// according to the format), and writes the config back
	Set(key, value, format, path string) error
	// If path is empty, tries to find the file in the default locations.
	// Otherwise, it tries to read the file and load it. If the file doesn't
	// exist, it tries to create it with the default configuration.
	FindOrGenerate(path string) (*Config, error)
	// Tries reading a config file at the given path, or generates a default config
	// and writes it to the path.
	ReadOrGenerate(path string) (*Config, error)
	// Tries reading a config file at the given path, or tries to find it in
	// the default locations if it doesn't exist.
	ReadOrFind(path string) (*Config, error)
	// Reads the config and returns a map where the keys are the flattened
	// paths.
	// e.g. "redpanda.tls.key_file" => "value"
	ReadFlat(path string) (map[string]string, error)
	// Reads the configuration as JSON
	ReadAsJSON(path string) (string, error)
	// Generates and writes the node's UUID
	WriteNodeUUID(conf *Config) error
	// Binds a flag's value to a configuration key.
	BindFlag(key string, flag *pflag.Flag) error
}

type manager struct {
	fs	afero.Fs
	v	*viper.Viper
}

func NewManager(fs afero.Fs) Manager {
	return &manager{fs, InitViper(fs)}
}

func (m *manager) FindOrGenerate(path string) (*Config, error) {
	if path == "" {
		addConfigPaths(m.v)
		err := m.v.ReadInConfig()
		if err != nil {
			_, notFound := err.(viper.ConfigFileNotFoundError)
			if !notFound {
				return nil, err
			}
			path = Default().ConfigFile
		} else {
			conf, err := unmarshal(m.v)
			if err != nil {
				return nil, err
			}
			conf.ConfigFile, err = absPath(m.v.ConfigFileUsed())
			return conf, err
		}

	}
	return m.ReadOrGenerate(path)
}

func (m *manager) ReadOrGenerate(path string) (*Config, error) {
	m.v.SetConfigFile(path)
	err := m.v.ReadInConfig()
	if err == nil {
		// The config file's there, there's nothing to do.
		return unmarshal(m.v)
	}
	_, notFound := err.(viper.ConfigFileNotFoundError)
	notExist := os.IsNotExist(err)
	if err != nil && !notFound && !notExist {
		return nil, fmt.Errorf(
			"An error happened while trying to read %s: %v",
			path,
			err,
		)
	}
	log.Debug(err)
	log.Infof(
		"Couldn't find config file at %s. Generating it.",
		path,
	)
	err = m.v.WriteConfigAs(path)
	if err != nil {
		return nil, fmt.Errorf(
			"Couldn't write config to %s: %v",
			path,
			err,
		)
	}
	return unmarshal(m.v)
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

func (m *manager) ReadFlat(path string) (map[string]string, error) {
	m.v.SetConfigFile(path)
	err := m.v.ReadInConfig()
	if err != nil {
		return nil, err
	}
	keys := m.v.AllKeys()
	flatMap := map[string]string{}
	compactAddrFields := []string{
		"redpanda.kafka_api",
		"redpanda.rpc_server",
		"redpanda.admin",
	}
	unmarshalKey := func(key string, val interface{}) error {
		return m.v.UnmarshalKey(
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
			for i, s := range *seeds {
				key := fmt.Sprintf("%s.%d", k, i)
				flatMap[key] = fmt.Sprintf(
					"%s:%d",
					s.Host.Address,
					s.Host.Port,
				)
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

		s := m.v.GetString(k)
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
	abs, err := fp.Abs(path)
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
	return m.v.AllSettings(), nil
}

func (m *manager) WriteNodeUUID(conf *Config) error {
	id, err := uuid.NewUUID()
	if err != nil {
		return err
	}
	conf.NodeUuid = base58Encode(id.String())
	return m.Write(conf)
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
	v.MergeConfigMap(m.v.AllSettings())
	v.MergeConfigMap(confMap)
	return checkAndWrite(m.fs, v, conf.ConfigFile)
}

func write(v *viper.Viper, path string) error {
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

func (m *manager) Set(key, value, format, path string) error {
	confMap, err := m.readMap(path)
	if err != nil {
		return err
	}
	m.v.MergeConfigMap(confMap)
	var newConfValue interface{}
	switch strings.ToLower(format) {
	case "single":
		m.v.Set(key, parse(value))
		err = checkAndWrite(m.fs, m.v, path)
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
	m.v.MergeConfigMap(newV.AllSettings())
	err = checkAndWrite(m.fs, m.v, path)
	if err == nil {
		checkAndPrintRestartWarning(key)
	}
	return err
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
	lastBackupFile, err := findBackup(fs, fp.Dir(path))
	if err != nil {
		return err
	}
	exists, err := afero.Exists(fs, path)
	if err != nil {
		return err
	}
	if !exists {
		// If the config doesn't exist, just write it.
		return write(v, path)
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
	err = write(v, path)
	if err != nil {
		return recover(fs, backup, path, err)
	}
	return nil
}

func (m *manager) BindFlag(key string, flag *pflag.Flag) error {
	return m.v.BindPFlag(key, flag)
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
	v2214conf := &v2114Config{}
	err := v.Unmarshal(v2214conf)
	if err != nil {
		return nil, fmt.Errorf(
			"Couldn't parse config as v21.1.4: %w",
			err,
		)
	}
	conf, err := v2214conf.ToGeneric()
	if err != nil {
		return nil, err
	}
	conf.ConfigFile, err = absPath(v.ConfigFileUsed())
	return conf, err
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
	absPath, err := fp.Abs(path)
	if err != nil {
		return "", fmt.Errorf(
			"Couldn't convert the used config file path to"+
				" absolute: %s",
			path,
		)
	}
	return absPath, nil
}
