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
	"reflect"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/icza/dyno"
	"github.com/imdario/mergo"
	"github.com/mitchellh/mapstructure"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
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
		"redpanda.rpc_server",
		"redpanda.admin",
	}
	unmarshalKey := func(key string, val interface{}) error {
		return m.v.UnmarshalKey(
			key,
			val,
			func(c *mapstructure.DecoderConfig) {
				c.TagName = "mapstructure"
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
		if k == "redpanda.advertised_kafka_api" || k == "redpanda.kafka_api" {
			addrs := []NamedSocketAddress{}
			err := unmarshalKey(k, &addrs)
			if err != nil {
				return nil, err
			}
			for i, a := range addrs {
				key := fmt.Sprintf("%s.%d", k, i)
				str := fmt.Sprintf(
					"%s:%d",
					a.Address,
					a.Port,
				)
				if a.Name != "" {
					str = fmt.Sprintf("%s://%s", a.Name, str)
				}
				flatMap[key] = str
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
	strMap := dyno.ConvertMapI2MapS(m.v.AllSettings())
	return strMap.(map[string]interface{}), nil
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
	err = m.merge(confMap)
	if err != nil {
		return err
	}
	return checkAndWrite(m.fs, m.v, conf.ConfigFile)
}

// Merge a new config map into the existing config.
// As a side effect, it "resets" m's viper instance with a new one, initialized
// through InitViper. This is needed because viper#MergeConfigMap doesn't allow
// replacing old values with new ones of a different type.
func (m *manager) merge(src map[string]interface{}) error {
	dst := m.v.AllSettings()
	merged, err := mergeMaps(dst, src)
	if err != nil {
		return err
	}
	m.v = InitViper(m.fs)
	return m.v.MergeConfigMap(merged)
}

// Merges src into dst. If the type of a value in src differs from dst's,
// dst's value's type takes precedence and replaces the value in src
// altogether. This is in contrast to viper's viper#MergeConfigMap, which
// doesn't support different types for a given key.
func mergeMaps(
	dst, src map[string]interface{},
) (map[string]interface{}, error) {
	d := dyno.ConvertMapI2MapS(dst)
	s := dyno.ConvertMapI2MapS(src)
	dd := d.(map[string]interface{})
	err := mergo.Merge(&dd, s, mergo.WithOverride)
	if err != nil {
		return nil, err
	}
	return dd, nil
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

	err = m.merge(confMap)
	if err != nil {
		return err
	}

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

	src := defaultMap()
	dst := m.v.AllSettings()
	err = dyno.SSet(src, newConfValue, strings.Split(key, ".")...)

	merged, err := mergeMaps(dst, src)
	if err != nil {
		return err
	}

	err = m.merge(merged)
	if err != nil {
		return err
	}

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
	decoderConfig := mapstructure.DecoderConfig{
		Result: result,
		// Sometimes viper will save int values as strings (i.e.
		// through BindPFlag) so we have to allow mapstructure
		// to cast them.
		WeaklyTypedInput: true,
		DecodeHook: mapstructure.ComposeDecodeHookFunc(
			v21_1_4MapToNamedSocketAddressSlice,
		),
	}
	decoder, err := mapstructure.NewDecoder(&decoderConfig)
	if err != nil {
		return nil, err
	}
	err = decoder.Decode(v.AllSettings())
	if err != nil {
		return nil, err
	}
	result.ConfigFile, err = absPath(v.ConfigFileUsed())
	if err != nil {
		return nil, err
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
		switch from.Kind() {
		case reflect.Map:
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
