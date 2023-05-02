// Copyright 2023 Redpanda Data, Inc.
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
	"reflect"

	"github.com/spf13/afero"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"

	rpkos "github.com/redpanda-data/redpanda/src/go/rpk/pkg/os"
)

// DefaultRpkYamlPath returns the OS equivalent of ~/.config/rpk/rpk.yaml, if
// $HOME is defined. The returned path is an absolute path.
func DefaultRpkYamlPath() (string, error) {
	configDir, err := os.UserConfigDir()
	if err != nil {
		return "", errors.New("unable to load the user config directory -- is $HOME unset?")
	}
	return filepath.Join(configDir, "rpk", "rpk.yaml"), nil
}

func defaultMaterializedRpkYaml() RpkYaml {
	return RpkYaml{
		CurrentContext: "default",
		Contexts: []RpkContext{{
			Name:        "default",
			Description: "Default rpk context",
		}},
		Version: 1,
	}
}

func emptyMaterializedRpkYaml() RpkYaml {
	return RpkYaml{
		Version: 1,
	}
}

type (
	// RpkYaml contains the configuration for ~/.config/rpk/config.yml, the
	// next generation of rpk's configuration file.
	RpkYaml struct {
		fileLocation string
		fileRaw      []byte

		Version        int            `yaml:"version"`
		CurrentContext string         `yaml:"current_context"`
		Contexts       []RpkContext   `yaml:"contexts,omitempty"`
		CloudAuths     []RpkCloudAuth `yaml:"cloud_auth,omitempty"`
		Tuners         RpkNodeTuners  `yaml:"tuners,omitempty"`
	}

	RpkContext struct {
		Name         string           `yaml:"name,omitempty"`
		Description  string           `yaml:"description,omitempty"`
		CloudCluster *RpkCloudCluster `yaml:"cloud_cluster,omitempty"`
		KafkaAPI     RpkKafkaAPI      `yaml:"kafka_api,omitempty"`
		AdminAPI     RpkAdminAPI      `yaml:"admin_api,omitempty"`

		// We stash the config struct itself so that we can provide
		// the Logger.
		c *Config
	}

	RpkCloudCluster struct {
		Namespace string `yaml:"namespace"`
		Cluster   string `yaml:"cluster"`
		Auth      string `yaml:"auth"`
	}

	RpkCloudAuth struct {
		Token          string `yaml:"token,omitempty"`
		RefreshToken   string `yaml:"refresh_token,omitempty"`
		ClientID       string `yaml:"client_id,omitempty"`
		ClientSecret   string `yaml:"client_secret,omitempty"`
		OrganizationID string `yaml:"organization,omitempty"`
		Name           string `yaml:"name,omitempty"`
	}
)

// ContextReturns the given context, or nil if it doesn't exist.
func (y *RpkYaml) Context(name string) *RpkContext {
	for i, cx := range y.Contexts {
		if cx.Name == name {
			return &y.Contexts[i]
		}
	}
	return nil
}

///////////
// FUNCS //
///////////

// Logger returns the logger for the original configuration, or a nop logger if
// it was invalid.
func (cx *RpkContext) Logger() *zap.Logger {
	return cx.c.logger
}

// SugarLogger returns Logger().Sugar().
func (cx *RpkContext) SugarLogger() *zap.SugaredLogger {
	return cx.c.logger.Sugar()
}

// Returns if the raw config is the same as the one in memory.
func (y *RpkYaml) isTheSameAsRawFile() bool {
	var init, final *RpkYaml
	if err := yaml.Unmarshal(y.fileRaw, &init); err != nil {
		return false
	}
	// Avoid DeepEqual comparisons on non-exported fields.
	finalRaw, err := yaml.Marshal(y)
	if err != nil {
		return false
	}
	if err := yaml.Unmarshal(finalRaw, &final); err != nil {
		return false
	}
	return reflect.DeepEqual(init, final)
}

// Write writes the configuration at the previously loaded path, or the default
// path.
func (y *RpkYaml) Write(fs afero.Fs) error {
	if y.isTheSameAsRawFile() {
		return nil
	}
	location := y.fileLocation
	if location == "" {
		def, err := DefaultRpkYamlPath()
		if err != nil {
			return err
		}
		location = def
	}
	return y.WriteAt(fs, location)
}

// WriteAt writes the configuration to the given path.
func (y *RpkYaml) WriteAt(fs afero.Fs, path string) error {
	b, err := yaml.Marshal(y)
	if err != nil {
		return fmt.Errorf("marshal error in loaded config, err: %s", err)
	}
	return rpkos.ReplaceFile(fs, path, b, 0o644)
}
