// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package auth

import (
	"fmt"
	"os"
	"path/filepath"

	os2 "github.com/redpanda-data/redpanda/src/go/rpk/pkg/os"
	"gopkg.in/yaml.v3"
)

// Config represents the cloud configuration used to log in to the Redpanda
// Cloud.
type Config struct {
	path string

	ClientID     string `yaml:"client_id,omitempty"`
	ClientSecret string `yaml:"client_secret,omitempty"`
	AuthToken    string `yaml:"auth_token,omitempty"`
}

// defaultCfgPath returns the default path where the cloud configuration will
// live, normally: '$HOME/.config/rpk/__cloud.yaml'.
func defaultCfgPath() (string, error) {
	dir, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("unable to get home dir: %w", err)
	}
	return filepath.Join(dir, ".config", "rpk", "__cloud.yaml"), nil
}

// LoadConfig loads the cloud configuration in the default path.
func LoadConfig() (*Config, error) {
	if os2.IsRunningSudo() {
		return nil, fmt.Errorf("detected rpk is running with sudo; please execute this command without sudo to avoid saving the cloud configuration as a root owned file")
	}
	path, err := defaultCfgPath()
	if err != nil {
		return nil, err
	}

	contents, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("unable to load config file at path %s: %w", path, err)
	}

	var cfg Config
	if err := yaml.Unmarshal(contents, &cfg); err != nil {
		return nil, fmt.Errorf("unable to decode config file at path %s: %w", path, err)
	}

	cfg.path = path

	return &cfg, nil
}

// Save writes the config to disk at its previously specified path or at the
// default path if the file does not exist yet.
func (c *Config) Save() error {
	contents, err := yaml.Marshal(c)
	if err != nil {
		return fmt.Errorf("unable to encode config file: %w", err)
	}

	if c.path == "" {
		path, err := defaultCfgPath()
		if err != nil {
			return err
		}
		c.path = path
	}
	dir := filepath.Dir(c.path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("unable to create config dir %s: %v", dir, err)
	}
	if err := os.WriteFile(c.path, contents, 0o600); err != nil {
		return fmt.Errorf("unable to write config file %s: %v", c.path, err)
	}
	fmt.Printf("Configuration saved to %q.\n", c.path)
	return nil
}

// Pretty returns a string with the configuration formatted in a human-readable
// form.
func (c *Config) Pretty() string {
	format := `
Client ID:            %s 
Client Secret:        %s
Authorization Token:  %s
`
	return fmt.Sprintf(format, c.ClientID, c.ClientSecret, c.AuthToken)
}
