// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package cloudcfg

import (
	"fmt"
	"os"
	"path/filepath"

	rpkos "github.com/redpanda-data/redpanda/src/go/rpk/pkg/os"
	"github.com/spf13/afero"
	"gopkg.in/yaml.v3"
)

type fileCfg struct {
	clientID     string
	clientSecret string
	authToken    string
}

// Config represents the cloud configuration used to log in to the Redpanda
// Cloud.
type Config struct {
	path   string
	file   fileCfg
	exists bool

	ClientID     string `yaml:"client_id,omitempty"`
	ClientSecret string `yaml:"client_secret,omitempty"`
	AuthToken    string `yaml:"auth_token,omitempty"`

	AuthURL          string `yaml:"-"`
	AuthAudience     string `yaml:"-"`
	CloudURL         string `yaml:"-"`
	SkipVersionCheck string `yaml:"-"`
}

func (c *Config) fileCfg() fileCfg {
	return fileCfg{
		clientID:     c.ClientID,
		clientSecret: c.ClientSecret,
		authToken:    c.AuthToken,
	}
}

// Exists returns true if the config was loaded from an existing file.
func (c *Config) Exists() bool { return c.exists }

// HasClientCredentials returns if both ClientID and ClientSecret are empty.
func (c *Config) HasClientCredentials() bool { return c.ClientID != "" && c.ClientSecret != "" }

// ClearCredentials sets the in-memory credentials to it's zero value.
func (c *Config) ClearCredentials() {
	c.ClientID = ""
	c.ClientSecret = ""
	c.AuthToken = ""
}

// defaultCfgPath returns the default path where the cloud configuration will
// live, normally: '$HOME/.config/rpk/__cloud.yaml'.
func defaultCfgPath() (string, error) {
	dir, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("unable to get home dir: %w", err)
	}
	return filepath.Join(dir, "rpk", "__cloud.yaml"), nil
}

// SaveAll saves the in-memory client ID, client secret, and token to the
// config file.
func (c *Config) SaveAll(fs afero.Fs) error {
	if c.fileCfg() == c.file {
		return nil // no changes
	}
	return c.save(fs)
}

// SaveToken saves only the in-memory token to the config file, preserving the
// previous client ID and secret if they existed.
func (c *Config) SaveToken(fs afero.Fs) error {
	if c.AuthToken == c.file.authToken {
		return nil // no changes
	}
	return (&Config{
		path:         c.path,
		ClientID:     c.file.clientID,
		ClientSecret: c.file.clientSecret,
		AuthToken:    c.AuthToken,
	}).save(fs)
}

// save writes the config to disk at its previously specified path or at the
// default path if the file does not exist yet.
func (c *Config) save(fs afero.Fs) error {
	contents, err := yaml.Marshal(c)
	if err != nil {
		return fmt.Errorf("unable to encode config file: %w", err)
	}
	path := c.path
	if path == "" {
		path, err = defaultCfgPath()
		if err != nil {
			return err
		}
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("unable to create config dir %s: %v", dir, err)
	}
	return rpkos.ReplaceFile(fs, path, contents, 0o600)
}
