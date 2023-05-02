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
	"github.com/spf13/afero"
	"go.uber.org/zap"
)

const (
	DefaultKafkaPort     = 9092
	DefaultSchemaRegPort = 8081
	DefaultProxyPort     = 8082
	DefaultAdminPort     = 9644
	DefaultRPCPort       = 33145
	DefaultListenAddress = "0.0.0.0"

	DefaultBallastFilePath = "/var/lib/redpanda/data/ballast"
	DefaultBallastFileSize = "1GiB"
)

// Config encapsulates a redpanda.yaml and/or an rpk.yaml. This is the
// entrypoint that params.Config returns, after which you can get either the
// materialized or actual configurations.
type Config struct {
	configFlag string // --config, for rare use cases -- see below

	redpandaYaml       RedpandaYaml // processed, defaults/env/flags
	redpandaYamlActual RedpandaYaml // unprocessed
	redpandaYamlExists bool         // whether the redpanda.yaml file exists

	rpkYaml       RpkYaml // processed, defaults/env/flags
	rpkYamlActual RpkYaml // unprocessed
	rpkYamlExists bool    // whether the rpk.yaml file exists

	logger *zap.Logger
}

// MaterializedRedpandaYaml returns a redpanda.yaml, starting with defaults,
// then decoding a potential file, then applying env vars and then flags.
func (c *Config) MaterializedRedpandaYaml() *RedpandaYaml {
	return &c.redpandaYaml
}

// ActualRedpandaYaml returns an actual redpanda.yaml if it exists, with no
// other defaults over overrides applied.
func (c *Config) ActualRedpandaYaml() (*RedpandaYaml, bool) {
	return &c.redpandaYamlActual, c.redpandaYamlExists
}

// ActualRedpandaYamlOrDefaults returns an actual redpanda.yaml if it exists,
// otherwise this returns dev defaults. This function is meant to be used
// for writing a redpanda.yaml file, populating it with defaults if needed.
func (c *Config) ActualRedpandaYamlOrDefaults() *RedpandaYaml {
	if c.redpandaYamlExists {
		return &c.redpandaYamlActual
	}
	redpandaYaml := DevDefault()
	if c.configFlag != "" { // --config set but the file does not yet exist
		redpandaYaml.fileLocation = c.configFlag
	}
	return redpandaYaml
}

// MaterializedRpkYaml returns an rpk.yaml, starting with defaults, then
// decoding a potential file, then applying env vars and then flags.
func (c *Config) MaterializedRpkYaml() *RpkYaml {
	return &c.rpkYaml
}

// MaterializedContext returns an rpk.yaml's current materialized context,
// starting with defaults, then decoding a potential file, then applying env
// vars and then flags. This always returns non-nil due to a guaranee from
// Params.Load.
func (c *Config) MaterializedContext() *RpkContext {
	return c.rpkYaml.Context(c.rpkYaml.CurrentContext)
}

// ActualRpkYaml returns an actual rpk.yaml if it exists, with no other
// defaults over overrides applied.
func (c *Config) ActualRpkYaml() (*RpkYaml, bool) {
	return &c.rpkYamlActual, c.rpkYamlExists
}

// ActualRpkYamlOrDefaults returns an actual rpk.yaml if it exists, otherwise
// this returns a blank rpk.yaml. If this function tries to return a default
// rpk.yaml but cannot read the user config dir, this returns an error.
func (c *Config) ActualRpkYamlOrEmpty() (*RpkYaml, error) {
	if c.rpkYamlExists {
		return &c.rpkYamlActual, nil
	}
	rpkYaml := emptyMaterializedRpkYaml()
	if c.configFlag != "" {
		rpkYaml.fileLocation = c.configFlag
	} else {
		path, err := DefaultRpkYamlPath()
		if err != nil {
			return nil, err
		}
		rpkYaml.fileLocation = path
	}
	return &rpkYaml, nil
}

// LoadMaterializedRedpandaYaml is a shortcut for p.Load followed by
// cfg.MaterializedRedpandaYaml.
func (p *Params) LoadMaterializedRedpandaYaml(fs afero.Fs) (*RedpandaYaml, error) {
	cfg, err := p.Load(fs)
	if err != nil {
		return nil, err
	}
	return cfg.MaterializedRedpandaYaml(), nil
}

// LoadActualRedpandaYaml is a shortcut for p.Load followed by
// cfg.ActualRedpandaYaml.
func (p *Params) LoadActualRedpandaYamlOrDefaults(fs afero.Fs) (*RedpandaYaml, error) {
	cfg, err := p.Load(fs)
	if err != nil {
		return nil, err
	}
	return cfg.ActualRedpandaYamlOrDefaults(), nil
}

// LoadMaterializedContext is a shortcut for p.Load followed by
// cfg.MaterializedContext.
func (p *Params) LoadMaterializedContext(fs afero.Fs) (*RpkContext, error) {
	cfg, err := p.Load(fs)
	if err != nil {
		return nil, err
	}
	return cfg.MaterializedContext(), nil
}
