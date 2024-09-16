// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package project

import (
	"fmt"
	"strings"

	"github.com/spf13/afero"
	"gopkg.in/yaml.v3"
)

type WasmLang string

const (
	WasmLangTinygoWithGoroutines WasmLang = "tinygo-with-goroutines"
	WasmLangTinygoNoGoroutines   WasmLang = "tinygo-no-goroutines"
	WasmLangRust                 WasmLang = "rust"
	WasmLangJavaScript           WasmLang = "javascript"
	WasmLangTypeScript           WasmLang = "typescript"
)

var AllWasmLangs = []string{
	string(WasmLangTinygoNoGoroutines),
	string(WasmLangTinygoWithGoroutines),
	string(WasmLangRust),
	string(WasmLangJavaScript),
	string(WasmLangTypeScript),
}

// AllWasmLangsWithDescriptions is AllWasmLangs but with extended descriptions.
//
// The order here must match AllWasmLangs.
var AllWasmLangsWithDescriptions = []string{
	"Tinygo (no goroutines) - higher throughput",
	"Tinygo (with goroutines)",
	"Rust",
	"JavaScript",
	"TypeScript",
}

func (l *WasmLang) Set(str string) error {
	lower := strings.ToLower(str)
	// For compatibility with old projects, we map bare `tinygo` to the version
	// without goroutines.
	if lower == "tinygo" {
		*l = WasmLangTinygoNoGoroutines
		return nil
	}
	for _, s := range AllWasmLangs {
		if lower == s {
			*l = WasmLang(s)
			return nil
		}
	}
	return fmt.Errorf("unknown language: %q", str)
}

func (l WasmLang) String() string {
	return string(l)
}

func (WasmLang) Type() string {
	return "string"
}

type Config struct {
	Name         string            `yaml:"name"`
	InputTopic   string            `yaml:"input-topic"`
	OutputTopics []string          `yaml:"output-topics"`
	Language     WasmLang          `yaml:"language"`
	Env          map[string]string `yaml:"env,omitempty"`
	Compression  string            `yaml:"compression,omitempty"`
	FromOffset   string            `yaml:"from-offset,omitempty"`
}

var ConfigFileName = "transform.yaml"

type rawConfig struct {
	Name         string            `yaml:"name"`
	Description  string            `yaml:"description,omitempty"`
	InputTopic   string            `yaml:"input-topic"`
	OutputTopic  string            `yaml:"output-topic"`
	OutputTopics []string          `yaml:"output-topics"`
	Language     string            `yaml:"language"`
	Env          map[string]string `yaml:"env,omitempty"`
	Compression  string            `yaml:"compression,omitempty"`
}

func MarshalConfig(c Config) ([]byte, error) {
	return yaml.Marshal(c)
}

func UnmarshalConfig(b []byte, c *Config) error {
	raw := rawConfig{}
	if err := yaml.Unmarshal(b, &raw); err != nil {
		return err
	}
	*c = Config{
		Name:         raw.Name,
		InputTopic:   raw.InputTopic,
		OutputTopics: raw.OutputTopics,
		Language:     "",
		Env:          raw.Env,
		Compression:  raw.Compression,
	}
	if len(c.OutputTopics) == 0 && len(raw.OutputTopic) > 0 {
		c.OutputTopics = []string{raw.OutputTopic}
	}
	return c.Language.Set(raw.Language)
}

func LoadCfg(fs afero.Fs) (c Config, err error) {
	b, err := afero.ReadFile(fs, ConfigFileName)
	if err != nil {
		return c, err
	}
	err = UnmarshalConfig(b, &c)
	return c, err
}
