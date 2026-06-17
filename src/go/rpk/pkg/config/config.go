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
	"fmt"
	"strings"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
)

const (
	DefaultKafkaPort     = 9092
	DefaultSchemaRegPort = 8081
	DefaultProxyPort     = 8082
	DefaultAdminPort     = 9644
	DefaultRPCPort       = 33145
	DefaultConsolePort   = 8080
	DefaultListenAddress = "0.0.0.0"
	LoopbackIP           = "127.0.0.1"

	DefaultBallastFilePath = "/var/lib/redpanda/data/ballast"
	DefaultBallastFileSize = "1GiB"
)

// DevOverrides contains available overrides that are used for developer
// testing. This list can be used wherever in rpk. These are not persisted to
// any configuration file and they are not available as flags.
type DevOverrides struct {
	// CloudAuthURL is used by `rpk cloud` to override the auth0 URL
	// we talk to.
	CloudAuthURL string `env:"RPK_CLOUD_AUTH_URL"`
	// CloudAuthAudience is used by `rpk cloud` to override the auth0
	// audience we use.
	CloudAuthAudience string `env:"RPK_CLOUD_AUTH_AUDIENCE"`
	// CloudAuthAppClientID is used by `rpk cloud` to override the client
	// ID we use when talking to auth0.
	CloudAuthAppClientID string `env:"RPK_AUTH_APP_CLIENT_ID"`
	// CloudAPIURL is used by `rpk cloud` to override the Redpanda Cloud
	// URL we talk to.
	CloudAPIURL string `env:"RPK_CLOUD_URL"`
	// BYOCSkipVersionCheck is used by `rpk cloud byoc` and skips any byoc
	// plugin version checking, instead using whatever is available.
	BYOCSkipVersionCheck string `env:"RPK_CLOUD_SKIP_VERSION_CHECK"`
	// AllowRpkCloudAdmin bypasses out.CheckExitCloudAdmin, allowing rpk to
	// continue to use an admin command even if the command is technically
	// not supported because the cluster is a cloud cluster.
	AllowRpkCloudAdmin bool `env:"ALLOW_RPK_CLOUD_ADMIN"`
	// CloudToken bypasses the oauth.LoadFlow, allowing you to pass a cloud
	// token instead of logging in.
	CloudToken string `env:"RPK_CLOUD_TOKEN"`
	// PublicAPIURL is used by `rpk cloud` to override the public API URL.
	PublicAPIURL string `env:"RPK_PUBLIC_API_URL"`
}

// Config encapsulates a redpanda.yaml and/or an rpk.yaml. This is the
// entrypoint that params.Config returns, after which you can get either the
// Virtual or actual configurations.
type Config struct {
	p *Params

	redpandaYaml       RedpandaYaml // processed, defaults/env/flags
	redpandaYamlActual RedpandaYaml // unprocessed
	redpandaYamlExists bool         // whether the redpanda.yaml file exists
	redpandaYamlInitd  bool         // if OrDefaults was returned to initialize a new "actual" file that has not yet been written

	rpkYaml       RpkYaml // processed, defaults/env/flags
	rpkYamlActual RpkYaml // unprocessed
	rpkYamlExists bool    // whether the rpk.yaml file exists
	rpkYamlInitd  bool    // if OrEmpty was returned to initialize a new "actual" file that has not yet been written

	devOverrides DevOverrides
}

// CheckExitCloudAdmin exits if the profile has FromCloud=true and no
// ALLOW_RPK_CLOUD_ADMIN override.
func CheckExitCloudAdmin(p *RpkProfile) {
	if p.FromCloud && !p.DevOverrides().AllowRpkCloudAdmin {
		out.Die("this admin API based command is not supported on Redpanda Cloud clusters")
	}
}

// CheckExitServerlessAdmin exits if the profile has FromCloud=true and the
// cluster is a Serverless cluster.
func CheckExitServerlessAdmin(p *RpkProfile) {
	if p.FromCloud && p.CloudCluster.IsServerless() {
		out.Die("this admin API based command is not supported on Redpanda Cloud serverless clusters")
	}
}

// CheckExitNotServerlessAdmin exits if the profile has FromCloud=true and the
// cluster is NOT a Serverless cluster.
func CheckExitNotServerlessAdmin(p *RpkProfile) {
	if p.FromCloud && !p.CloudCluster.IsServerless() {
		out.Die("this admin API based command is not supported on Redpanda Cloud clusters")
	}
}

// VirtualRedpandaYaml returns a redpanda.yaml, starting with defaults,
// then decoding a potential file, then applying env vars and then flags.
func (c *Config) VirtualRedpandaYaml() *RedpandaYaml {
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
	if c.redpandaYamlExists || c.redpandaYamlInitd {
		return &c.redpandaYamlActual
	}
	defer func() { c.redpandaYamlInitd = true }()
	redpandaYaml := DevDefault()
	if c.p.ConfigFlag != "" { // --config set but the file does not yet exist
		redpandaYaml.fileLocation = c.p.ConfigFlag
	}
	c.redpandaYamlActual = *redpandaYaml
	return &c.redpandaYamlActual
}

// VirtualRpkYaml returns an rpk.yaml, starting with defaults, then
// decoding a potential file, then applying env vars and then flags.
func (c *Config) VirtualRpkYaml() *RpkYaml {
	return &c.rpkYaml
}

// VirtualProfile returns an rpk.yaml's current Virtual profile,
// starting with defaults, then decoding a potential file, then applying env
// vars and then flags. This always returns non-nil due to a guarantee from
// Params.Load.
func (c *Config) VirtualProfile() *RpkProfile {
	return c.rpkYaml.Profile(c.rpkYaml.CurrentProfile)
}

// ActualProfile returns an actual rpk.yaml's current profile.
// This may return nil if there is no current profile.
func (c *Config) ActualProfile() *RpkProfile {
	return c.rpkYamlActual.Profile(c.rpkYamlActual.CurrentProfile)
}

// ActualRpkYaml returns an actual rpk.yaml if it exists, with no other
// defaults over overrides applied.
func (c *Config) ActualRpkYaml() (*RpkYaml, bool) {
	return &c.rpkYamlActual, c.rpkYamlExists
}

// ActualRpkYamlOrEmpty returns an actual rpk.yaml if it exists, otherwise this
// returns a blank rpk.yaml. If this function tries to return a default rpk.yaml
// but cannot read the user config dir, this returns an error.
func (c *Config) ActualRpkYamlOrEmpty() (y *RpkYaml, err error) {
	if c.rpkYamlExists || c.rpkYamlInitd {
		return &c.rpkYamlActual, nil
	}
	defer func() { c.rpkYamlInitd = true }()
	rpkYaml := emptyVirtualRpkYaml()
	if c.p.ConfigFlag != "" {
		rpkYaml.fileLocation = c.p.ConfigFlag
	} else {
		path, err := DefaultRpkYamlPath()
		if err != nil {
			return nil, err
		}
		rpkYaml.fileLocation = path
	}
	c.rpkYamlActual = rpkYaml
	return &c.rpkYamlActual, nil
}

// DevOverrides returns any currently set dev overrides.
func (c *Config) DevOverrides() DevOverrides {
	return c.devOverrides
}

// LoadVirtualRedpandaYaml is a shortcut for p.Load followed by
// cfg.VirtualRedpandaYaml.
func (p *Params) LoadVirtualRedpandaYaml(fs afero.Fs) (*RedpandaYaml, error) {
	cfg, err := p.Load(fs)
	if err != nil {
		return nil, err
	}
	return cfg.VirtualRedpandaYaml(), nil
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

// LoadVirtualProfile is a shortcut for p.Load followed by
// cfg.VirtualProfile.
func (p *Params) LoadVirtualProfile(fs afero.Fs) (*RpkProfile, error) {
	cfg, err := p.Load(fs)
	if err != nil {
		return nil, err
	}
	return cfg.VirtualProfile(), nil
}

///////////
// MODES //
///////////

const (
	ModeDev      = "dev"
	ModeProd     = "prod"
	ModeRecovery = "recovery"
)

// Mode returns the configuration mode that is currently active in the given
// redpanda.yaml node configuration. Recovery takes precedence because it can be
// enabled on top of dev or prod; otherwise the mode is dev or prod depending on
// whether developer_mode is set.
func (y *RedpandaYaml) Mode() string {
	switch {
	case y.Redpanda.RecoveryModeEnabled:
		return ModeRecovery
	case y.Redpanda.DeveloperMode:
		return ModeDev
	default:
		return ModeProd
	}
}

func (c *Config) SetMode(fs afero.Fs, mode string) error {
	yRedpanda := c.ActualRedpandaYamlOrDefaults()
	switch {
	case mode == "" || strings.HasPrefix("development", mode):
		yRedpanda.setDevMode()
	case strings.HasPrefix("production", mode):
		yRedpanda.setProdMode()
	case strings.HasPrefix("recovery", mode):
		yRedpanda.setRecoveryMode()
	default:
		return fmt.Errorf("unknown mode %q", mode)
	}
	return yRedpanda.Write(fs)
}

func (y *RedpandaYaml) setDevMode() {
	y.Redpanda.DeveloperMode = true
	y.Redpanda.RecoveryModeEnabled = false
	// Defaults to setting all tuners to false
	y.Rpk = RpkNodeConfig{
		KafkaAPI:             y.Rpk.KafkaAPI,
		AdminAPI:             y.Rpk.AdminAPI,
		SR:                   y.Rpk.SR,
		AdditionalStartFlags: y.Rpk.AdditionalStartFlags,
		SMP:                  DevDefault().Rpk.SMP,
		Overprovisioned:      true,
		Tuners: RpkNodeTuners{
			CoredumpDir:     y.Rpk.Tuners.CoredumpDir,
			BallastFilePath: y.Rpk.Tuners.BallastFilePath,
			BallastFileSize: y.Rpk.Tuners.BallastFileSize,
		},
	}
}

func (y *RedpandaYaml) setProdMode() {
	y.Redpanda.DeveloperMode = false
	y.Redpanda.RecoveryModeEnabled = false
	y.Rpk.Overprovisioned = false
	y.Rpk.Tuners.TuneNetwork = true
	y.Rpk.Tuners.TuneDiskScheduler = true
	y.Rpk.Tuners.TuneNomerges = true
	y.Rpk.Tuners.TuneDiskIrq = true
	y.Rpk.Tuners.TuneFstrim = false
	y.Rpk.Tuners.TuneCPU = true
	y.Rpk.Tuners.TuneAioEvents = true
	y.Rpk.Tuners.TuneClocksource = true
	y.Rpk.Tuners.TuneSwappiness = true
	y.Rpk.Tuners.TuneDiskWriteCache = true
	y.Rpk.Tuners.TuneBallastFile = true
}

func (y *RedpandaYaml) setRecoveryMode() {
	y.Redpanda.RecoveryModeEnabled = true
}

func ParseAdditionalStartFlags(flags []string) map[string]string {
	parsed := map[string]string{}
	for i := 0; i < len(flags); i++ {
		f := flags[i]
		isFlag := strings.HasPrefix(f, "-")
		trimmed := strings.Trim(f, " -")

		// Filter out elements that aren't flags or are empty.
		if !isFlag || trimmed == "" {
			continue
		}

		// Check if it's in name=value format
		// Split only into 2 tokens, since some flags can have multiple '='
		// in them, like --logger-log-level=archival=debug:cloud_storage=debug
		parts := strings.SplitN(trimmed, "=", 2)
		if len(parts) >= 2 {
			name := strings.Trim(parts[0], " ")
			value := strings.Trim(parts[1], ` "`)
			parsed[name] = value
			continue
		}
		// Otherwise, it can be a boolean flag (i.e. -v) or in
		// name<space>value format

		if i == len(flags)-1 {
			// We've reached the last element, so it's a single flag
			parsed[trimmed] = ""
			continue
		}

		// Check if the next element starts with a hyphen
		// If it does, it's another flag, and the current element is a
		// boolean flag
		next := flags[i+1]
		if strings.HasPrefix(next, "-") {
			parsed[trimmed] = ""
			continue
		}

		// Otherwise, the current element is the name of the flag and
		// the next one is its value
		parsed[trimmed] = next
		i += 1
	}
	return parsed
}
