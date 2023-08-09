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
	"time"

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

func defaultVirtualRpkYaml() (RpkYaml, error) {
	path, _ := DefaultRpkYamlPath() // if err is non-nil, we fail in Write
	y := RpkYaml{
		fileLocation: path,
		Version:      1,
		Profiles:     []RpkProfile{DefaultRpkProfile()},
		CloudAuths:   []RpkCloudAuth{DefaultRpkCloudAuth()},
	}
	y.CurrentProfile = y.Profiles[0].Name
	y.CurrentCloudAuth = y.CloudAuths[0].Name
	return y, nil
}

// DefaultRpkProfile returns the default profile to use / create if no prior
// profile exists.
func DefaultRpkProfile() RpkProfile {
	return RpkProfile{
		Name:        "default",
		Description: "Default rpk profile",
	}
}

// DefaultRpkCloudAuth returns the default auth to use / create if no prior
// auth exists.
func DefaultRpkCloudAuth() RpkCloudAuth {
	return RpkCloudAuth{
		Name:        "default",
		Description: "Default rpk cloud auth",
	}
}

func emptyVirtualRpkYaml() RpkYaml {
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

		// Version is used for forwards and backwards compatibility.
		// If Version is <= 1, the file is not a valid rpk.yaml file.
		// If we read a config with an older version, we can parse it.
		// If we read a config with a newer version, we exit with a
		// message saying "we don't know how to parse this, please
		// upgrade rpk".
		Version int `json:"version" yaml:"version"`

		Globals RpkGlobals `json:"globals,omitempty" yaml:"globals,omitempty"`

		CurrentProfile   string         `json:"current_profile" yaml:"current_profile"`
		CurrentCloudAuth string         `json:"current_cloud_auth" yaml:"current_cloud_auth"`
		Profiles         []RpkProfile   `json:"profiles,omitempty" yaml:"profiles,omitempty"`
		CloudAuths       []RpkCloudAuth `json:"cloud_auth,omitempty" yaml:"cloud_auth,omitempty"`
	}

	RpkGlobals struct {
		// Prompt is the prompt to use for all profiles, unless the
		// profile itself overrides it.
		Prompt string `json:"prompt" yaml:"prompt"`

		// NoDefaultCluster disables localhost:{9092,9644} as a default
		// profile when no other is selected.
		NoDefaultCluster bool `json:"no_default_cluster" yaml:"no_default_cluster"`

		// DialTimeout is how long we allow for initiating a connection
		// to brokers for the Admin API and Kafka API.
		DialTimeout Duration `json:"dial_timeout" yaml:"dial_timeout"`

		// RequestTimeoutOverhead, for Kafka API requests, how long do
		// we give the request on top of any request's timeout field.
		RequestTimeoutOverhead Duration `json:"request_timeout_overhead" yaml:"request_timeout_overhead"`

		// RetryTimeout allows us to retry requests. If see we need to
		// retry before the retry timeout has elapsed, we do -- even if
		// backing off after we know to retry pushes us past the
		// timeout.
		RetryTimeout Duration `json:"retry_timeout" yaml:"retry_timeout"`

		// FetchMaxWait is how long we give the broker to respond to
		// fetch requests.
		FetchMaxWait Duration `json:"fetch_max_wait" yaml:"fetch_max_wait"`

		// KafkaProtocolReqClientID is the client ID to use for the Kafka API.
		KafkaProtocolReqClientID string `json:"kafka_protocol_request_client_id" yaml:"kafka_protocol_request_client_id"`
	}

	RpkProfile struct {
		Name         string           `json:"name" yaml:"name"`
		Description  string           `json:"description,omitempty" yaml:"description,omitempty"`
		Prompt       string           `json:"prompt,omitempty" yaml:"prompt,omitempty"`
		FromCloud    bool             `json:"from_cloud,omitempty" yaml:"from_cloud,omitempty"`
		CloudCluster *RpkCloudCluster `json:"cloud_cluster,omitempty" yaml:"cloud_cluster,omitempty"`
		KafkaAPI     RpkKafkaAPI      `json:"kafka_api,omitempty" yaml:"kafka_api,omitempty"`
		AdminAPI     RpkAdminAPI      `json:"admin_api,omitempty" yaml:"admin_api,omitempty"`

		// We stash the config struct itself so that we can provide
		// the logger / dev overrides.
		c *Config
	}

	RpkCloudCluster struct {
		Namespace string `json:"namespace" yaml:"namespace"`
		Cluster   string `json:"cluster" yaml:"cluster"`
		Auth      string `json:"auth" yaml:"auth"`
	}

	RpkCloudAuth struct {
		Name         string `json:"name" yaml:"name"`
		Description  string `json:"description,omitempty" yaml:"description,omitempty"`
		AuthToken    string `json:"auth_token,omitempty" yaml:"auth_token,omitempty"`
		RefreshToken string `json:"refresh_token,omitempty" yaml:"refresh_token,omitempty"`
		ClientID     string `json:"client_id,omitempty" yaml:"client_id,omitempty"`
		ClientSecret string `json:"client_secret,omitempty" yaml:"client_secret,omitempty"`
	}

	Duration struct{ time.Duration }
)

// Profile returns the given profile, or nil if it does not exist.
func (y *RpkYaml) Profile(name string) *RpkProfile {
	for i, p := range y.Profiles {
		if p.Name == name {
			return &y.Profiles[i]
		}
	}
	return nil
}

// PushProfile pushes a profile to the front and returns the profile's name.
func (y *RpkYaml) PushProfile(p RpkProfile) string {
	y.Profiles = append([]RpkProfile{p}, y.Profiles...)
	return p.Name
}

// MoveProfileToFront moves the given profile to the front of the list.
func (y *RpkYaml) MoveProfileToFront(p *RpkProfile) {
	reordered := []RpkProfile{*p}
	for i := range y.Profiles {
		if &y.Profiles[i] == p {
			continue
		}
		reordered = append(reordered, y.Profiles[i])
	}
	y.Profiles = reordered
}

// Auth returns the given auth, or nil if it does not exist.
func (y *RpkYaml) Auth(name string) *RpkCloudAuth {
	for i, a := range y.CloudAuths {
		if a.Name == name {
			return &y.CloudAuths[i]
		}
	}
	return nil
}

// PushAuth pushes an auth to the front and returns the auth's name.
func (y *RpkYaml) PushAuth(a RpkCloudAuth) string {
	y.CloudAuths = append([]RpkCloudAuth{a}, y.CloudAuths...)
	return a.Name
}

// MoveAuthToFront moves the given auth to the front of the list.
func (y *RpkYaml) MoveAuthToFront(a *RpkCloudAuth) {
	reordered := []RpkCloudAuth{*a}
	for i := range y.CloudAuths {
		if &y.CloudAuths[i] == a {
			continue
		}
		reordered = append(reordered, y.CloudAuths[i])
	}
	y.CloudAuths = reordered
}

type CloudAuthKind string

const (
	CloudAuthUninitialized     CloudAuthKind = "uninitialized"
	CloudAuthSSO               CloudAuthKind = "sso"
	CloudAuthClientCredentials CloudAuthKind = "client-credentials"
)

// Kind returns either a known auth kind or "uninitialized".
func (a *RpkCloudAuth) Kind() (CloudAuthKind, bool) {
	switch {
	case a.ClientID != "" && a.ClientSecret != "":
		return CloudAuthClientCredentials, true
	case a.ClientID != "":
		return CloudAuthSSO, true
	default:
		return CloudAuthUninitialized, false
	}
}

///////////
// FUNCS //
///////////

// Logger returns the logger for the original configuration, or a nop logger if
// it was invalid.
func (p *RpkProfile) Logger() *zap.Logger {
	return p.c.p.Logger()
}

// SugarLogger returns Logger().Sugar().
func (p *RpkProfile) SugarLogger() *zap.SugaredLogger {
	return p.Logger().Sugar()
}

// Defaults returns the virtual defaults for the rpk.yaml.
func (p *RpkProfile) Defaults() *RpkGlobals {
	return &p.c.rpkYaml.Globals
}

// CurrentAuth returns the current cloud Auth.
func (p *RpkProfile) CurrentAuth() *RpkCloudAuth {
	return p.c.rpkYaml.Auth(p.c.rpkYaml.CurrentCloudAuth)
}

// DevOverrides returns any configured dev overrides.
func (p *RpkProfile) DevOverrides() DevOverrides {
	return p.c.devOverrides
}

// HasSASLCredentials returns if both Kafka SASL user and password are empty.
func (p *RpkProfile) HasSASLCredentials() bool {
	s := p.KafkaAPI.SASL
	return s != nil && s.User != "" && s.Password != ""
}

// HasClientCredentials returns if both ClientID and ClientSecret are empty.
func (a *RpkCloudAuth) HasClientCredentials() bool {
	k, _ := a.Kind()
	return k == CloudAuthClientCredentials
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

// FileLocation returns the path to this rpk.yaml, whether it exists or not.
func (y *RpkYaml) FileLocation() string {
	return y.fileLocation
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

////////////////
// MISC TYPES //
////////////////

// MarshalText implements encoding.TextMarshaler.
func (d Duration) MarshalText() ([]byte, error) {
	return []byte(d.Duration.String()), nil
}

// UnmarshalText implements encoding.TextUnmarshaler.
func (d *Duration) UnmarshalText(text []byte) error {
	var err error
	d.Duration, err = time.ParseDuration(string(text))
	return err
}

func (*Duration) YamlTypeNameForTest() string { return "duration" }
