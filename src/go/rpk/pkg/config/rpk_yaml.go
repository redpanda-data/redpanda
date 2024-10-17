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

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/publicapi"
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
		Version:      6,
		Profiles:     []RpkProfile{DefaultRpkProfile()},
		CloudAuths:   []RpkCloudAuth{DefaultRpkCloudAuth()},
	}
	y.CurrentProfile = y.Profiles[0].Name
	y.CurrentCloudAuthOrgID = y.CloudAuths[0].OrgID
	y.CurrentCloudAuthKind = y.CloudAuths[0].Kind
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
		Name:         "default",
		Organization: "Default organization",
		OrgID:        "default-org-no-id",
	}
}

func emptyVirtualRpkYaml() RpkYaml {
	return RpkYaml{
		Version: 6,
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

		Globals RpkGlobals `json:"globals" yaml:"globals"`

		CurrentProfile        string         `json:"current_profile" yaml:"current_profile"`
		CurrentCloudAuthOrgID string         `json:"current_cloud_auth_org_id" yaml:"current_cloud_auth_org_id"`
		CurrentCloudAuthKind  string         `json:"current_cloud_auth_kind" yaml:"current_cloud_auth_kind"`
		Profiles              []RpkProfile   `json:"profiles" yaml:"profiles"`
		CloudAuths            []RpkCloudAuth `json:"cloud_auth" yaml:"cloud_auth"`
	}

	RpkGlobals struct {
		// Prompt is the prompt to use for all profiles, unless the
		// profile itself overrides it.
		Prompt string `json:"prompt" yaml:"prompt"`

		// NoDefaultCluster disables localhost:{9092,9644} as a default
		// profile when no other is selected.
		NoDefaultCluster bool `json:"no_default_cluster" yaml:"no_default_cluster"`

		// CommandTimeout is how long to allow commands to run, for
		// certain potentially-slow commands.
		CommandTimeout Duration `json:"command_timeout" yaml:"command_timeout"`

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
		Name         string               `json:"name" yaml:"name"`
		Description  string               `json:"description" yaml:"description"`
		Prompt       string               `json:"prompt" yaml:"prompt"`
		FromCloud    bool                 `json:"from_cloud" yaml:"from_cloud"`
		CloudCluster RpkCloudCluster      `json:"cloud_cluster,omitempty" yaml:"cloud_cluster,omitempty"`
		KafkaAPI     RpkKafkaAPI          `json:"kafka_api" yaml:"kafka_api"`
		AdminAPI     RpkAdminAPI          `json:"admin_api" yaml:"admin_api"`
		SR           RpkSchemaRegistryAPI `json:"schema_registry" yaml:"schema_registry"`
		LicenseCheck *LicenseStatusCache  `json:"license_check,omitempty" yaml:"license_check,omitempty"`

		// We stash the config struct itself so that we can provide
		// the logger / dev overrides.
		c *Config
	}

	RpkCloudCluster struct {
		Namespace     string `json:"namespace" yaml:"namespace"`
		ResourceGroup string `json:"resource_group" yaml:"resource_group"`
		ClusterID     string `json:"cluster_id" yaml:"cluster_id"`
		ClusterName   string `json:"cluster_name" yaml:"cluster_name"`
		AuthOrgID     string `json:"auth_org_id" yaml:"auth_org_id"`
		AuthKind      string `json:"auth_kind" yaml:"auth_kind"`
		ClusterType   string `json:"cluster_type" yaml:"cluster_type"`
		ClusterURL    string `json:"cluster_url,omitempty" yaml:"cluster_url,omitempty"`
	}

	// RpkCloudAuth is unique by name and org ID. We support multiple auths
	// per org ID in case a person wants to use client credentials and SSO.
	RpkCloudAuth struct {
		Name         string `json:"name" yaml:"name"`
		Organization string `json:"organization" yaml:"organization"`
		OrgID        string `json:"org_id" yaml:"org_id"`
		Kind         string `json:"kind" yaml:"kind"`
		AuthToken    string `json:"auth_token,omitempty" yaml:"auth_token,omitempty"`
		RefreshToken string `json:"refresh_token,omitempty" yaml:"refresh_token,omitempty"`
		ClientID     string `json:"client_id,omitempty" yaml:"client_id,omitempty"`
		ClientSecret string `json:"client_secret,omitempty" yaml:"client_secret,omitempty"`
	}

	Duration struct{ time.Duration }

	LicenseStatusCache struct {
		LastUpdate int64 `json:"last_update" yaml:"last_update"`
	}
)

// Profile returns the given profile, or nil if it does not exist.
// This is safe to call even if y is nil.
func (y *RpkYaml) Profile(name string) *RpkProfile {
	if y == nil {
		return nil
	}
	for i, p := range y.Profiles {
		if p.Name == name {
			return &y.Profiles[i]
		}
	}
	return nil
}

// PushProfile pushes a profile to the front, updates the current profile, and
// returns the prior profile's auth and the current profile's auth.
func (y *RpkYaml) PushProfile(p RpkProfile) (priorAuth, currentAuth *RpkCloudAuth) {
	priorAuth = y.CurrentAuth()
	y.Profiles = append([]RpkProfile{p}, y.Profiles...)
	if p.FromCloud {
		y.CurrentCloudAuthOrgID = p.CloudCluster.AuthOrgID
		y.CurrentCloudAuthKind = p.CloudCluster.AuthKind
	}
	currentAuth = y.CurrentAuth()
	y.CurrentProfile = p.Name
	return priorAuth, currentAuth
}

// MoveProfileToFront moves the given profile to the front of the list.
func (y *RpkYaml) MoveProfileToFront(p **RpkProfile) (priorAuth, currentAuth *RpkCloudAuth) {
	priorAuth = y.CurrentAuth()
	reordered := []RpkProfile{**p}
	for i := range y.Profiles {
		if &y.Profiles[i] == *p {
			continue
		}
		reordered = append(reordered, y.Profiles[i])
	}
	y.Profiles = reordered
	*p = &reordered[0]
	y.CurrentProfile = (*p).Name

	// If this is a cloud profile, we switch the auth as well.
	if (*p).FromCloud {
		y.CurrentCloudAuthOrgID = (*p).CloudCluster.AuthOrgID
		y.CurrentCloudAuthKind = (*p).CloudCluster.AuthKind
	}
	currentAuth = y.CurrentAuth()
	return priorAuth, currentAuth
}

// LookupAuth returns an RpkCloudAuth based on the org and kind.
func (y *RpkYaml) LookupAuth(org, kind string) *RpkCloudAuth {
	for i, a := range y.CloudAuths {
		if a.OrgID == org && a.Kind == kind {
			return &y.CloudAuths[i]
		}
	}
	return nil
}

// PushNewAuth pushes an auth to the front and sets it as the current auth.
func (y *RpkYaml) PushNewAuth(a RpkCloudAuth) {
	y.CloudAuths = append([]RpkCloudAuth{a}, y.CloudAuths...)
	y.CurrentCloudAuthOrgID = a.OrgID
	y.CurrentCloudAuthKind = a.Kind
}

// MakeAuthCurrent finds the given auth, moves it to the front, and updates
// the current cloud auth fields. This pointer must exist, if it does not,
// this function panics.
// This updates *a to point to the new address of the auth.
func (y *RpkYaml) MakeAuthCurrent(a **RpkCloudAuth) {
	reordered := []RpkCloudAuth{**a}
	var found bool
	for i := range y.CloudAuths {
		if &y.CloudAuths[i] == *a {
			found = true
			continue
		}
		reordered = append(reordered, y.CloudAuths[i])
	}
	if !found {
		panic("MakeAuthCurrent called with an auth that does not exist")
	}
	*a = &reordered[0]
	y.CloudAuths = reordered
	y.CurrentCloudAuthOrgID = (*a).OrgID
	y.CurrentCloudAuthKind = (*a).Kind
}

// DropAuth removes the given auth from the list of auths. If this was the
// current auth, this clears the current auth.
func (y *RpkYaml) DropAuth(a *RpkCloudAuth) {
	dropped := y.CloudAuths[:0]
	for i := range y.CloudAuths {
		if &y.CloudAuths[i] == a {
			continue
		}
		dropped = append(dropped, y.CloudAuths[i])
	}
	y.CloudAuths = dropped
	if y.CurrentCloudAuthOrgID == a.OrgID && y.CurrentCloudAuthKind == a.Kind {
		y.CurrentCloudAuthOrgID = ""
		y.CurrentCloudAuthKind = ""
	}
}

// CurrentAuth returns the auth corresponding to the current cloud auth, if
// it exists.
func (y *RpkYaml) CurrentAuth() *RpkCloudAuth {
	return y.LookupAuth(y.CurrentCloudAuthOrgID, y.CurrentCloudAuthKind)
}

const (
	CloudAuthUninitialized     = ""
	CloudAuthSSO               = "sso"
	CloudAuthClientCredentials = "client-credentials"
)

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
	return p.c.rpkYaml.LookupAuth(p.c.rpkYaml.CurrentCloudAuthOrgID, p.c.rpkYaml.CurrentCloudAuthKind)
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

// ActualAuth returns the actual cloud auth for this profile, if it exists.
func (p *RpkProfile) ActualAuth() *RpkCloudAuth {
	if p == nil {
		return nil
	}
	return p.c.rpkYamlActual.LookupAuth(p.CloudCluster.AuthOrgID, p.CloudCluster.AuthKind)
}

// VirtualAuth returns the virtual cloud auth for this profile if
// this profile is for a cloud cluster (following the newer scheme
// of the CloudCluster field).
func (p *RpkProfile) VirtualAuth() *RpkCloudAuth {
	if p == nil {
		return nil
	}
	return p.c.rpkYaml.LookupAuth(p.CloudCluster.AuthOrgID, p.CloudCluster.AuthKind)
}

func (p *RpkProfile) ActualConfig() (*RpkYaml, bool) {
	if p.c == nil {
		return nil, false
	}
	return p.c.ActualRpkYaml()
}

// HasClientCredentials returns if both ClientID and ClientSecret are non-empty.
func (a *RpkCloudAuth) HasClientCredentials() bool {
	return a.ClientID != "" && a.ClientSecret != ""
}

// Equals returns if the two cloud auths are the same, which is true
// if the name matches (the name embeds the org name, ID, and auth kind).
func (a *RpkCloudAuth) Equals(other *RpkCloudAuth) bool {
	if a == nil && other == nil {
		return true
	}
	if a == nil || other == nil {
		return false
	}
	return a.Name == other.Name
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

// FullName returns "resource_group/cluster_name".
func (c *RpkCloudCluster) FullName() string {
	return fmt.Sprintf("%s/%s", c.ResourceGroup, c.ClusterName)
}

// HasAuth returns if the cluster has the given auth.
// This is ok to call even if c is nil.
func (c *RpkCloudCluster) HasAuth(a RpkCloudAuth) bool {
	if c == nil {
		return false
	}
	return c.AuthOrgID == a.OrgID && c.AuthKind == a.Kind
}

func (c *RpkCloudCluster) IsServerless() bool {
	return c != nil && c.ClusterType == publicapi.ServerlessClusterType
}

func (c *RpkCloudCluster) CheckClusterURL() (string, error) {
	if c == nil {
		return "", fmt.Errorf("cluster information not present in current profile; please delete and re-create the current profile with 'rpk profile create --from-cloud'")
	}
	if c.ClusterURL == "" {
		if c.ClusterID != "" {
			return "", fmt.Errorf("cluster URL not present in profile; please delete and re-create the current profile with 'rpk profile create <name> --from-cloud=%v'", c.ClusterID)
		}
		return "", errors.New("cluster URL not present in profile; please delete and re-create the current profile with 'rpk profile create --from-cloud' and selecting this cluster")
	}
	return c.ClusterURL, nil
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

/////////////////////
// GLOBALS GETTERS //
/////////////////////

// GetCommandTimeout returns the command timeout, or 10s if none is set.
func (g *RpkGlobals) GetCommandTimeout() time.Duration {
	if g.CommandTimeout.Duration == 0 {
		return 10 * time.Second
	}
	return g.CommandTimeout.Duration
}

//////////
// MISC //
//////////

// MaybePrintAuthSwitchMessage prints a message if the prior and current
// auths are different.
func MaybePrintAuthSwitchMessage(priorAuth *RpkCloudAuth, currentAuth *RpkCloudAuth) {
	if priorAuth == nil {
		if currentAuth == nil {
			return
		}
		fmt.Printf("rpk cloud commands are now talking to organization %q (%s).\n", currentAuth.Organization, currentAuth.OrgID)
		return
	}
	if currentAuth == nil {
		fmt.Printf("rpk cloud commands are no longer talking to organization %q (%s) and are now talking to a self hosted cluster.\n", priorAuth.Organization, priorAuth.OrgID)
		return
	}
	if priorAuth.Name != currentAuth.Name {
		fmt.Printf("rpk switched from talking to organization %q (%s) to %q (%s).\n", priorAuth.Organization, priorAuth.OrgID, currentAuth.Organization, currentAuth.OrgID)
	}
}
