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
	"strings"

	"github.com/spf13/afero"
	"gopkg.in/yaml.v3"
)

const (
	FlagClientID     = "client-id"
	FlagClientSecret = "client-secret"

	envClientID     = "RPK_CLOUD_CLIENT_ID"     // The user's client ID.
	envClientSecret = "RPK_CLOUD_CLIENT_SECRET" // The user's client secret.

	envAuthURL          = "RPK_CLOUD_AUTH_URL"           // The authentication server URL.
	envAuthAudience     = "RPK_CLOUD_AUTH_AUDIENCE"      // The Auth0 audience.
	envCloudURL         = "RPK_CLOUD_URL"                // The Cloud API URL.
	envAuthClientID     = "RPK_AUTH_APP_CLIENT_ID"       // The ClientID of rpk to authenticate against the auth server.
	envSkipVersionCheck = "RPK_CLOUD_SKIP_VERSION_CHECK" // If true, rpk won't validate the plugin version against the Cloud API.

)

// Params contains values that can be set by flags.
type Params struct {
	ClientID     string
	ClientSecret string
}

// Load loads a potentially existing config file and applies env and flag
// overrides.
func (p *Params) Load(fs afero.Fs) (*Config, error) {
	path, err := defaultCfgPath()
	if err != nil {
		return nil, err
	}

	cfg := Config{path: path}
	file, err := afero.ReadFile(fs, path)
	switch {
	case err == nil:
		if err := yaml.Unmarshal(file, &cfg); err != nil {
			return nil, fmt.Errorf("unable to decode config file at path %s: %w", path, err)
		}
		cfg.file = cfg.fileCfg()
		cfg.exists = true
	case os.IsNotExist(err):
	default:
		return nil, fmt.Errorf("unable to load config file at path %s: %w", path, err)
	}

	if err := checkEnvTogether(envClientID, envClientSecret); err != nil {
		return nil, err
	}
	if err := checkEnvTogether(envAuthURL, envAuthAudience); err != nil {
		return nil, err
	}

	for _, override := range []struct {
		env  string
		flag string
		dst  *string
	}{
		{envClientID, p.ClientID, &cfg.ClientID},
		{envClientSecret, p.ClientSecret, &cfg.ClientSecret},
		{envAuthURL, "", &cfg.AuthURL},
		{envAuthAudience, "", &cfg.AuthAudience},
		{envCloudURL, "", &cfg.CloudURL},
		{envAuthClientID, "", &cfg.AuthClientID},
		{envSkipVersionCheck, "", &cfg.SkipVersionCheck},
	} {
		if v, ok := os.LookupEnv(override.env); ok && len(v) > 0 {
			*override.dst = v
		}
		if override.flag != "" {
			*override.dst = override.flag
		}
	}
	return &cfg, nil
}

// checkEnvTogether checks if all the passed environment variables are set
// together.
func checkEnvTogether(envs ...string) error {
	var set, notSet []string
	for _, env := range envs {
		if _, ok := os.LookupEnv(env); ok {
			set = append(set, env)
		} else {
			notSet = append(notSet, env)
		}
	}
	if len(set) > 0 && len(notSet) > 0 {
		return fmt.Errorf("if any environment variables [%s] are set, all must be set; missing: [%s]", strings.Join(envs, ", "), strings.Join(notSet, ","))
	}
	return nil
}
