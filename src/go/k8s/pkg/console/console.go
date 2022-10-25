// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// Package console defines Console reconcile methods
package console

import (
	"time"

	"github.com/cloudhut/common/rest"
	"github.com/redpanda-data/console/backend/pkg/connect"
	"github.com/redpanda-data/console/backend/pkg/kafka"
	redpandav1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
)

const (
	debugLogLevel = 4
)

// ConsoleConfig is the config passed to the Redpanda Console app
type ConsoleConfig struct {
	// Grabbed from https://github.com/redpanda-data/console/
	// Copying the config types because they don't have Enterprise fields and not all fields are supported yet
	MetricsNamespace string `json:"metricsNamespace" yaml:"metricsNamespace"`
	ServeFrontend    bool   `json:"serveFrontend" yaml:"serveFrontend"`

	Server  rest.Config    `json:"server" yaml:"server"`
	Kafka   kafka.Config   `json:"kafka" yaml:"kafka"`
	Connect connect.Config `json:"connect" yaml:"connect"`

	License    string          `json:"license,omitempty" yaml:"license,omitempty"`
	Enterprise Enterprise      `json:"enterprise,omitempty" yaml:"enterprise,omitempty"`
	Login      EnterpriseLogin `json:"login,omitempty" yaml:"login,omitempty"`
	Cloud      CloudConfig     `json:"cloud,omitempty" yaml:"cloud,omitempty"`
}

// SetDefaults sets sane defaults
func (cc *ConsoleConfig) SetDefaults() {
	cc.Kafka.SetDefaults()
}

// Enterprise is the Console Enterprise config
type Enterprise struct {
	RBAC EnterpriseRBAC `json:"rbac" yaml:"rbac"`
}

// EnterpriseRBAC is the Console Enterprise RBAC config
type EnterpriseRBAC struct {
	Enabled              bool   `json:"enabled" yaml:"enabled"`
	RoleBindingsFilepath string `json:"roleBindingsFilepath" yaml:"roleBindingsFilepath"`
}

// EnterpriseLogin is the Console Enterprise Login config
type EnterpriseLogin struct {
	Enabled       bool                                           `json:"enabled" yaml:"enabled"`
	JWTSecret     string                                         `json:"jwtSecret,omitempty" yaml:"jwtSecret,omitempty"`
	Google        *EnterpriseLoginGoogle                         `json:"google,omitempty" yaml:"google,omitempty"`
	RedpandaCloud *redpandav1alpha1.EnterpriseLoginRedpandaCloud `json:"redpandaCloud,omitempty" yaml:"redpandaCloud,omitempty"`
}

// EnterpriseLoginGoogle is the Console Enterprise Google SSO config
type EnterpriseLoginGoogle struct {
	Enabled      bool                            `json:"enabled" yaml:"enabled"`
	ClientID     string                          `json:"clientId" yaml:"clientId"`
	ClientSecret string                          `json:"clientSecret" yaml:"clientSecret"`
	Directory    *EnterpriseLoginGoogleDirectory `json:"directory,omitempty" yaml:"directory,omitempty"`
}

// EnterpriseLoginGoogleDirectory is the Console Enterprise RBAC Google groups sync config
type EnterpriseLoginGoogleDirectory struct {
	ServiceAccountFilepath string `json:"serviceAccountFilepath" yaml:"serviceAccountFilepath"`
	TargetPrincipal        string `json:"targetPrincipal" yaml:"targetPrincipal"`
}

type CloudConfig struct {
	// PrometheusEndpoint configures the Prometheus endpoint that shall be
	// exposed in Redpanda Cloud so that users can scrape this URL to
	// collect their dataplane's metrics in their own time-series database.
	PrometheusEndpoint PrometheusEndpointConfig `yaml:"prometheusEndpoint"`
}

type PrometheusEndpointConfig struct {
	Enabled bool `yaml:"enabled"`

	// password is set via env var by mounting a secret
	BasicAuth struct {
		Username string `yaml:"username"`
	} `yaml:"basicAuth"`
	ResponseCacheDuration time.Duration    `yaml:"responseCacheDuration"`
	Prometheus            PrometheusConfig `yaml:"prometheus"`
}

type PrometheusConfig struct {
	// Address to Prometheus endpoint (e.g. "https://prometheus-blocks-prod-us-central1.grafana.net/api/prom)
	Address string `yaml:"address"`

	// BasicAuth that shall be used when talking to the Prometheus target.
	BasicAuth PrometheusClientBasicAuthConfig `yaml:"basicAuth"`

	// Jobs is the list of Prometheus Jobs that we want to discover so that we
	// can then scrape the discovered targets ourselves.
	Jobs []PrometheusScraperJobConfig `yaml:"jobs"`

	TargetRefreshInterval time.Duration `yaml:"targetRefreshInterval"`
}

// PrometheusScraperJobConfig is the configuration object that determines what Prometheus
// targets we should scrape.
type PrometheusScraperJobConfig struct {
	// JobName refers to the Prometheus job name whose discovered targets we want to scrape
	JobName string `yaml:"jobName"`
	// KeepLabels is a list of label keys that are added by Prometheus when scraping
	// the target and should remain for all metrics as exposed to the Prometheus endpoint.
	KeepLabels []string `yaml:"keepLabels"`
}

type PrometheusClientBasicAuthConfig struct {
	Enabled  bool   `yaml:"enabled"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}
