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
	"github.com/cloudhut/common/rest"
	"github.com/redpanda-data/console/backend/pkg/connect"
	"github.com/redpanda-data/console/backend/pkg/kafka"
)

const (
	debugLogLevel = 4
)

// ConsoleConfig is the config passed to the Redpanda Console app
type ConsoleConfig struct { // nolint:revive // more readable
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
	Enabled   bool                   `json:"enabled" yaml:"enabled"`
	JWTSecret string                 `json:"jwtSecret,omitempty" yaml:"jwtSecret,omitempty"`
	Google    *EnterpriseLoginGoogle `json:"google,omitempty" yaml:"google,omitempty"`
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
