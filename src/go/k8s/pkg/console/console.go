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
}

// SetDefaults sets sane defaults
func (cc *ConsoleConfig) SetDefaults() {
	cc.Kafka.SetDefaults()
}
