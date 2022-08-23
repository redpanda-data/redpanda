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
type ConsoleConfig struct {
	// Grabbed from https://github.com/redpanda-data/console/
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
