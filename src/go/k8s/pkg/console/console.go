package console

import (
	"github.com/redpanda-data/console/backend/pkg/kafka"
	redpandav1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
)

// ConsoleConfig is the config passed to the Redpanda Console app
type ConsoleConfig struct {
	// Grabbed from https://github.com/redpanda-data/console/
	MetricsNamespace string `json:"metricsNamespace" yaml:"metricsNamespace"`
	ServeFrontend    bool   `json:"serveFrontend" yaml:"serveFrontend"`

	Server redpandav1alpha1.Server `json:"server" yaml:"server"`
	Kafka  kafka.Config            `json:"kafka" yaml:"kafka"`
}

// SetDefaults sets sane defaults
func (cc *ConsoleConfig) SetDefaults() {
	cc.ServeFrontend = true
	cc.MetricsNamespace = "console"

	cc.Kafka.SetDefaults()
}
