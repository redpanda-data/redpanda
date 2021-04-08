package kafka_test

import (
	"testing"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/config"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/kafka"
)

func Test_LoadConfig(t *testing.T) {

	getCfg := func() *config.Config {
		cfg := config.Default()
		cfg.Rpk.SCRAM.User = "some_user"
		cfg.Rpk.SCRAM.Password = "some_password"
		cfg.Rpk.SCRAM.Type = sarama.SASLTypeSCRAMSHA256
		return cfg
	}

	tests := []struct {
		name  string
		conf  func() *config.Config
		check func(*testing.T, *config.Config, *sarama.Config)
	}{{
		name: "it should load the SCRAM config",
		check: func(st *testing.T, cfg *config.Config, c *sarama.Config) {
			assert.Equal(t, cfg.Rpk.SCRAM.User, c.Net.SASL.User)
			assert.Equal(t, cfg.Rpk.SCRAM.Password, c.Net.SASL.Password)
			assert.Equal(t, sarama.SASLMechanism(cfg.Rpk.SCRAM.Type), c.Net.SASL.Mechanism)
		},
	}, {
		name: "it shouldn't load the SCRAM user if it's missing",
		conf: func() *config.Config {
			cfg := getCfg()
			cfg.Rpk.SCRAM.User = ""
			return cfg
		},
		check: func(st *testing.T, cfg *config.Config, c *sarama.Config) {
			assert.Equal(t, cfg.Rpk.SCRAM.User, c.Net.SASL.User)
			assert.NotEqual(t, cfg.Rpk.SCRAM.Password, c.Net.SASL.Password)
			assert.NotEqual(t, sarama.SASLMechanism(cfg.Rpk.SCRAM.Type), c.Net.SASL.Mechanism)
		},
	}, {
		name: "it shouldn't load the SCRAM password if it's missing",
		conf: func() *config.Config {
			cfg := getCfg()
			cfg.Rpk.SCRAM.Password = ""
			return cfg
		},
		check: func(st *testing.T, cfg *config.Config, c *sarama.Config) {
			assert.NotEqual(t, cfg.Rpk.SCRAM.User, c.Net.SASL.User)
			assert.Equal(t, cfg.Rpk.SCRAM.Password, c.Net.SASL.Password)
			assert.NotEqual(t, sarama.SASLMechanism(cfg.Rpk.SCRAM.Type), c.Net.SASL.Mechanism)
		},
	}, {
		name: "it shouldn't load the SCRAM type if it's missing",
		conf: func() *config.Config {
			cfg := getCfg()
			cfg.Rpk.SCRAM.Type = ""
			return cfg
		},
		check: func(st *testing.T, cfg *config.Config, c *sarama.Config) {
			assert.NotEqual(t, cfg.Rpk.SCRAM.User, c.Net.SASL.User)
			assert.NotEqual(t, cfg.Rpk.SCRAM.Password, c.Net.SASL.Password)
			assert.Equal(t, sarama.SASLMechanism(cfg.Rpk.SCRAM.Type), c.Net.SASL.Mechanism)
		},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(st *testing.T) {
			conf := getCfg()
			if tt.conf != nil {
				conf = tt.conf()
			}
			c, err := kafka.LoadConfig(conf)
			require.NoError(t, err)
			tt.check(st, conf, c)
		})
	}
}
