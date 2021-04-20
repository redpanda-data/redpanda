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
			c, err := kafka.LoadConfig(&conf.Rpk.TLS, &conf.Rpk.SCRAM)
			require.NoError(t, err)
			tt.check(st, conf, c)
		})
	}
}

func TestConfigureSASL(t *testing.T) {
	tests := []struct {
		name           string
		scram          *config.SCRAM
		check          func(*testing.T, *sarama.Config)
		expectedErrMsg string
	}{{
		name: "it should fail if the mechanism is not supported",
		scram: &config.SCRAM{
			User:     "admin",
			Password: "admin123",
			Type:     "unsupported",
		},
		expectedErrMsg: "unrecongnized Salted Challenge Response Authentication Mechanism (SCRAM): 'unsupported'.",
	}, {
		name: "it shouldn't enable SASL if user is empty",
		scram: &config.SCRAM{
			Password: "admin123",
			Type:     "SCRAM-SHA-256",
		},
		check: func(st *testing.T, cfg *sarama.Config) {
			require.False(st, cfg.Net.SASL.Enable, "cfg.Net.SASL.Enable")
			require.Empty(st, cfg.Net.SASL.User, "cfg.Net.SASL.User")
			require.Empty(st, cfg.Net.SASL.Password, "cfg.Net.SASL.Password")
			require.Empty(st, cfg.Net.SASL.Mechanism, "cfg.Net.SASL.Mechanism")
			require.Nil(st, cfg.Net.SASL.SCRAMClientGeneratorFunc, "cfg.Net.SASL.SCRAMClientGeneratorFunc")
		},
	}, {
		name: "it shouldn't enable SASL if password is empty",
		scram: &config.SCRAM{
			User: "user1",
			Type: "SCRAM-SHA-256",
		},
		check: func(st *testing.T, cfg *sarama.Config) {
			require.False(st, cfg.Net.SASL.Enable, "cfg.Net.SASL.Enable")
			require.Empty(st, cfg.Net.SASL.User, "cfg.Net.SASL.User")
			require.Empty(st, cfg.Net.SASL.Password, "cfg.Net.SASL.Password")
			require.Empty(st, cfg.Net.SASL.Mechanism, "cfg.Net.SASL.Mechanism")
			require.Nil(st, cfg.Net.SASL.SCRAMClientGeneratorFunc, "cfg.Net.SASL.SCRAMClientGeneratorFunc")
		},
	}, {
		name: "it shouldn't enable SASL if mechanism is empty",
		scram: &config.SCRAM{
			User:     "user1",
			Password: "pass",
		},
		check: func(st *testing.T, cfg *sarama.Config) {
			require.False(st, cfg.Net.SASL.Enable, "cfg.Net.SASL.Enable")
			require.Empty(st, cfg.Net.SASL.User, "cfg.Net.SASL.User")
			require.Empty(st, cfg.Net.SASL.Password, "cfg.Net.SASL.Password")
			require.Empty(st, cfg.Net.SASL.Mechanism, "cfg.Net.SASL.Mechanism")
			require.Nil(st, cfg.Net.SASL.SCRAMClientGeneratorFunc, "cfg.Net.SASL.SCRAMClientGeneratorFunc")
		},
	}, {
		name: "it should set the appropriate mechanism for SCRAM-SHA-256",
		scram: &config.SCRAM{
			User:     "user1",
			Password: "pass",
			Type:     "SCRAM-SHA-256",
		},
		check: func(st *testing.T, cfg *sarama.Config) {
			require.Equal(st, sarama.SASLTypeSCRAMSHA256, string(cfg.Net.SASL.Mechanism))
		},
	}, {
		name: "it should set the appropriate mechanism for SCRAM-SHA-512",
		scram: &config.SCRAM{
			User:     "user1",
			Password: "pass",
			Type:     "SCRAM-SHA-512",
		},
		check: func(st *testing.T, cfg *sarama.Config) {
			require.Equal(st, sarama.SASLTypeSCRAMSHA512, string(cfg.Net.SASL.Mechanism))
		},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(st *testing.T) {
			res, err := kafka.ConfigureSASL(kafka.DefaultConfig(), tt.scram)
			if tt.expectedErrMsg != "" {
				require.EqualError(st, err, tt.expectedErrMsg)
				return
			}
			tt.check(st, res)
		})
	}
}
