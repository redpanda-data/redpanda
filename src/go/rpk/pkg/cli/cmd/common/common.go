// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package common

import (
	"crypto/tls"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/burdiyan/kafkautil"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/cli/cmd/container/common"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/config"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/kafka"
	vtls "github.com/vectorizedio/redpanda/src/go/rpk/pkg/tls"
)

const FeedbackMsg = `We'd love to hear about your experience with redpanda:
https://vectorized.io/feedback`

const (
	saslMechanismFlag          = "sasl-mechanism"
	enableTLSFlag              = "tls-enabled"
	certFileFlag               = "tls-cert"
	keyFileFlag                = "tls-key"
	truststoreFileFlag         = "tls-truststore"
	adminAPIEnableTLSFlag      = "admin-api-tls-enabled"
	adminAPICertFileFlag       = "admin-api-tls-cert"
	adminAPIKeyFileFlag        = "admin-api-tls-key"
	adminAPITruststoreFileFlag = "admin-api-tls-truststore"
)

var ErrNoCredentials = errors.New("empty username and password")

func Deprecated(newCmd *cobra.Command, newUse string) *cobra.Command {
	newCmd.Deprecated = deprecationMessage(newUse)
	newCmd.Hidden = true
	return newCmd
}

func deprecationMessage(newUse string) string {
	return fmt.Sprintf("use '%s' instead.", newUse)
}

// exactArgs makes sure exactly n arguments are passed, if not, a custom error
// err is returned back. This is so we can return more contextually friendly errors back
// to users.
func ExactArgs(n int, err string) cobra.PositionalArgs {
	return func(cmd *cobra.Command, args []string) error {
		if len(args) != n {
			return fmt.Errorf(err + "\n\n" + cmd.UsageString())
		}
		return nil
	}
}

// Try to read the config from the default expected locations, or from the
// specific path passed with --config. If --config wasn't passed, and the config
// wasn't found, return the default configuration.
func FindConfigFile(
	mgr config.Manager, configFile *string,
) func() (*config.Config, error) {
	var conf *config.Config
	var err error
	return func() (*config.Config, error) {
		if conf != nil {
			return conf, nil
		}
		conf, err = mgr.ReadOrFind(*configFile)
		if err != nil {
			log.Debug(err)
			if os.IsNotExist(err) && *configFile == "" {
				log.Debug(
					"Config file not found and --config" +
						" wasn't passed, using default" +
						" config",
				)
				return config.Default(), nil
			}
		}
		return conf, err
	}
}

// Returns the configured brokers list.
// The configuration priority is as follows (highest to lowest):
// 1. Values passed through flags (`brokers`)
// 2. A list of brokers set through the `REDPANDA_BROKERS` environment
//    variable
// 3. The addresses of a running local container cluster deployed with
//    `rpk container start`.
// 4. A list of brokers from the `rpk.kafka_api.brokers` field in the
//    config file.
// 5. The listeners in `redpanda.kafka_api`, and any seed brokers
//    (`redpanda.seed_servers`)
//
// If none of those sources yield a list of broker addresses, the default
// local address (127.0.0.1:9092) is assumed.
func DeduceBrokers(
	client func() (common.Client, error),
	configuration func() (*config.Config, error),
	brokers *[]string,
) func() []string {
	return func() []string {
		defaultAddr := "127.0.0.1:9092"
		defaultAddrs := []string{defaultAddr}
		bs := *brokers
		// Prioritize brokers passed through --brokers
		if len(bs) != 0 {
			log.Debugf("Using --brokers: %s", strings.Join(bs, ", "))
			return bs
		}
		// If no values were passed directly, look for the env vars.
		envVar := "REDPANDA_BROKERS"
		envBrokers := os.Getenv(envVar)
		if envBrokers != "" {
			log.Debugf("Using %s: %s", envVar, envBrokers)
			return strings.Split(envBrokers, ",")
		}
		// Otherwise, try to detect if a local container cluster is
		// running, and use its brokers' addresses.
		c, err := client()
		if err != nil {
			log.Debug(err)
		} else {
			bs, stopped := ContainerBrokers(c)
			if len(stopped) > 0 {
				log.Errorf(
					"%d local container nodes have stopped. Run"+
						" 'rpk container start' to restart them.",
					len(stopped),
				)
			}
			if len(bs) > 0 {
				log.Debugf(
					"Using container cluster brokers %s",
					strings.Join(bs, ", "),
				)
				return bs
			}
		}

		// Otherwise, try to find an existing config file.
		conf, err := configuration()
		if err != nil {
			log.Debugf(
				"Couldn't read the config file."+
					" Assuming %s.",
				defaultAddr,
			)
			log.Debug(err)
			return defaultAddrs
		}

		// Check if the rpk config has a list of brokers.
		if len(conf.Rpk.KafkaApi.Brokers) != 0 {
			log.Debugf(
				"Using rpk.kafka_api.brokers: %s",
				strings.Join(conf.Rpk.KafkaApi.Brokers, ", "),
			)
			return conf.Rpk.KafkaApi.Brokers
		}

		log.Debug(
			"Empty rpk.kafka_api.brokers. Checking redpanda.kafka_api.",
		)
		// If rpk.kafka_api.brokers is empty, check for a local broker's cluster configuration
		if len(conf.Redpanda.KafkaApi) == 0 && len(conf.Redpanda.SeedServers) == 0 {
			log.Debugf(
				"Empty redpanda.kafka_api and redpanda.seed_servers."+
					" Assuming %s.",
				defaultAddr,
			)
			return defaultAddrs
		}
		// Add the seed servers' Kafka addrs.
		for _, b := range conf.Redpanda.SeedServers {
			addr := fmt.Sprintf(
				"%s:%d",
				b.Host.Address,
				b.Host.Port,
			)
			bs = append(bs, addr)
		}
		if len(conf.Redpanda.KafkaApi) > 0 {
			// Add the current node's 1st Kafka listener.
			selfAddr := fmt.Sprintf(
				"%s:%d",
				conf.Redpanda.KafkaApi[0].Address,
				conf.Redpanda.KafkaApi[0].Port,
			)
			bs = append(bs, selfAddr)
		}
		log.Debugf(
			"Using brokers from config: %s",
			strings.Join(bs, ", "),
		)
		return bs
	}
}

func CreateProducer(
	brokers func() []string,
	configuration func() (*config.Config, error),
	tlsConfig func() (*tls.Config, error),
	authConfig func() (*config.SASL, error),
) func(bool, int32) (sarama.SyncProducer, error) {
	return func(jvmPartitioner bool, partition int32) (sarama.SyncProducer, error) {
		conf, err := configuration()
		if err != nil {
			return nil, err
		}

		tls, err := tlsConfig()
		if err != nil {
			return nil, err
		}

		sasl, err := authConfig()
		if err != nil {
			// If the user passed the credentials and there was still an
			// error, return it.
			if !errors.Is(err, ErrNoCredentials) {
				return nil, err
			}
			// If no SASL config was set, try to look for it in the
			// config file.
			if conf.Rpk.KafkaApi.SASL != nil {
				sasl = conf.Rpk.KafkaApi.SASL
			} else {
				sasl = conf.Rpk.SASL
			}
		}

		cfg, err := kafka.LoadConfig(tls, sasl)
		if err != nil {
			return nil, err
		}
		if jvmPartitioner {
			cfg.Producer.Partitioner = kafkautil.NewJVMCompatiblePartitioner
		}

		if partition > -1 {
			cfg.Producer.Partitioner = sarama.NewManualPartitioner
		}

		return sarama.NewSyncProducer(brokers(), cfg)
	}
}

func CreateClient(
	brokers func() []string,
	configuration func() (*config.Config, error),
	tlsConfig func() (*tls.Config, error),
	authConfig func() (*config.SASL, error),
) func() (sarama.Client, error) {
	return func() (sarama.Client, error) {
		conf, err := configuration()
		if err != nil {
			return nil, err
		}
		tls, err := tlsConfig()
		if err != nil {
			return nil, err
		}

		sasl, err := authConfig()
		if err != nil {
			// If the user passed the credentials and there was still an
			// error, return it.
			if !errors.Is(err, ErrNoCredentials) {
				return nil, err
			}
			// If no SASL config was set, try to look for it in the
			// config file.
			if conf.Rpk.KafkaApi.SASL != nil {
				sasl = conf.Rpk.KafkaApi.SASL
			} else {
				sasl = conf.Rpk.SASL
			}
		}

		bs := brokers()
		client, err := kafka.InitClientWithConf(tls, sasl, bs...)
		return client, wrapConnErr(err, bs)
	}
}

func CreateAdmin(
	brokers func() []string,
	configuration func() (*config.Config, error),
	tlsConfig func() (*tls.Config, error),
	authConfig func() (*config.SASL, error),
) func() (sarama.ClusterAdmin, error) {
	return func() (sarama.ClusterAdmin, error) {
		var err error
		conf, err := configuration()
		if err != nil {
			return nil, err
		}

		tls, err := tlsConfig()
		if err != nil {
			return nil, err
		}

		sasl, err := authConfig()
		if err != nil {
			// If the user passed the credentials and there was still an
			// error, return it.
			if !errors.Is(err, ErrNoCredentials) {
				return nil, err
			}
			// If no SASL config was set, try to look for it in the
			// config file.
			if conf.Rpk.KafkaApi.SASL != nil {
				sasl = conf.Rpk.KafkaApi.SASL
			} else {
				sasl = conf.Rpk.SASL
			}
		}

		cfg, err := kafka.LoadConfig(tls, sasl)
		if err != nil {
			return nil, err
		}

		bs := brokers()
		admin, err := sarama.NewClusterAdmin(bs, cfg)
		return admin, wrapConnErr(err, bs)
	}
}

// Returns the SASL Auth configuration for the Kafka API.
// The configuration priority is as follows (highest to lowest):
// 1. Values passed through flags
// 2. A list of brokers set through environment variables (listed below)
// 3. Values set in `rpk.kafka_api.sasl`.
// 5. Values set in `rpk.sasl` (deprecated)
func KafkaAuthConfig(
	user, password, mechanism *string,
	configuration func() (*config.Config, error),
) func() (*config.SASL, error) {
	return func() (*config.SASL, error) {
		u := *user
		p := *password
		m := *mechanism
		// If the values are empty, check for env vars.
		if u == "" {
			u = os.Getenv("REDPANDA_SASL_USERNAME")
		}
		if p == "" {
			p = os.Getenv("REDPANDA_SASL_PASSWORD")
		}
		if m == "" {
			m = os.Getenv("REDPANDA_SASL_MECHANISM")
		}

		// Then check the config if any of the values are empty.
		if u == "" || p == "" || m == "" {
			conf, err := configuration()
			if err != nil {
				return nil, err
			}
			// First check the recommended fields
			if conf.Rpk.KafkaApi.SASL != nil {
				if u == "" {
					u = conf.Rpk.KafkaApi.SASL.User
				}
				if p == "" {
					p = conf.Rpk.KafkaApi.SASL.Password
				}
				if m == "" {
					m = conf.Rpk.KafkaApi.SASL.Mechanism
				}
			} else if conf.Rpk.SASL != nil {
				// Otherwise, check the deprecated fields for backwards-compatibility
				if u == "" {
					u = conf.Rpk.SASL.User
				}
				if p == "" {
					p = conf.Rpk.SASL.Password
				}
				if m == "" {
					m = conf.Rpk.SASL.Mechanism
				}
			}
		}

		if u == "" && p == "" {
			return nil, ErrNoCredentials
		}
		if u == "" && p != "" {
			return nil, errors.New("empty user. Pass --user or set rpk.kafka_api.sasl.user.")
		}
		if u != "" && p == "" {
			return nil, errors.New("empty password. Pass --password or set rpk.kafka_api.sasl.password.")
		}
		if m != sarama.SASLTypeSCRAMSHA256 && m != sarama.SASLTypeSCRAMSHA512 {
			return nil, fmt.Errorf(
				"unsupported mechanism '%s'. Pass --%s or set rpk.kafka_api.sasl.mechanism."+
					" Supported: %s, %s.",
				m,
				saslMechanismFlag,
				sarama.SASLTypeSCRAMSHA256,
				sarama.SASLTypeSCRAMSHA512,
			)
		}
		return &config.SASL{
			User:      u,
			Password:  p,
			Mechanism: m,
		}, nil
	}
}

func BuildAdminApiTLSConfig(
	fs afero.Fs,
	enableTLS *bool,
	certFile, keyFile, truststoreFile *string,
	configuration func() (*config.Config, error),
) func() (*tls.Config, error) {
	return func() (*tls.Config, error) {
		defaultVal := func() (*config.TLS, error) {
			conf, err := configuration()
			if err != nil {
				return nil, err
			}
			// If the specific field is there, return it.
			if conf.Rpk.AdminApi.TLS != nil {
				return conf.Rpk.AdminApi.TLS, nil
			}
			// Otherwise return the general purpose TLS field (deprecated).
			return conf.Rpk.TLS, nil
		}
		return buildTLS(
			fs,
			enableTLS,
			certFile,
			keyFile,
			truststoreFile,
			"REDPANDA_ADMIN_TLS_CERT",
			"REDPANDA_ADMIN_TLS_KEY",
			"REDPANDA_ADMIN_TLS_TRUSTSTORE",
			defaultVal,
		)
	}
}

func BuildKafkaTLSConfig(
	fs afero.Fs,
	enableTLS *bool,
	certFile, keyFile, truststoreFile *string,
	configuration func() (*config.Config, error),
) func() (*tls.Config, error) {
	return func() (*tls.Config, error) {
		defaultVal := func() (*config.TLS, error) {
			conf, err := configuration()
			if err != nil {
				return nil, err
			}
			// If the specific field is there, return it.
			if conf.Rpk.KafkaApi.TLS != nil {
				return conf.Rpk.KafkaApi.TLS, nil
			}
			// Otherwise return the general purpose TLS field (deprecated).
			return conf.Rpk.TLS, nil
		}
		return buildTLS(
			fs,
			enableTLS,
			certFile,
			keyFile,
			truststoreFile,
			"REDPANDA_TLS_CERT",
			"REDPANDA_TLS_KEY",
			"REDPANDA_TLS_TRUSTSTORE",
			defaultVal,
		)
	}
}

// Builds an instance of config.TLS.
// If certFile, keyFile or truststoreFile are nil, then their corresponding
// env vars are checked (given by certEnvVar, keyEnvVar & truststoreEnvVar).
// If after that no value is found for any of them, the result of calling
// defaultVal is returned.
func buildTLS(
	fs afero.Fs,
	enableTLS *bool,
	certFile, keyFile, truststoreFile *string,
	certEnvVar, keyEnvVar, truststoreEnvVar string,
	defaultVal func() (*config.TLS, error),
) (*tls.Config, error) {
	// Give priority to building the TLS config with args that were passed
	// directly or as env vars.
	enable := *enableTLS
	c := *certFile
	k := *keyFile
	t := *truststoreFile

	if c == "" {
		c = os.Getenv(certEnvVar)
	}
	if k == "" {
		k = os.Getenv(keyEnvVar)
	}
	if t == "" {
		t = os.Getenv(truststoreEnvVar)
	}
	if t == "" && c == "" && k == "" {
		// If the values weren't set with flags nor env vars,
		// return the TLS config for the Admin API from the config
		defaultTLS, err := defaultVal()
		if err != nil {
			return nil, err
		}
		if defaultTLS != nil {
			c = defaultTLS.CertFile
			k = defaultTLS.KeyFile
			t = defaultTLS.TruststoreFile
		}
	}
	return vtls.BuildTLSConfig(
		fs,
		enable,
		c,
		k,
		t,
	)
}

func CreateDockerClient() (common.Client, error) {
	return common.NewDockerClient()
}

func ContainerBrokers(c common.Client) ([]string, []string) {
	nodes, err := common.GetExistingNodes(c)
	if err != nil {
		log.Debug(err)
		return nil, nil
	}
	if len(nodes) == 0 {
		return nil, nil
	}

	addrs := []string{}
	stopped := []string{}
	for _, node := range nodes {
		addr := common.HostAddr(node.HostKafkaPort)
		if !node.Running {
			stopped = append(stopped, addr)
			continue
		}
		addrs = append(addrs, addr)
	}
	return addrs, stopped
}

func AddKafkaFlags(
	command *cobra.Command,
	configFile, user, password, saslMechanism *string,
	enableTLS *bool,
	certFile, keyFile, truststoreFile *string,
	brokers *[]string,
) *cobra.Command {
	command.PersistentFlags().StringSliceVar(
		brokers,
		"brokers",
		[]string{},
		"Comma-separated list of broker ip:port pairs (e.g."+
			" --brokers '192.168.78.34:9092,192.168.78.35:9092,192.179.23.54:9092' )."+
			" Alternatively, you may set the REDPANDA_BROKERS environment"+
			" variable with the comma-separated list of broker addresses.",
	)
	command.PersistentFlags().StringVar(
		configFile,
		"config",
		"",
		"Redpanda config file, if not set the file will be searched for"+
			" in the default locations",
	)
	command.PersistentFlags().StringVar(
		user,
		"user",
		"",
		"SASL user to be used for authentication.",
	)
	command.PersistentFlags().StringVar(
		password,
		"password",
		"",
		"SASL password to be used for authentication.",
	)
	command.PersistentFlags().StringVar(
		saslMechanism,
		saslMechanismFlag,
		"",
		fmt.Sprintf(
			"The authentication mechanism to use. Supported values: %s, %s.",
			sarama.SASLTypeSCRAMSHA256,
			sarama.SASLTypeSCRAMSHA512,
		),
	)

	AddTLSFlags(command, enableTLS, certFile, keyFile, truststoreFile)

	return command
}

func AddTLSFlags(
	command *cobra.Command,
	enableTLS *bool,
	certFile, keyFile, truststoreFile *string,
) *cobra.Command {
	command.PersistentFlags().BoolVar(
		enableTLS,
		enableTLSFlag,
		false,
		"Enable TLS for the Kafka API (not necessary if specifying custom certs).",
	)
	command.PersistentFlags().StringVar(
		certFile,
		certFileFlag,
		"",
		"The certificate to be used for TLS authentication with the broker.",
	)
	command.PersistentFlags().StringVar(
		keyFile,
		keyFileFlag,
		"",
		"The certificate key to be used for TLS authentication with the broker.",
	)
	command.PersistentFlags().StringVar(
		truststoreFile,
		truststoreFileFlag,
		"",
		"The truststore to be used for TLS communication with the broker.",
	)

	return command
}

func AddAdminAPITLSFlags(
	command *cobra.Command,
	enableTLS *bool,
	certFile, keyFile, truststoreFile *string,
) *cobra.Command {
	command.PersistentFlags().BoolVar(
		enableTLS,
		adminAPIEnableTLSFlag,
		false,
		"Enable TLS for the Admin API (not necessary if specifying custom certs).",
	)
	command.PersistentFlags().StringVar(
		certFile,
		adminAPICertFileFlag,
		"",
		"The certificate to be used for TLS authentication with the Admin API.",
	)
	command.PersistentFlags().StringVar(
		keyFile,
		adminAPIKeyFileFlag,
		"",
		"The certificate key to be used for TLS authentication with the Admin API.",
	)
	command.PersistentFlags().StringVar(
		truststoreFile,
		adminAPITruststoreFileFlag,
		"",
		"The truststore to be used for TLS communication with the Admin API.",
	)

	return command
}

func wrapConnErr(err error, addrs []string) error {
	if err == nil {
		return nil
	}
	log.Debug(err)
	return fmt.Errorf("couldn't connect to redpanda at %s."+
		" Try using --brokers to specify other brokers to connect to.",
		strings.Join(addrs, ", "),
	)
}
