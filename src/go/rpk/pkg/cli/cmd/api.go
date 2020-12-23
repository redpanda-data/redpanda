// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package cmd

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"vectorized/pkg/cli/cmd/api"
	"vectorized/pkg/cli/cmd/container/common"
	"vectorized/pkg/config"
	"vectorized/pkg/kafka"

	"github.com/Shopify/sarama"
	"github.com/burdiyan/kafkautil"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
)

func NewApiCommand(fs afero.Fs, mgr config.Manager) *cobra.Command {
	var (
		brokers    []string
		configFile string
	)
	command := &cobra.Command{
		Use:   "api",
		Short: "Interact with the Redpanda API",
	}

	command.PersistentFlags().StringSliceVar(
		&brokers,
		"brokers",
		[]string{},
		"Comma-separated list of broker ip:port pairs",
	)
	command.PersistentFlags().StringVar(
		&configFile,
		"config",
		"",
		"Redpanda config file, if not set the file will be searched for"+
			" in the default locations",
	)
	// The ideal way to pass common (global flags') values would be to
	// declare PersistentPreRun hooks on each command root (such as rpk
	// api), validating them there and them passing them down to its
	// subcommands. However, Cobra only executes the last hook defined in
	// the command chain. Since NewTopicCommand requires a PersistentPreRun
	// hook to initialize the sarama Client and Admin, it overrides whatever
	// PersistentPreRun hook was declared in a parent command.
	// An alternative would be to declare a global var to hold the global
	// flag's value, but this would require flattening out the package
	// hierarchy to avoid import cycles (parent command imports child
	// command's package, child cmd import parent cmd's package to access
	// the flag's value), but this leads to entangled code.
	// As a cleaner workaround, the brokers value has to be gotten through a
	// closure with references to the required values (the config file
	// path, the list of brokers passed through --brokers) to deduce the
	// actual brokers list to be used.
	configClosure := findConfigFile(mgr, &configFile)
	brokersClosure := deduceBrokers(fs, configClosure, &brokers)
	producerClosure := createProducer(fs, brokersClosure, configClosure)
	clientClosure := createClient(fs, brokersClosure, configClosure)
	adminClosure := createAdmin(fs, brokersClosure, configClosure)

	command.AddCommand(api.NewStatusCommand(adminClosure))
	command.AddCommand(
		api.NewTopicCommand(fs, clientClosure, adminClosure),
	)
	command.AddCommand(
		api.NewProduceCommand(producerClosure),
	)
	command.AddCommand(
		api.NewConsumeCommand(clientClosure),
	)
	return command
}

// Try to read the config from the default expected locations, or from the
// specific path passed with --config. If --config wasn't passed, and the config
// wasn't found, return the default configuration.
func findConfigFile(
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

func deduceBrokers(
	fs afero.Fs, configuration func() (*config.Config, error), brokers *[]string,
) func() []string {
	return func() []string {
		bs := *brokers
		// Prioritize brokers passed through --brokers
		if len(bs) != 0 {
			log.Debugf("Using --brokers: %s", strings.Join(bs, ", "))
			return bs
		}
		// Otherwise, try to detect if a local container cluster is
		// running, and use its brokers' addresses.
		bs = containerBrokers(fs)
		if len(bs) > 0 {
			log.Debugf(
				"Using container cluster brokers %s",
				strings.Join(bs, ", "),
			)
			return bs
		}
		// Otherwise, try to find an existing config file.
		conf, err := configuration()
		if err != nil {
			log.Trace(
				"Couldn't read the config file." +
					" Assuming 127.0.0.1:9092",
			)
			log.Debug(err)
			return []string{"127.0.0.1:9092"}
		}

		// Add the seed servers' Kafka addrs.
		if len(conf.Redpanda.SeedServers) > 0 {
			for _, b := range conf.Redpanda.SeedServers {
				addr := fmt.Sprintf(
					"%s:%d",
					b.Host.Address,
					conf.Redpanda.KafkaApi.Port,
				)
				bs = append(bs, addr)
			}
		}
		// Add the current node's Kafka addr.
		selfAddr := fmt.Sprintf(
			"%s:%d",
			conf.Redpanda.KafkaApi.Address,
			conf.Redpanda.KafkaApi.Port,
		)
		bs = append(bs, selfAddr)
		log.Debugf(
			"Using brokers from config: %s",
			strings.Join(bs, ", "),
		)
		return bs
	}
}

func createProducer(
	fs afero.Fs,
	brokers func() []string,
	configuration func() (*config.Config, error),
) func(bool, int32) (sarama.SyncProducer, error) {
	return func(jvmPartitioner bool, partition int32) (sarama.SyncProducer, error) {
		conf, err := configuration()
		if err != nil {
			return nil, err
		}
		cfg, err := kafka.LoadConfig(conf)
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

func createClient(
	fs afero.Fs,
	brokers func() []string,
	configuration func() (*config.Config, error),
) func() (sarama.Client, error) {
	return func() (sarama.Client, error) {
		conf, err := configuration()
		if err != nil {
			return nil, err
		}
		bs := brokers()
		return kafka.InitClientWithConf(conf, bs...)
	}
}

func createAdmin(
	fs afero.Fs,
	brokers func() []string,
	configuration func() (*config.Config, error),
) func() (sarama.ClusterAdmin, error) {
	return func() (sarama.ClusterAdmin, error) {
		conf, err := configuration()
		if err != nil {
			return nil, err
		}
		cfg, err := kafka.LoadConfig(conf)
		if err != nil {
			return nil, err
		}
		return sarama.NewClusterAdmin(brokers(), cfg)
	}
}

func containerBrokers(fs afero.Fs) []string {
	c, err := common.NewDockerClient()
	if err != nil {
		log.Debug(err)
		return []string{}
	}
	nodes, err := common.GetExistingNodes(c)
	if err != nil {
		log.Debug(err)
		return []string{}
	}
	grp := errgroup.Group{}
	mu := sync.Mutex{}
	addrs := []string{}
	for _, node := range nodes {
		s := node
		grp.Go(func() error {
			mu.Lock()
			defer mu.Unlock()
			addrs = append(
				addrs,
				fmt.Sprintf(
					"127.0.0.1:%d",
					s.HostKafkaPort,
				),
			)
			return nil
		})
	}
	err = grp.Wait()
	if err != nil {
		log.Debug(err)
		return []string{}
	}
	return addrs
}
