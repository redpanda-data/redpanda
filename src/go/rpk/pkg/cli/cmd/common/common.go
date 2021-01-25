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
)

const FeedbackMsg = `We'd love to hear about your experience with redpanda:
https://vectorized.io/feedback`

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

func DeduceBrokers(
	fs afero.Fs,
	client func() (common.Client, error),
	configuration func() (*config.Config, error),
	brokers *[]string,
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

func CreateProducer(
	brokers func() []string, configuration func() (*config.Config, error),
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

func CreateClient(
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

func CreateAdmin(
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
