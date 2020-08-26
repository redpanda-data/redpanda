package cmd

import (
	"fmt"
	"strings"
	"vectorized/pkg/cli/cmd/api"
	"vectorized/pkg/config"
	"vectorized/pkg/kafka"

	"github.com/Shopify/sarama"
	"github.com/burdiyan/kafkautil"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func NewApiCommand(fs afero.Fs) *cobra.Command {
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
		config.DefaultConfig().ConfigFile,
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
	brokersClosure := deduceBrokers(fs, &configFile, &brokers)
	producerClosure := createProducer(brokersClosure)
	clientClosure := createClient(brokersClosure)
	adminClosure := createAdmin(brokersClosure)
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

func deduceBrokers(
	fs afero.Fs, configFile *string, brokers *[]string,
) func() []string {
	return func() []string {
		bs := *brokers
		if len(bs) != 0 {
			log.Debugf("Using --brokers: %s", strings.Join(bs, ", "))
			return bs
		}
		conf, err := config.ReadConfigFromPath(fs, *configFile)
		if err != nil {
			log.Trace(
				"Couldn't read the config at '%s'."+
					" Assuming 127.0.0.1:9092",
				*configFile,
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
	brokers func() []string,
) func(bool, int32) (sarama.SyncProducer, error) {
	return func(jvmPartitioner bool, partition int32) (sarama.SyncProducer, error) {
		cfg := kafka.DefaultConfig()
		if jvmPartitioner {
			cfg.Producer.Partitioner = kafkautil.NewJVMCompatiblePartitioner
		}

		if partition > -1 {
			cfg.Producer.Partitioner = sarama.NewManualPartitioner
		}

		return sarama.NewSyncProducer(brokers(), cfg)
	}
}

func createClient(brokers func() []string) func() (sarama.Client, error) {
	return func() (sarama.Client, error) {
		bs := brokers()
		return kafka.InitClient(bs...)
	}
}

func createAdmin(brokers func() []string) func() (sarama.ClusterAdmin, error) {
	return func() (sarama.ClusterAdmin, error) {
		return sarama.NewClusterAdmin(brokers(), kafka.DefaultConfig())
	}
}
