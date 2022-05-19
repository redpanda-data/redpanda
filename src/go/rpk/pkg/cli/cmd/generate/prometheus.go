// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package generate

import (
	"context"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"gopkg.in/yaml.v2"
)

type ScrapeConfig struct {
	JobName       string         `yaml:"job_name"`
	StaticConfigs []StaticConfig `yaml:"static_configs"`
}

type StaticConfig struct {
	Targets []string `yaml:"targets"`
}

func NewPrometheusConfigCmd(fs afero.Fs) *cobra.Command {
	var (
		jobName    string
		nodeAddrs  []string
		seedAddr   string
		configFile string
	)
	command := &cobra.Command{
		Use:   "prometheus-config",
		Short: "Generate the Prometheus configuration to scrape redpanda nodes.",
		Long: `
Generate the Prometheus configuration to scrape redpanda nodes. This command's
output should be added to the 'scrape_configs' array in your Prometheus
instance's YAML config file.

If --seed-addr is passed, it will be used to discover the rest of the cluster
hosts via redpanda's Kafka API. If --node-addrs is passed, they will be used
directly. Otherwise, 'rpk generate prometheus-conf' will read the redpanda
config file and use the node IP configured there. --config may be passed to
specify an arbitrary config file.`,
		Run: func(cmd *cobra.Command, args []string) {
			log.SetFormatter(cli.NewNoopFormatter())
			// The logger's default stream is stderr, which prevents piping to files
			// from working without redirecting them with '2>&1'.
			if log.StandardLogger().Out == os.Stderr {
				log.SetOutput(os.Stdout)
			}
			p := config.ParamsFromCommand(cmd)
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			yml, err := executePrometheusConfig(
				cfg,
				jobName,
				nodeAddrs,
				seedAddr,
			)
			out.MaybeDieErr(err)
			log.Infof("\n%s", string(yml))
		},
	}
	command.Flags().StringVar(
		&jobName,
		"job-name",
		"redpanda",
		"The prometheus job name by which to identify the redpanda nodes")
	command.Flags().StringSliceVar(
		&nodeAddrs,
		"node-addrs",
		[]string{},
		fmt.Sprintf(`A comma-delimited list of the addresses (<host>:<port>) of all the redpanda nodes in a cluster. The port must be the one configured for the nodes' admin API (%d by default)`,
			config.DefaultAdminPort,
		))
	command.Flags().StringVar(
		&seedAddr,
		"seed-addr",
		"",
		"The URL of a redpanda node with which to discover the rest")
	command.Flags().StringVar(
		&configFile,
		"config",
		"",
		"The path to the redpanda config file")
	return command
}

func executePrometheusConfig(
	cfg *config.Config,
	jobName string,
	nodeAddrs []string,
	seedAddr string,
) ([]byte, error) {
	if len(nodeAddrs) > 0 {
		return renderConfig(jobName, nodeAddrs)
	}
	if seedAddr != "" {
		host, port, err := splitAddress(seedAddr)
		if err != nil {
			return []byte(""), err
		}
		if port == 0 {
			port = 9092
		}
		hosts, err := discoverHosts(host, port)
		if err != nil {
			return []byte(""), err
		}
		return renderConfig(jobName, hosts)
	}
	hosts, err := discoverHosts(
		cfg.Redpanda.KafkaAPI[0].Address,
		cfg.Redpanda.KafkaAPI[0].Port,
	)
	if err != nil {
		return []byte(""), err
	}
	return renderConfig(jobName, hosts)
}

func renderConfig(jobName string, targets []string) ([]byte, error) {
	scrapeConfig := ScrapeConfig{
		JobName:       jobName,
		StaticConfigs: []StaticConfig{{Targets: targets}},
	}
	return yaml.Marshal([]ScrapeConfig{scrapeConfig})
}

func discoverHosts(url string, port int) ([]string, error) {
	addr := net.JoinHostPort(url, strconv.Itoa(port))
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr))
	if err != nil {
		return nil, err
	}
	var hosts []string
	brokers, err := kadm.NewClient(cl).ListBrokers(context.Background())
	if err != nil {
		return nil, err
	}

	for _, b := range brokers {
		hosts = append(
			hosts,
			net.JoinHostPort(b.Host, strconv.Itoa(config.DefaultAdminPort)),
		)
	}
	return hosts, nil
}

func splitAddress(address string) (string, int, error) {
	parts := strings.Split(address, ":")
	if len(parts) == 0 {
		return "", 0, fmt.Errorf(
			"couldn't parse address '%s'",
			address,
		)
	}
	if len(parts) == 1 {
		return parts[0], 0, nil
	}
	host := parts[0]
	var err error
	port, err := strconv.Atoi(parts[1])
	if err != nil {
		return "", 0, err
	}
	return host, port, nil
}
