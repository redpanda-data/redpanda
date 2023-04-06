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
	"strconv"
	"strings"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cmd/common"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"gopkg.in/yaml.v3"
)

type ScrapeConfig struct {
	JobName       string         `yaml:"job_name"`
	StaticConfigs []StaticConfig `yaml:"static_configs"`
	MetricsPath   string         `yaml:"metrics_path"`
	TLSConfig     prometheusTLS  `yaml:"tls_config,omitempty"`
}

type StaticConfig struct {
	Targets []string `yaml:"targets"`
}

type prometheusTLS struct {
	CAFile   string `yaml:"ca_file,omitempty"`
	CertFile string `yaml:"cert_file,omitempty"`
	KeyFile  string `yaml:"key_file,omitempty"`
}

type tlsConfig struct {
	TLS       config.TLS
	enableTLS bool
}

func newPrometheusConfigCmd(fs afero.Fs) *cobra.Command {
	var (
		configFile string
		intMetrics bool
		jobName    string
		nodeAddrs  []string
		seedAddr   string
		tlsConfig  tlsConfig
	)
	command := &cobra.Command{
		Use:   "prometheus-config",
		Short: "Generate the Prometheus configuration to scrape redpanda nodes",
		Long: `
Generate the Prometheus configuration to scrape redpanda nodes. This command's
output should be added to the 'scrape_configs' array in your Prometheus
instance's YAML config file.

If --seed-addr is passed, it will be used to discover the rest of the cluster
hosts via Redpanda's Kafka API. If --node-addrs is passed, they will be used
directly. Otherwise, 'rpk generate prometheus-conf' will read the redpanda
config file and use the node IP configured there. --config may be passed to
specify an arbitrary config file.

If the node you want to scrape uses TLS, you can provide the TLS flags:
--tls-key, --tls-cert, and --tls-truststore and rpk will generate the required
tls_config section in the scrape configuration.`,
		Run: func(cmd *cobra.Command, args []string) {
			p := config.ParamsFromCommand(cmd)
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			yml, err := executePrometheusConfig(
				cfg,
				jobName,
				nodeAddrs,
				seedAddr,
				intMetrics,
				tlsConfig,
				fs,
			)
			out.MaybeDieErr(err)
			fmt.Println(string(yml))
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
	command.Flags().BoolVar(&intMetrics, "internal-metrics", false, "Include scrape config for internal metrics (/metrics)")
	command.Flags().StringVar(&tlsConfig.TLS.TruststoreFile, "ca-file", "", "CA certificate used to sign node_exporter certificate")
	command.Flags().StringVar(&tlsConfig.TLS.CertFile, "cert-file", "", "Cert file presented to node_exporter to authenticate Prometheus as a client")
	command.Flags().StringVar(&tlsConfig.TLS.KeyFile, "key-file", "", "Key file presented to node_exporter to authenticate Prometheus as a client")
	command.Flags().MarkHidden("ca-file")
	command.Flags().MarkHidden("cert-file")
	command.Flags().MarkHidden("key-file")

	common.AddTLSFlags(command, &tlsConfig.enableTLS, &tlsConfig.TLS.CertFile, &tlsConfig.TLS.KeyFile, &tlsConfig.TLS.TruststoreFile)
	return command
}

func executePrometheusConfig(
	cfg *config.Config,
	jobName string,
	nodeAddrs []string,
	seedAddr string,
	intMetrics bool,
	tlsCfg tlsConfig,
	fs afero.Fs,
) ([]byte, error) {
	if len(nodeAddrs) > 0 {
		return renderConfig(jobName, nodeAddrs, intMetrics, tlsCfg.TLS)
	}
	if seedAddr != "" {
		host, port, err := splitAddress(seedAddr)
		if err != nil {
			return []byte(""), err
		}
		if port == 0 {
			port = 9092
		}
		hosts, err := discoverHosts(host, port, tlsCfg, fs)
		if err != nil {
			return []byte(""), err
		}
		return renderConfig(jobName, hosts, intMetrics, tlsCfg.TLS)
	}
	hosts, err := discoverHosts(
		cfg.Redpanda.KafkaAPI[0].Address,
		cfg.Redpanda.KafkaAPI[0].Port,
		tlsCfg,
		fs,
	)
	if err != nil {
		return []byte(""), err
	}
	return renderConfig(jobName, hosts, intMetrics, tlsCfg.TLS)
}

func renderConfig(jobName string, targets []string, intMetrics bool, tlsConfig config.TLS) ([]byte, error) {
	prometheusTLSConfig := prometheusTLS{
		CAFile:   tlsConfig.TruststoreFile,
		CertFile: tlsConfig.CertFile,
		KeyFile:  tlsConfig.KeyFile,
	}

	scrapeConfigs := []ScrapeConfig{{
		JobName:       jobName,
		StaticConfigs: []StaticConfig{{Targets: targets}},
		MetricsPath:   "/public_metrics",
		TLSConfig:     prometheusTLSConfig,
	}}
	if intMetrics {
		scrapeConfigs = append(scrapeConfigs, ScrapeConfig{
			JobName:       jobName,
			StaticConfigs: []StaticConfig{{Targets: targets}},
			MetricsPath:   "/metrics",
			TLSConfig:     prometheusTLSConfig,
		})
	}
	return yaml.Marshal(scrapeConfigs)
}

func discoverHosts(url string, port int, tlsConfig tlsConfig, fs afero.Fs) ([]string, error) {
	addr := net.JoinHostPort(url, strconv.Itoa(port))
	cl, err := createClient(fs, addr, tlsConfig)
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

func createClient(fs afero.Fs, addr string, tlsCfg tlsConfig) (*kgo.Client, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(addr),
	}
	if tlsCfg.TLS != (config.TLS{}) || tlsCfg.enableTLS {
		tc, err := tlsCfg.TLS.Config(fs)
		if err != nil {
			return nil, err
		}
		if tc != nil {
			opts = append(opts, kgo.DialTLSConfig(tc))
		}
	}

	return kgo.NewClient(opts...)
}
