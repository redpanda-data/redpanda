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

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"gopkg.in/yaml.v3"
)

//////////////////////////////
// Prometheus Scrape Config //
//////////////////////////////

type ScrapeConfig struct {
	JobName       string         `yaml:"job_name"`
	StaticConfigs []StaticConfig `yaml:"static_configs"`
	MetricsPath   string         `yaml:"metrics_path"`
	TLSConfig     prometheusTLS  `yaml:"tls_config,omitempty"`
}

type StaticConfig struct {
	Targets []string          `yaml:"targets"`
	Labels  map[string]string `yaml:"labels,omitempty"`
}

type prometheusTLS struct {
	CAFile   string `yaml:"ca_file,omitempty"`
	CertFile string `yaml:"cert_file,omitempty"`
	KeyFile  string `yaml:"key_file,omitempty"`
}

///////////
// Flags //
///////////

type tlsConfig struct {
	TLS       config.TLS
	enableTLS bool
}

type prometheusOptions struct {
	intMetrics bool
	jobName    string
	labels     []string
	nodeAddrs  []string
	seedAddr   string
}

func newPrometheusConfigCmd(fs afero.Fs, p *config.Params) *cobra.Command {
	var (
		opts   prometheusOptions
		tlsCfg tlsConfig
	)
	cmd := &cobra.Command{
		Use:   "prometheus-config",
		Short: "Generate the Prometheus configuration to scrape Redpanda nodes",
		Long:  prometheusHelpText,
		Args:  cobra.NoArgs,
		Run: func(_ *cobra.Command, _ []string) {
			y, err := p.LoadVirtualRedpandaYaml(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)

			yml, err := executePrometheusConfig(y, opts, tlsCfg, fs)
			out.MaybeDieErr(err)
			fmt.Println(string(yml))
		},
	}

	f := cmd.Flags()

	f.StringVar(&opts.jobName, "job-name", "redpanda", "The prometheus job name by which to identify the Redpanda nodes")
	f.StringSliceVar(&opts.nodeAddrs, "node-addrs", nil, "Comma-separated list of admin API host:ports")
	f.StringVar(&opts.seedAddr, "seed-addr", "", "The URL of a Redpanda node with which to discover the rest")
	f.BoolVar(&opts.intMetrics, "internal-metrics", false, "Include scrape config for internal metrics (/metrics)")
	f.StringSliceVar(&opts.labels, "labels", nil, "Comma-separated labels and their target metric (int or pub): [metric|labelName:labelValue, ...]")

	// Backcompat
	f.StringVar(&tlsCfg.TLS.TruststoreFile, "ca-file", "", "CA certificate used to sign node_exporter certificate")
	f.StringVar(&tlsCfg.TLS.CertFile, "cert-file", "", "Cert file presented to node_exporter to authenticate Prometheus as a client")
	f.StringVar(&tlsCfg.TLS.KeyFile, "key-file", "", "Key file presented to node_exporter to authenticate Prometheus as a client")
	f.MarkHidden("ca-file")
	f.MarkHidden("cert-file")
	f.MarkHidden("key-file")

	// Prometheus generation uses its own TLS for some reason.
	// TODO clean up this entire flag set / just use the Redpanda config?
	pf := cmd.PersistentFlags()
	pf.BoolVar(&tlsCfg.enableTLS, config.FlagEnableTLS, false, "Enable TLS for the Kafka API (not necessary if specifying custom certs)")
	pf.StringVar(&tlsCfg.TLS.TruststoreFile, config.FlagTLSCA, "", "The CA certificate to be used for TLS communication with the broker")
	pf.StringVar(&tlsCfg.TLS.CertFile, config.FlagTLSCert, "", "The certificate to be used for TLS authentication with the broker")
	pf.StringVar(&tlsCfg.TLS.KeyFile, config.FlagTLSKey, "", "The certificate key to be used for TLS authentication with the broker")
	return cmd
}

func executePrometheusConfig(
	y *config.RedpandaYaml,
	opts prometheusOptions,
	tlsCfg tlsConfig,
	fs afero.Fs,
) ([]byte, error) {
	if len(opts.nodeAddrs) > 0 {
		return renderConfig(opts.jobName, opts.nodeAddrs, opts.labels, opts.intMetrics, tlsCfg.TLS)
	}
	if opts.seedAddr != "" {
		host, port, err := splitAddress(opts.seedAddr)
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
		return renderConfig(opts.jobName, hosts, opts.labels, opts.intMetrics, tlsCfg.TLS)
	}
	hosts, err := discoverHosts(
		y.Redpanda.KafkaAPI[0].Address,
		y.Redpanda.KafkaAPI[0].Port,
		tlsCfg,
		fs,
	)
	if err != nil {
		return []byte(""), err
	}
	return renderConfig(opts.jobName, hosts, opts.labels, opts.intMetrics, tlsCfg.TLS)
}

func renderConfig(jobName string, targets []string, labels []string, intMetrics bool, tlsConfig config.TLS) ([]byte, error) {
	intLabel, pubLabel, err := parseLabels(labels)
	if err != nil {
		return nil, err
	}

	prometheusTLSConfig := prometheusTLS{
		CAFile:   tlsConfig.TruststoreFile,
		CertFile: tlsConfig.CertFile,
		KeyFile:  tlsConfig.KeyFile,
	}

	scrapeConfigs := []ScrapeConfig{{
		JobName:       jobName,
		StaticConfigs: []StaticConfig{{Targets: targets, Labels: pubLabel}},
		MetricsPath:   "/public_metrics",
		TLSConfig:     prometheusTLSConfig,
	}}
	if intMetrics {
		scrapeConfigs = append(scrapeConfigs, ScrapeConfig{
			JobName:       jobName,
			StaticConfigs: []StaticConfig{{Targets: targets, Labels: intLabel}},
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

// parseLabels parses label strings and returns two maps (int and pub). Labels
// should be in the format "[metric:labelName=labelValue]". 'metric' determines
// if the label targets the public or internal metrics. It also checks for at
// most one label per job.
func parseLabels(labels []string) (int, pub map[string]string, err error) {
	int = make(map[string]string)
	pub = make(map[string]string)

	for _, label := range labels {
		if s := strings.SplitN(label, ":", 2); len(s) > 1 {
			metric, labelFormat := s[0], s[1]
			s = strings.SplitN(labelFormat, "=", 2)
			if len(s) == 1 {
				return nil, nil, fmt.Errorf("unable to parse label %q, is not in the format [metric:labelName=labelValue]", label)
			}
			k, v := s[0], s[1]
			switch strings.ToLower(metric) {
			case "public":
				pub[k] = v
			case "internal":
				int[k] = v
			default:
				return nil, nil, fmt.Errorf("unable to parse label %q, unrecognized metric target %q; please use one of [public, internal]", label, metric)
			}
		} else {
			s := strings.SplitN(label, "=", 2)
			if len(s) == 1 {
				return nil, nil, fmt.Errorf("unable to parse label %q, is not in the format [metric:labelName=labelValue]", label)
			}
			k, v := s[0], s[1]
			pub[k] = v
			int[k] = v
		}
	}
	if len(int) > 1 || len(pub) > 1 {
		return nil, nil, fmt.Errorf("unable to parse labels %v, you can only specify 1 label per job", labels)
	}
	return int, pub, err
}

const prometheusHelpText = `Generate the Prometheus configuration to scrape Redpanda nodes. 

The output of this command should be included in the 'scrape_configs' array 
within the YAML configuration file of your Prometheus instance.

There are different options you can use when generating the configuration:

 - If you provide the --seed-addr flag, the command will use the address to 
   discover the rest of the cluster hosts using Redpanda's Kafka API.
 - If you provide the --node-addrs flag, the command will directly use the 
   provided addresses.
 - If neither --seed-addr nor --node-addrs are passed, the command will read the 
   redpanda config file and use the node IP configured there.

If the node you want to scrape uses TLS, you can provide the TLS flags 
(--tls-key, --tls-cert, and --tls-truststore). The command will generate the 
required tls_config section in the scrape configuration.

Additionally, you have the option to define labels for the target in the 
static-config section by using the --labels flag. You can specify the desired 
metric that the label should target, either internal (/metrics) or public 
(/public_metrics).

For example:

  --job-name test --labels "public:group=one,internal:group=two"

This will result in two separate configs for the test job, each with a 
different label:

  - job_name: test
    static_configs:
      - targets: [<targets>]
        labels:
          group: one
    metrics_path: /public_metrics
  - job_name: test
    static_configs:
      - targets: [<targets>]
        labels:
          group: two
    metrics_path: /metrics

You can only provide one label per job. By default, if no metric target is 
specified, the label will be shared across the jobs.
`
