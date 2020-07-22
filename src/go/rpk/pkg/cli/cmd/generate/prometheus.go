package generate

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"vectorized/pkg/cli"
	"vectorized/pkg/config"
	"vectorized/pkg/kafka"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
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
especify an arbitrary config file.`,
		RunE: func(ccmd *cobra.Command, args []string) error {
			log.SetFormatter(cli.NewNoopFormatter())
			// The logger's default stream is stderr, which prevents piping to files
			// from working without redirecting them with '2>&1'.
			if log.StandardLogger().Out == os.Stderr {
				log.SetOutput(os.Stdout)
			}
			yml, err := executePrometheusConfig(
				fs,
				jobName,
				nodeAddrs,
				seedAddr,
				configFile,
			)
			if err != nil {
				return err
			}
			log.Infof("\n%s", string(yml))
			return nil
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
		fmt.Sprintf(`
A comma-delimited list of the addresses (<host:port>) of all the redpanda nodes
in a cluster. The port must be the one configured for the nodes' admin API
(%d by default)`,
			config.DefaultConfig().Redpanda.AdminApi.Port,
		))
	command.Flags().StringVar(
		&seedAddr,
		"seed-addr",
		"",
		"The URL of a redpanda node with which to discover the rest")
	command.Flags().StringVar(
		&configFile,
		"config",
		config.DefaultConfig().ConfigFile,
		"The path to the redpanda config file")
	return command
}

func executePrometheusConfig(
	fs afero.Fs, jobName string, nodeAddrs []string, seedAddr, configFile string,
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
	conf, err := config.ReadOrGenerate(fs, configFile)
	if err != nil {
		return []byte(""), err
	}
	hosts, err := discoverHosts(
		conf.Redpanda.KafkaApi.Address,
		conf.Redpanda.KafkaApi.Port,
	)
	if err != nil {
		return []byte(""), err
	}
	return renderConfig(jobName, hosts)
}

func renderConfig(jobName string, targets []string) ([]byte, error) {
	scrapeConfig := ScrapeConfig{
		JobName:       jobName,
		StaticConfigs: []StaticConfig{StaticConfig{Targets: targets}},
	}
	return yaml.Marshal([]ScrapeConfig{scrapeConfig})
}

func discoverHosts(url string, port int) ([]string, error) {
	hosts := []string{}
	addr := fmt.Sprintf("%s:%d", url, port)
	client, err := kafka.InitClient(addr)
	if err != nil {
		return hosts, err
	}
	brokers := client.Brokers()
	for _, b := range brokers {
		host, _, err := splitAddress(b.Addr())
		if err != nil {
			return hosts, err
		}
		hosts = append(hosts, fmt.Sprintf("%s:%d", host, 9644))
	}
	return hosts, nil
}

func splitAddress(address string) (string, int, error) {
	host := ""
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
	host = parts[0]
	var err error
	port, err := strconv.Atoi(parts[1])
	if err != nil {
		return "", 0, err
	}
	return host, port, nil
}
