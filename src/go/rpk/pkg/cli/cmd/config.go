package cmd

import (
	"errors"
	"fmt"
	"net"
	"vectorized/pkg/config"

	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

const configFileFlag = "config"

func NewConfigCommand(fs afero.Fs) *cobra.Command {
	root := &cobra.Command{
		Use:   "config <command>",
		Short: "Edit configuration",
	}
	root.AddCommand(set(fs))
	root.AddCommand(bootstrap(fs))

	return root
}

func set(fs afero.Fs) *cobra.Command {
	var (
		format     string
		configPath string
	)
	c := &cobra.Command{
		Use:   "set <key> <value>",
		Short: "Set configuration values, such as the node IDs or the list of seed servers",
		Args:  cobra.ExactArgs(2),
		RunE: func(_ *cobra.Command, args []string) error {
			key := args[0]
			value := args[1]
			return config.Set(fs, key, value, format, configPath)
		},
	}
	c.Flags().StringVar(&format,
		"format",
		"single",
		"The value format. Can be 'single', for single values such as"+
			" '/etc/redpanda' or 100; and 'json', 'toml', 'yaml',"+
			"'yml', 'properties', 'props', 'prop', or 'hcl'"+
			" when partially or completely setting config objects",
	)
	c.Flags().StringVar(
		&configPath,
		configFileFlag,
		config.DefaultConfig().ConfigFile,
		"Redpanda config file, if not set the file will be searched"+
			" for in the default location",
	)
	return c
}

func bootstrap(fs afero.Fs) *cobra.Command {
	var (
		ips        []string
		self       string
		id         int
		configPath string
	)
	c := &cobra.Command{
		Use:   "bootstrap --ips <ip1,ip2,...> --id <id> [--self <ip>]",
		Short: "Initialize the configuration to bootstrap a cluster",
		Long: `Initialize the configuration to bootstrap a cluster.
--ips is mandatory. Its elements must be separated by a comma, no spaces.`,
		Args: cobra.OnlyValidArgs,
		RunE: func(c *cobra.Command, args []string) error {
			defaultRpcPort := config.DefaultConfig().Redpanda.RPCServer.Port
			if len(ips) == 0 && self == "" {
				return errors.New(
					"either --ips or --self must be passed.",
				)
			}
			conf, err := config.ReadOrGenerate(fs, configPath)
			if err != nil {
				return err
			}
			ips, err := parseIPs(ips)
			if err != nil {
				return err
			}
			var ownIp net.IP
			if self != "" {
				ownIp = net.ParseIP(self)
				if ownIp == nil {
					return fmt.Errorf("%s is not a valid IP.", self)
				}
			} else {
				ownIp, err = ownIP()
				if err != nil {
					return err
				}
			}
			conf.Redpanda.Id = id
			conf.Redpanda.RPCServer.Address = ownIp.String()
			conf.Redpanda.KafkaApi.Address = ownIp.String()
			conf.Redpanda.AdminApi.Address = ownIp.String()
			conf.Redpanda.SeedServers = []*config.SeedServer{}
			seeds := []*config.SeedServer{}
			for i, ip := range ips {
				seed := &config.SeedServer{
					Id: i,
					Host: config.SocketAddress{
						ip.String(),
						defaultRpcPort,
					},
				}
				seeds = append(seeds, seed)
			}
			conf.Redpanda.SeedServers = seeds
			return config.WriteConfig(fs, conf, configPath)
		},
	}
	c.Flags().StringSliceVar(&ips, "ips", []string{}, "The list of node addresses or hostnames")
	c.Flags().StringVar(
		&configPath,
		configFileFlag,
		config.DefaultConfig().ConfigFile,
		"Redpanda config file, if not set the file will be searched"+
			" for in the default location",
	)
	c.Flags().StringVar(
		&self,
		"self",
		"",
		"Hint at this node's IP address from within the list passed in --ips",
	)
	c.Flags().IntVar(
		&id,
		"id",
		-1,
		"This node's ID. If omitted, the underlying integer"+
			" representation of the node's IP will be used",
	)
	cobra.MarkFlagRequired(c.Flags(), "id")
	return c
}

func parseIPs(ips []string) ([]net.IP, error) {
	parsed := []net.IP{}
	for _, i := range ips {
		p := net.ParseIP(i)
		if p == nil {
			return []net.IP{}, fmt.Errorf("%s is not a valid IP.", i)
		}
		parsed = append(parsed, p)
	}
	return parsed, nil
}

func ownIP() (net.IP, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil, err
	}

	filtered := []net.IP{}
	for _, a := range addrs {
		if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			filtered = append(filtered, ipnet.IP)
		}
	}
	if len(filtered) > 1 {
		return nil, errors.New(
			"found multiple non-loopback IPs for the current node." +
				" Try setting --self.",
		)
	}
	if len(filtered) == 1 {
		return nil, errors.New(
			"couldn't find any non-loopback IPs for the current node.",
		)
	}
	return filtered[0], nil
}
