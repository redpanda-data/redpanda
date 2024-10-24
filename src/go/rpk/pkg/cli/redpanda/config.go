// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

//go:build linux
// +build linux

package redpanda

import (
	"errors"
	"fmt"
	"net"
	"strings"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cobraext"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	vnet "github.com/redpanda-data/redpanda/src/go/rpk/pkg/net"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func NewConfigCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config",
		Short: "Edit configuration",
	}
	cmd.AddCommand(
		set(fs, p),
		bootstrap(fs, p),
		cobraext.DeprecatedCmd("init", 0),
	)
	return cmd
}

func set(fs afero.Fs, p *config.Params) *cobra.Command {
	c := &cobra.Command{
		Use:   "set [KEY] [VALUE]",
		Short: "Set configuration values, such as the redpanda node ID or the list of seed servers",
		Long: `Set configuration values, such as the redpanda node ID or the list of seed servers

This command modifies the redpanda.yaml you have locally on disk. The first
argument is the key within the yaml representing a property / field that you
would like to set. Nested fields can be accessed through a dot:

  rpk redpanda config set redpanda.developer_mode true

All values are parsed as yaml and, since yaml is a superset of json, you can
also format your input as json. Individual specific fields or full structs can
be set:

  rpk redpanda config set rpk.tune_disk_irq true
  rpk redpanda config set redpanda.rpc_server '{address: 3.250.158.1, port: 9092}'

You can set an entire array by wrapping all items with braces, or by using one
struct:

  rpk redpanda config set redpanda.advertised_kafka_api '[{address: 0.0.0.0, port: 9092}]'
  rpk redpanda config set redpanda.advertised_kafka_api '{address: 0.0.0.0, port: 9092}' # same

Indexing can be used to set specific items in an array. You can index one past
the end of an array to extend it:

  rpk redpanda config set redpanda.advertised_kafka_api[1] '{address: 0.0.0.0, port: 9092}'

You may also use <key>=<value> notation for setting configuration properties:

  rpk redpanda config set redpanda.kafka_api[0].port=9092
`,
		Args: cobra.MinimumNArgs(1),
		Run: func(_ *cobra.Command, args []string) {
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)

			var key, value string
			if len(args) == 1 && strings.Contains(args[0], "=") {
				kv := strings.SplitN(args[0], "=", 2)
				key, value = kv[0], kv[1]
			} else if len(args) == 2 {
				key, value = args[0], args[1]
			} else {
				out.Die("invalid arguments: %v, please use one of 'rpk redpanda config set <key> <value>' or 'rpk redpanda config set <key>=<value>'", args)
			}
			y := cfg.ActualRedpandaYamlOrDefaults() // we set fields in the raw file without writing env / flag overrides
			err = config.Set(y, key, value)
			out.MaybeDie(err, "unable to set %q: %v", key, err)
			err = y.Write(fs)
			out.MaybeDieErr(err)
		},
	}
	c.Flags().StringVar(new(string), "format", "yaml", "")
	c.Flags().MarkHidden("format")
	return c
}

func bootstrap(fs afero.Fs, p *config.Params) *cobra.Command {
	var (
		advKafkaStr string
		advRPCStr   string
		id          int
		ips         []string
		self        string
	)
	c := &cobra.Command{
		Use:   "bootstrap [--self <ip>] [--ips <ip1,ip2,...>]",
		Short: "Initialize the configuration to bootstrap a cluster",
		Long: `Initialize the configuration to bootstrap a cluster.

This command generates a redpanda.yaml configuration file to bootstrap a
cluster. If you are modifying the configuration file further, it is recommended
to first bootstrap and then modify. If the file already exists, this command
will set fields as requested by flags, and this may undo some of your earlier
edits.

The --ips flag specifies seed servers (ips, ip:ports, or hostnames) that this
broker will use to form a cluster.

The flags --advertised-kafka and --advertised-rpc specify the advertised
addresses that this broker will use to advertise the corresponding listeners. 
If not set, it will default to the private IP address or the one specified with
the --self flag.

By default, redpanda expects your machine to have one private IP address, and
redpanda will listen on it. If your machine has multiple private IP addresses,
you must use the --self flag to specify which ip redpanda should listen on.
`,
		Args: cobra.ExactArgs(0),
		Run: func(_ *cobra.Command, _ []string) {
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			y := cfg.ActualRedpandaYamlOrDefaults() // we modify fields in the raw file without writing env / flag overrides

			seeds, err := parseSeedIPs(ips)
			out.MaybeDieErr(err)

			selfIP, err := parseSelfIP(self)
			out.MaybeDieErr(err)

			if id >= 0 {
				y.Redpanda.ID = &id
			}
			advertisedKafka, err := parseAdvertisedKafka(advKafkaStr, selfIP)
			out.MaybeDie(err, "unable to parse --advertised-kafka %v: %v", advKafkaStr, err)

			advertisedRPC, err := parseAdvertisedRPC(advRPCStr, selfIP)
			out.MaybeDie(err, "unable to parse --advertised-rpc %v: %v", advRPCStr, err)

			// Defaults returns one RPC, one KafkaAPI, and one
			// AdminAPI. We only override values in a configuration
			// file if the current values are defaults, or if an
			// address array is empty. If an address array has
			// multiple elements, we trust that user modifications
			// were intentional.
			//
			// For addresses, we expect this to be fine: the
			// defaults are 0.0.0.0, which we do not expect people
			// to deliberately try to use. We change the address
			// even if the user has set a custom port--being
			// explicit is better than 0.0.0.0.
			defaults := config.DevDefault()

			// Sanity check: we only want to change defaults, and
			// we rely on exactly one element in our defaults. We
			// panic here to catch any changes in tests.
			if len(defaults.Redpanda.KafkaAPI) != 1 || len(defaults.Redpanda.AdminAPI) != 1 {
				panic("defaults now have more than one kafka / admin api address, bug!")
			}

			if a := &y.Redpanda.RPCServer.Address; *a == config.DefaultListenAddress {
				*a = selfIP
			}
			if a := y.Redpanda.AdvertisedRPCAPI; a != nil {
				// Redpanda default configuration use localhost as the default
				// advertised address.
				if a.Address == config.LoopbackIP && a.Port == config.DefaultRPCPort {
					y.Redpanda.AdvertisedRPCAPI = advertisedRPC
				}
			} else {
				y.Redpanda.AdvertisedRPCAPI = advertisedRPC
			}
			if a := &y.Redpanda.KafkaAPI; len(*a) == 1 {
				if first := &((*a)[0].Address); *first == config.DefaultListenAddress {
					*first = selfIP
				}
			} else if len(*a) == 0 {
				*a = []config.NamedAuthNSocketAddress{{
					Address: selfIP,
					Port:    config.DefaultKafkaPort,
				}}
			}
			if a := &y.Redpanda.AdvertisedKafkaAPI; len(*a) == 1 {
				if first := &((*a)[0]); first.Address == config.LoopbackIP && first.Port == config.DefaultKafkaPort {
					*first = advertisedKafka
				}
			} else if len(*a) == 0 {
				*a = []config.NamedSocketAddress{advertisedKafka}
			}
			if a := &y.Redpanda.AdminAPI; len(*a) == 1 {
				if first := &((*a)[0]).Address; *first == config.DefaultListenAddress {
					*first = selfIP
				}
			} else if len(*a) == 0 {
				*a = []config.NamedSocketAddress{{
					Address: selfIP,
					Port:    config.DefaultAdminPort,
				}}
			}
			y.Redpanda.SeedServers = seeds

			err = y.Write(fs)
			out.MaybeDie(err, "error writing config file: %v", err)
		},
	}
	c.Flags().StringSliceVar(&ips, "ips", nil, "Comma-separated list of the seed node addresses or hostnames; at least three are recommended")
	c.Flags().StringVar(&self, "self", "", "Optional IP address for Redpanda to listen on; if empty, defaults to a private address")
	c.Flags().StringVar(&advKafkaStr, "advertised-kafka", "", "Optional address:port for Redpanda to advertise the kafka listener; if empty, defaults to '--self'")
	c.Flags().StringVar(&advRPCStr, "advertised-rpc", "", "Optional address:port for Redpanda to advertise the rpc listener; if empty, defaults to '--self'")
	c.Flags().IntVar(&id, "id", -1, "This node's ID. If unset, redpanda will assign one automatically")
	c.Flags().MarkHidden("id")
	return c
}

func parseSelfIP(self string) (string, error) {
	if self != "" {
		selfIP := net.ParseIP(self)
		if selfIP == nil {
			return "", fmt.Errorf("%s is not a valid IP", self)
		}
		return selfIP.String(), nil
	}
	selfIP, err := getSelfIP()
	if err != nil {
		return "", err
	}
	return selfIP.String(), nil
}

func parseSeedIPs(ips []string) ([]config.SeedServer, error) {
	var seeds []config.SeedServer
	for _, i := range ips {
		_, hostport, err := vnet.ParseHostMaybeScheme(i)
		if err != nil {
			return nil, err
		}

		host, port := vnet.SplitHostPortDefault(hostport, config.DefaultRPCPort)
		seeds = append(seeds, config.SeedServer{Host: config.SocketAddress{
			Address: host,
			Port:    port,
		}})
	}
	return seeds, nil
}

func getSelfIP() (net.IP, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil, err
	}
	var v4private []net.IP
	for _, a := range addrs {
		ipnet, ok := a.(*net.IPNet)
		if !ok {
			continue
		}
		if ip := ipnet.IP; ip.IsPrivate() && ip.To4() != nil {
			v4private = append(v4private, ipnet.IP)
		}
	}
	switch len(v4private) {
	case 0:
		return nil, errors.New("unable to find private v4 IP for current node")
	case 1:
		return v4private[0], nil
	default:
		return nil, errors.New("multiple private v4 IPs found, please select one with --self")
	}
}

func parseAdvertisedKafka(adv string, defHost string) (config.NamedSocketAddress, error) {
	host, port := defHost, config.DefaultKafkaPort
	if adv != "" {
		_, hostport, err := vnet.ParseHostMaybeScheme(adv)
		if err != nil {
			return config.NamedSocketAddress{}, err
		}
		host, port = vnet.SplitHostPortDefault(hostport, config.DefaultKafkaPort)
	}
	if host == "0.0.0.0" {
		return config.NamedSocketAddress{}, errors.New("unexpected kafka advertised address '0.0.0.0' this will cause the broker to fail during startup validation")
	}
	return config.NamedSocketAddress{
		Address: host,
		Port:    port,
	}, nil
}

func parseAdvertisedRPC(adv string, defHost string) (*config.SocketAddress, error) {
	host, port := defHost, config.DefaultRPCPort
	if adv != "" {
		_, hostport, err := vnet.ParseHostMaybeScheme(adv)
		if err != nil {
			return nil, err
		}
		host, port = vnet.SplitHostPortDefault(hostport, config.DefaultRPCPort)
	}
	if host == "0.0.0.0" {
		return nil, errors.New("unexpected rpc advertised address '0.0.0.0' this will cause the broker to fail during startup validation")
	}
	return &config.SocketAddress{
		Address: host,
		Port:    port,
	}, nil
}
