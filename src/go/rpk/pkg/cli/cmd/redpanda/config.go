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

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

const configFileFlag = "config"

func NewConfigCommand(fs afero.Fs, mgr config.Manager) *cobra.Command {
	root := &cobra.Command{
		Use:   "config <command>",
		Short: "Edit configuration.",
	}
	root.AddCommand(set(fs, mgr))
	root.AddCommand(bootstrap(mgr))
	root.AddCommand(initNode(mgr))

	return root
}

func set(fs afero.Fs, mgr config.Manager) *cobra.Command {
	var (
		format     string
		configPath string
	)
	c := &cobra.Command{
		Use:   "set <key> <value>",
		Short: "Set configuration values, such as the node IDs or the list of seed servers.",
		Args:  cobra.ExactArgs(2),
		RunE: func(_ *cobra.Command, args []string) error {
			var err error
			key := args[0]
			value := args[1]
			if configPath == "" {
				configPath, err = config.FindConfigFile(fs)
				if err != nil {
					return err
				}
			}
			_, err = mgr.Read(configPath)
			if err != nil {
				return err
			}
			err = mgr.Set(key, value, format)
			if err != nil {
				return err
			}
			return mgr.WriteLoaded()
		},
	}
	c.Flags().StringVar(&format,
		"format",
		"single",
		"The value format. Can be 'single', for single values such as"+
			" '/etc/redpanda' or 100; and 'json' and 'yaml' when"+
			" partially or completely setting config objects",
	)
	c.Flags().StringVar(
		&configPath,
		configFileFlag,
		"",
		"Redpanda config file, if not set the file will be searched"+
			" for in the default location.",
	)
	return c
}

func bootstrap(mgr config.Manager) *cobra.Command {
	var (
		ips        []string
		self       string
		id         int
		configPath string
	)
	c := &cobra.Command{
		Use:   "bootstrap --id <id> [--self <ip>] [--ips <ip1,ip2,...>]",
		Short: "Initialize the configuration to bootstrap a cluster.",
		Long: "Initialize the configuration to bootstrap a cluster." +
			" --id is mandatory. bootstrap will expect the machine" +
			" it's running on to have only one private non-" +
			"loopback IP address associated to it, and use it in the" +
			" configuration as the node's address. If it has multiple" +
			" IPs, --self must be specified. In that case, the given" +
			" IP will be used without checking whether it's among the" +
			" machine's addresses or not. The elements in --ips must" +
			" be separated by a comma, no spaces. If omitted, the" +
			" node will be configured as a root node, that other" +
			" ones can join later.",
		Args: cobra.OnlyValidArgs,
		RunE: func(c *cobra.Command, args []string) error {
			defaultRpcPort := config.Default().Redpanda.RPCServer.Port
			conf, err := mgr.FindOrGenerate(configPath)
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
			conf.Redpanda.KafkaApi = []config.NamedSocketAddress{{
				SocketAddress: config.SocketAddress{
					Address: ownIp.String(),
					Port:    config.DefaultKafkaPort,
				},
			}}

			conf.Redpanda.AdminApi = []config.NamedSocketAddress{{
				SocketAddress: config.SocketAddress{
					Address: ownIp.String(),
					Port:    config.DefaultAdminPort,
				},
			}}
			conf.Redpanda.SeedServers = []config.SeedServer{}
			seeds := []config.SeedServer{}
			for _, ip := range ips {
				seed := config.SeedServer{
					Host: config.SocketAddress{
						ip.String(),
						defaultRpcPort,
					},
				}
				seeds = append(seeds, seed)
			}
			conf.Redpanda.SeedServers = seeds
			return mgr.Write(conf)
		},
	}
	c.Flags().StringSliceVar(
		&ips,
		"ips",
		[]string{},
		"The list of known node addresses or hostnames",
	)
	c.Flags().StringVar(
		&configPath,
		configFileFlag,
		"",
		"Redpanda config file, if not set the file will be searched"+
			" for in the default location.",
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
		"This node's ID (required).",
	)
	cobra.MarkFlagRequired(c.Flags(), "id")
	return c
}

func initNode(mgr config.Manager) *cobra.Command {
	var (
		configPath string
	)
	c := &cobra.Command{
		Use:   "init",
		Short: "Init the node after install, by setting the node's UUID.",
		Args:  cobra.OnlyValidArgs,
		RunE: func(_ *cobra.Command, args []string) error {
			conf, err := mgr.FindOrGenerate(configPath)
			if err != nil {
				return err
			}
			// Don't reset the node's UUID if it has already been set.
			if conf.NodeUuid == "" {
				return mgr.WriteNodeUUID(conf)
			}
			return nil
		},
	}
	c.Flags().StringVar(
		&configPath,
		configFileFlag,
		"",
		"Redpanda config file, if not set the file will be searched"+
			" for in the default location.",
	)
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
		ipnet, ok := a.(*net.IPNet)
		if !ok {
			continue
		}
		isV4 := ipnet.IP.To4() != nil
		private, err := isPrivate(ipnet.IP)
		if err != nil {
			return nil, err
		}

		if isV4 && private && !ipnet.IP.IsLoopback() {
			filtered = append(filtered, ipnet.IP)
		}
	}
	if len(filtered) > 1 {
		return nil, errors.New(
			"found multiple private non-loopback v4 IPs for the" +
				" current node. Please set one with --self.",
		)
	}
	if len(filtered) == 0 {
		return nil, errors.New(
			"couldn't find any non-loopback IPs for the current node.",
		)
	}
	return filtered[0], nil
}

func isPrivate(ip net.IP) (bool, error) {
	// The standard private subnet CIDRS
	var privateCIDRs = []string{
		"10.0.0.0/8",
		"172.16.0.0/12",
		"192.168.0.0/16",
	}
	for _, cidr := range privateCIDRs {
		_, ipNet, err := net.ParseCIDR(cidr)
		if err != nil {
			return false, err
		}
		if ipNet.Contains(ip) {
			return true, nil
		}
	}
	return false, nil
}
