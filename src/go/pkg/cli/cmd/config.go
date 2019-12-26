package cmd

import (
	"errors"
	"strconv"
	"vectorized/pkg/cli"
	"vectorized/pkg/redpanda"

	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

const configFileFlag = "redpanda-cfg"

func NewConfigCommand(fs afero.Fs) *cobra.Command {
	root := &cobra.Command{
		Use:   "config <command>",
		Short: "Edit configuration",
	}
	root.PersistentFlags().String(
		configFileFlag,
		"",
		"Redpanda config file, if not set the file will be searched for in default locations",
	)
	root.AddCommand(set(fs))

	return root
}

func set(fs afero.Fs) *cobra.Command {
	c := &cobra.Command{
		Use:   "set <field>",
		Short: "Set configuration values, such as the node IDs or the list of seed servers",
	}
	c.AddCommand(id(fs))
	c.AddCommand(seedNodes(fs))
	return c
}

func id(fs afero.Fs) *cobra.Command {
	c := &cobra.Command{
		Use:   "id <id>",
		Short: "Set this node's ID",
		Args: func(_ *cobra.Command, args []string) error {
			if len(args) < 1 {
				return errors.New("requires an id")
			}
			_, err := strconv.Atoi(args[0])
			if err != nil {
				return errors.New("requires an integer value")
			}
			return nil
		},
		RunE: func(c *cobra.Command, args []string) error {
			// Safe to access args[0] directly, and to ignore the error,
			// because it was previously validated.
			id, _ := strconv.Atoi(args[0])
			configFile, config, err := loadConfig(c, fs)
			if err != nil {
				return err
			}
			config.Redpanda.Id = id
			return redpanda.WriteConfig(fs, config, configFile)
		},
	}
	return c
}

func seedNodes(fs afero.Fs) *cobra.Command {
	var (
		ids   []int
		hosts []string
		ports []int
	)
	c := &cobra.Command{
		Use:   "seed-nodes --hosts <h1,h2,...>, [--ids <id1,id2,...>] [--ports <p1,p2,...>]",
		Short: "Set the cluster seed nodes",
		Long: `Set the list of seed nodes. --hosts is mandatory.
If --ports is omitted, the default port will be used. If --ids is omitted, the
corresponding --hosts list index will be used as that node's ID. If passed,
--ids and --ports must have the same number of elements as --hosts. The lists'
elements must be separated by commas.`,
		Args: cobra.OnlyValidArgs,
		RunE: func(c *cobra.Command, args []string) error {
			if len(hosts) == 0 {
				return errors.New("The host list must not be empty")
			}
			if len(ports) == 0 {
				ports = make([]int, len(hosts))
				for i := 0; i < len(hosts); i++ {
					ports[i] = 33145
				}
			} else if len(ports) != len(hosts) {
				return errors.New("There should be one port per host")
			}
			if len(ids) == 0 {
				ids = make([]int, len(hosts))
				for i := 0; i < len(hosts); i++ {
					ids[i] = i
				}
			} else if len(ids) != len(hosts) {
				return errors.New("There should be one id per host")
			}
			configFile, config, err := loadConfig(c, fs)
			if err != nil {
				return err
			}
			seeds := make([]*redpanda.SeedServer, len(hosts))
			for i := 0; i < len(hosts); i++ {
				seed := &redpanda.SeedServer{
					Id: ids[i],
					Host: redpanda.SocketAddress{
						Address: hosts[i],
						Port:    ports[i],
					},
				}
				seeds[i] = seed
			}
			config.Redpanda.SeedServers = seeds
			return redpanda.WriteConfig(fs, config, configFile)
		},
	}
	c.Flags().StringSliceVar(&hosts, "hosts", []string{}, "The list of node addresses or hostnames")
	cobra.MarkFlagRequired(c.Flags(), "hosts")
	c.Flags().IntSliceVar(&ids, "ids", []int{}, "The list of node IDs")
	c.Flags().IntSliceVar(&ports, "ports", []int{}, "The list of node ports")
	return c
}

func loadConfig(c *cobra.Command, fs afero.Fs) (string, *redpanda.Config, error) {
	redpandaConfigFile, err := c.Flags().GetString(configFileFlag)
	if err != nil {
		return "", nil, err
	}
	configFile, err := cli.GetOrFindConfig(fs, redpandaConfigFile)
	if err != nil {
		return "", nil, err
	}
	config, err := redpanda.ReadConfigFromPath(fs, configFile)
	return configFile, config, err
}
