package cmd

import (
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
		"Redpanda config file, if not set the file will be searched for in default location",
	)
	return c
}
