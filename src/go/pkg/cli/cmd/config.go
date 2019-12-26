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
