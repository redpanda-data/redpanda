package cmd

import (
	"fmt"
	"os"
	"vectorized/checkers"
	"vectorized/cli"
	"vectorized/redpanda"

	"github.com/olekukonko/tablewriter"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func NewCheckCommand(fs afero.Fs) *cobra.Command {
	var redpandaConfigFile string
	command := &cobra.Command{
		Use:          "check",
		Short:        "Checks the system for running redpanda",
		Long:         "",
		SilenceUsage: true,
		RunE: func(ccmd *cobra.Command, args []string) error {
			return executeCheck(fs, redpandaConfigFile)
		},
	}
	command.Flags().StringVar(&redpandaConfigFile,
		"redpanda-cfg", "", "Redpanda config file, if not set rpk will try "+
			"to find the config file in default locations")
	return command
}

func executeCheck(fs afero.Fs, configFileFlag string) error {
	configFile, err := cli.GetOrFindConfig(fs, configFileFlag)
	if err != nil {
		return err
	}
	config, err := redpanda.ReadConfigFromPath(fs, configFile)
	if err != nil {
		return err
	}
	checkers := []checkers.Checker{
		checkers.NewConfigChecker(config),
		checkers.NewMemoryChecker(),
		checkers.NewDataDirWritableChecker(fs, config.Directory),
		checkers.NewFreeDiskSpaceChecker(config.Directory),
		checkers.NewFilesystemTypeChecker(fs, config.Directory),
	}
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{
		"Condition",
		"Required",
		"Current",
		"Passed",
		"Critical",
	})
	table.SetAlignment(tablewriter.ALIGN_LEFT)
	var isOk = true
	for _, c := range checkers {
		result := c.Check()
		if result.Err != nil {
			return result.Err
		}
		log.Debugf("Checker '%s' result %+v", c.GetDesc(), result)
		isOk = isOk && result.IsOk
		table.Append([]string{
			c.GetDesc(),
			fmt.Sprint(c.GetRequiredAsString()),
			result.Current,
			fmt.Sprint(result.IsOk),
			fmt.Sprint(c.IsCritical()),
		})
	}
	table.Render()

	return nil
}
