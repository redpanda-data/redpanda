package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"vectorized/checkers"
	"vectorized/cli"
	"vectorized/cli/ui"
	"vectorized/redpanda"

	"github.com/fatih/color"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func NewCheckCommand(fs afero.Fs) *cobra.Command {
	var redpandaConfigFile string
	command := &cobra.Command{
		Use:          "check",
		Short:        "Check if system meets redpanda requirements",
		Long:         "",
		SilenceUsage: true,
		RunE: func(ccmd *cobra.Command, args []string) error {
			return executeCheck(fs, redpandaConfigFile)
		},
	}
	command.Flags().StringVar(&redpandaConfigFile,
		"redpanda-cfg", "", "Redpanda config file, if not set the file will be "+
			"searched for in default locations")
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
	ioConfigFile := redpanda.GetIOConfigPath(filepath.Dir(configFile))
	checkersMap := checkers.RedpandaCheckers(fs, ioConfigFile, config)
	table := ui.NewRpkTable(os.Stdout)
	table.SetHeader([]string{
		"Condition",
		"Required",
		"Current",
		"Severity",
		"Passed",
	})
	var isOk = true
	for _, c := range checkersMap {
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
			fmt.Sprint(c.GetSeverity()),
			fmt.Sprint(printResult(c.GetSeverity(), result.IsOk)),
		})
	}
	fmt.Println()
	fmt.Println("System check results")
	fmt.Println()
	table.Render()

	return nil
}

func printResult(sev checkers.Severity, isOk bool) string {
	if isOk {
		return color.GreenString("%v", isOk)
	}
	switch sev {
	case checkers.Fatal:
		return color.RedString("%v", isOk)
	case checkers.Warning:
		return color.YellowString("%v", isOk)
	}

	return fmt.Sprint(isOk)
}
