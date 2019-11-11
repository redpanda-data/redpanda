package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"
	"vectorized/pkg/checkers"
	"vectorized/pkg/cli"
	"vectorized/pkg/cli/ui"
	"vectorized/pkg/redpanda"

	"github.com/fatih/color"
	"github.com/olekukonko/tablewriter"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func NewCheckCommand(fs afero.Fs) *cobra.Command {
	var (
		redpandaConfigFile string
		timeout            time.Duration
	)
	command := &cobra.Command{
		Use:          "check",
		Short:        "Check if system meets redpanda requirements",
		Long:         "",
		SilenceUsage: true,
		RunE: func(ccmd *cobra.Command, args []string) error {
			return executeCheck(fs, redpandaConfigFile, timeout)
		},
	}
	command.Flags().StringVar(&redpandaConfigFile,
		"redpanda-cfg", "", "Redpanda config file, if not set the file will be "+
			"searched for in default locations")
	command.Flags().DurationVar(
		&timeout,
		"timeout",
		2000*time.Millisecond,
		"The maximum amount of time to wait for the checks and tune processes to complete. "+
			"The value passed is a sequence of decimal numbers, each with optional "+
			"fraction and a unit suffix, such as '300ms', '1.5s' or '2h45m'. "+
			"Valid time units are 'ns', 'us' (or 'µs'), 'ms', 's', 'm', 'h'",
	)
	return command
}

type row struct {
	desc     string
	required string
	current  string
	severity string
	result   string
}

func (r row) appendToTable(t *tablewriter.Table) {
	t.Append([]string{
		r.desc,
		r.required,
		r.current,
		r.severity,
		r.result,
	})
}

func executeCheck(
	fs afero.Fs, configFileFlag string, timeout time.Duration,
) error {
	configFile, err := cli.GetOrFindConfig(fs, configFileFlag)
	if err != nil {
		return err
	}
	config, err := redpanda.ReadConfigFromPath(fs, configFile)
	if err != nil {
		return err
	}
	ioConfigFile := redpanda.GetIOConfigPath(filepath.Dir(configFile))
	checkersMap, err := redpanda.RedpandaCheckers(fs, ioConfigFile, config, timeout)
	if err != nil {
		return err
	}
	table := ui.NewRpkTable(os.Stdout)
	table.SetHeader([]string{
		"Condition",
		"Required",
		"Current",
		"Severity",
		"Passed",
	})

	var rows []row
	for _, checkersSlice := range checkersMap {
		for _, c := range checkersSlice {
			result := c.Check()
			if result.Err != nil {
				if c.GetSeverity() == checkers.Fatal {
					return result.Err
				}
				log.Warnf("System check '%s' failed with non-fatal error '%s'", c.GetDesc(), result.Err)
			}
			log.Debugf("Checker '%s' result %+v", c.GetDesc(), result)
			rows = append(rows, row{
				desc:     c.GetDesc(),
				required: fmt.Sprint(c.GetRequiredAsString()),
				current:  result.Current,
				severity: fmt.Sprint(c.GetSeverity()),
				result:   fmt.Sprint(printResult(c.GetSeverity(), result.IsOk)),
			})

		}
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].desc < rows[j].desc })
	for _, row := range rows {
		row.appendToTable(table)
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
