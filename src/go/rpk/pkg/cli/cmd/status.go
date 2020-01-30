package cmd

import (
	"fmt"
	"time"
	"vectorized/pkg/api"
	"vectorized/pkg/cli"
	"vectorized/pkg/cli/ui"
	"vectorized/pkg/config"
	"vectorized/pkg/system"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func NewStatusCommand(fs afero.Fs) *cobra.Command {
	var (
		configFile string
		send       bool
		timeout    time.Duration
	)
	command := &cobra.Command{
		Use:          "status",
		Short:        "Check the resource usage in the system, and optionally send it to Vectorized",
		Long:         "",
		SilenceUsage: true,
		RunE: func(ccmd *cobra.Command, args []string) error {
			return executeStatus(fs, configFile, timeout, send)
		},
	}
	command.Flags().StringVar(&configFile,
		"redpanda-cfg", "", "Redpanda config file, if not set the file will be "+
			"searched for in default locations")
	command.Flags().BoolVar(
		&send,
		"send",
		false,
		"Tells `status` whether to send the gathered resource usage data to Vectorized")
	command.Flags().DurationVar(
		&timeout,
		"timeout",
		2000*time.Millisecond,
		"The maximum amount of time to wait for the metrics to be gathered. "+
			"The value passed is a sequence of decimal numbers, each with optional "+
			"fraction and a unit suffix, such as '300ms', '1.5s' or '2h45m'. "+
			"Valid time units are 'ns', 'us' (or 'µs'), 'ms', 's', 'm', 'h'",
	)
	return command
}

func executeStatus(fs afero.Fs, configFile string, timeout time.Duration, send bool) error {
	configFile, err := cli.GetOrFindConfig(fs, configFile)
	if err != nil {
		return err
	}
	conf, err := config.ReadConfigFromPath(fs, configFile)
	if err != nil {
		return err
	}
	if !conf.Rpk.EnableUsageStats {
		log.Info("Usage stats are disabled. To enable them, set rpk.enable_usage_stats to true.")
		return nil
	}
	metrics, errs := system.GatherMetrics(fs, timeout, *conf)
	if len(errs) != 0 {
		for _, err := range errs {
			log.Error(err)
		}
	}

	printMetrics(metrics)

	if send {
		if conf.NodeUuid == "" {
			conf, err = config.GenerateAndWriteNodeUuid(fs, conf)
			if err != nil {
				return err
			}
		}
		payload := api.MetricsPayload{
			uint64(metrics.CpuPercentage),
			metrics.FreeMemory,
			metrics.FreeSpace,
		}
		return api.SendMetrics(payload, *conf)
	}
	return nil
}

func printMetrics(p *system.Metrics) {
	t := ui.NewRpkTable(log.StandardLogger().Out)
	t.SetHeader([]string{"Name", "Value"})
	t.Append([]string{"CPU%", fmt.Sprint(p.CpuPercentage)})
	t.Append([]string{"Free Memory", fmt.Sprint(p.FreeMemory)})
	t.Append([]string{"Free Space", fmt.Sprint(p.FreeSpace)})
	t.Render()
}
