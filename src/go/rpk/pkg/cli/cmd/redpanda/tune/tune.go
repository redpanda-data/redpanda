// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

//go:build linux

package tune

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/ui"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/factory"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/hwloc"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

type result struct {
	name      string
	applied   bool
	enabled   bool
	supported bool
	errMsg    string
}

func NewCommand(fs afero.Fs) *cobra.Command {
	tunerParams := factory.TunerParams{}
	var (
		configFile        string
		outTuneScriptFile string
		cpuSet            string
		timeout           time.Duration
	)
	baseMsg := "Sets the OS parameters to tune system performance"
	longMsg := fmt.Sprintf(`Sets the OS parameters to tune system performance.

Available tuners:

  - all.
  - %s

To learn more about a tuner, run 'rpk redpanda tune help <tuner name>'.
`, strings.Join(factory.AvailableTuners(), "\n  - "))
	command := &cobra.Command{
		Use:   "tune <list of elements to tune>",
		Short: baseMsg,
		Long:  longMsg,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) < 1 {
				return errors.New("requires the list of elements to tune")
			}
			if len(args) == 1 && args[0] == "all" {
				return nil
			}

			for _, toTune := range strings.Split(args[0], ",") {
				if !factory.IsTunerAvailable(toTune) {
					return fmt.Errorf("invalid element to tune '%s' "+
						"only %s are supported",
						args[0], factory.AvailableTuners())
				}
			}
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			if !tunerParamsEmpty(&tunerParams) && configFile != "" {
				out.Die("use either tuner params or redpanda config file")
			}
			var tuners []string
			p := config.ParamsFromCommand(cmd)
			if args[0] == "all" {
				tuners = factory.AvailableTuners()
			} else {
				tuners = strings.Split(args[0], ",")
			}
			cpuMask, err := hwloc.TranslateToHwLocCPUSet(cpuSet)
			out.MaybeDieErr(err)

			tunerParams.CPUMask = cpuMask
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)
			var tunerFactory factory.TunersFactory
			if outTuneScriptFile != "" {
				tunerFactory = factory.NewScriptRenderingTunersFactory(
					fs, *cfg, outTuneScriptFile, timeout)
			} else {
				tunerFactory = factory.NewDirectExecutorTunersFactory(
					fs, *cfg, timeout)
			}
			exit1, err := tune(cfg, tuners, tunerFactory, &tunerParams)
			out.MaybeDieErr(err)
			if exit1 {
				os.Exit(1)
			}
		},
	}
	addTunerParamsFlags(command, &tunerParams)
	command.Flags().StringVar(&cpuSet,
		"cpu-set",
		"all",
		"Set of CPUs for tuner to use in cpuset(7) format if not specified tuner will use all available CPUs")
	command.Flags().StringVar(
		&configFile,
		config.FlagConfig,
		"",
		"Redpanda config file, if not set the file will be searched for in $PWD or /etc/redpanda/redpanda.yaml.",
	)
	command.Flags().StringVar(&outTuneScriptFile,
		"output-script",
		"",
		"If set tuners will generate tuning file that can later be used to tune the system")
	command.Flags().DurationVar(
		&timeout,
		"timeout",
		10000*time.Millisecond,
		"The maximum time to wait for the tune processes to complete. "+
			"The value passed is a sequence of decimal numbers, each with optional "+
			"fraction and a unit suffix, such as '300ms', '1.5s' or '2h45m'. "+
			"Valid time units are 'ns', 'us' (or 'µs'), 'ms', 's', 'm', 'h'",
	)
	// Deprecated
	command.Flags().BoolVar(
		new(bool),
		"interactive",
		false,
		"Ask for confirmation on every step (e.g. configuration generation)",
	)
	command.Flags().MarkDeprecated("interactive", "not needed: tune will use default configuration if config file is not found.")

	command.AddCommand(newHelpCommand())
	command.AddCommand(newListCommand(fs))
	return command
}

func addTunerParamsFlags(command *cobra.Command, tunerParams *factory.TunerParams) {
	command.Flags().StringVarP(&tunerParams.Mode,
		"mode", "m", "",
		"Operation Mode: one of: [sq, sq_split, mq]")
	command.Flags().StringSliceVarP(&tunerParams.Disks,
		"disks", "d",
		[]string{}, "Lists of devices to tune f.e. 'sda1'")
	command.Flags().StringSliceVarP(&tunerParams.Nics,
		"nic", "n",
		[]string{}, "Network Interface Controllers to tune")
	command.Flags().StringSliceVarP(&tunerParams.Directories,
		"dirs", "r",
		[]string{}, "List of *data* directories or places to store data,"+
			" i.e.: '/var/vectorized/redpanda/',"+
			" usually your XFS filesystem on an NVMe SSD device.")
	command.Flags().BoolVar(&tunerParams.RebootAllowed,
		"reboot-allowed", false, "If set will allow tuners to tune boot parameters"+
			" and request system reboot.")
}

func tune(
	conf *config.Config,
	tunerNames []string,
	tunersFactory factory.TunersFactory,
	params *factory.TunerParams,
) (bool, error) {
	params, err := factory.MergeTunerParamsConfig(params, conf)
	if err != nil {
		return false, err
	}
	var (
		rebootRequired, includeErr, exit1 bool
		results                           []result
		allDisabled                       = true
	)

	for _, tunerName := range tunerNames {
		enabled := factory.IsTunerEnabled(tunerName, conf.Rpk)
		allDisabled = allDisabled && !enabled
		tuner := tunersFactory.CreateTuner(tunerName, params)
		supported, reason := tuner.CheckIfSupported()
		if !enabled || !supported {
			includeErr = includeErr || !supported
			results = append(results, result{tunerName, false, enabled, supported, reason})
			// We exit with code 1 when it's enabled and not supported except
			// for disk_write_cache since it's only supported for GCP.
			// We also allow clocksource to fail, see #6444.
			exit1 = exit1 || enabled && !supported && !(tunerName == "disk_write_cache" || tunerName == "clocksource")
			continue
		}
		log.Debugf("Tuner parameters %+v", params)
		res := tuner.Tune()
		includeErr = includeErr || res.IsFailed()
		rebootRequired = rebootRequired || res.IsRebootRequired()
		errMsg := ""
		if res.IsFailed() {
			errMsg = res.Error().Error()
			exit1 = true
		}
		results = append(results, result{tunerName, !res.IsFailed(), enabled, supported, errMsg})
	}

	if allDisabled {
		fmt.Println("All tuners were disabled, so none were applied. You may run `rpk redpanda mode prod` to enable the recommended set of tuners for non-containerized production use.")
	}

	printTuneResult(results, includeErr)

	if rebootRequired {
		red := color.New(color.FgRed).SprintFunc()
		fmt.Printf(
			"%s: Reboot system and run 'rpk redpanda tune %s' again\n",
			red("IMPORTANT"),
			strings.Join(tunerNames, ","),
		)
	}
	return exit1, nil
}

func tunerParamsEmpty(params *factory.TunerParams) bool {
	return len(params.Directories) == 0 &&
		len(params.Disks) == 0 &&
		len(params.Nics) == 0
}

func printTuneResult(results []result, includeErr bool) {
	sort.Slice(results, func(i, j int) bool {
		return results[i].name < results[j].name
	})
	headers := []string{
		"Tuner",
		"Applied",
		"Enabled",
		"Supported",
	}
	if includeErr {
		headers = append(headers, "Error")
	}

	t := ui.NewRpkTable(os.Stdout)
	t.SetHeader(headers)
	red := color.New(color.FgRed).SprintFunc()
	yellow := color.New(color.FgYellow).SprintFunc()
	green := color.New(color.FgGreen).SprintFunc()
	white := color.New(color.FgHiWhite).SprintFunc()

	for _, res := range results {
		c := white
		row := []string{
			res.name,
			strconv.FormatBool(res.applied),
			strconv.FormatBool(res.enabled),
			strconv.FormatBool(res.supported),
		}
		if includeErr {
			row = append(row, res.errMsg)
		}
		if !res.supported {
			c = yellow
		} else if res.errMsg != "" {
			c = red
		} else if res.applied {
			c = green
		}
		t.Append(colorRow(c, row))
	}
	t.Render()
}

func colorRow(c func(...interface{}) string, row []string) []string {
	for i, s := range row {
		row[i] = c(s)
	}
	return row
}
