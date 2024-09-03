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
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/factory"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/hwloc"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/irq"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

type result struct {
	name      string
	applied   bool
	enabled   bool
	supported bool
	errMsg    string
}

func NewCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var (
		tunerParams       factory.TunerParams
		outTuneScriptFile string
		cpuSet            string
		timeout           time.Duration
	)
	cmd := &cobra.Command{
		Use:   "tune [list of elements to tune]",
		Short: "Sets the OS parameters to tune system performance",
		Long: fmt.Sprintf(`Sets the OS parameters to tune system performance.

Available tuners:

  - all.
  - %s

To learn more about a tuner, run 'rpk redpanda tune help <tuner name>'.
`, strings.Join(factory.AvailableTuners(), "\n  - ")),
		Args: func(_ *cobra.Command, args []string) error {
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
		Run: func(_ *cobra.Command, args []string) {
			if !tunerParamsEmpty(&tunerParams) && p.ConfigFlag != "" {
				out.Die("use either tuner params or redpanda config file")
			}
			var tuners []string
			if args[0] == "all" {
				tuners = factory.AvailableTuners()
			} else {
				tuners = strings.Split(args[0], ",")
			}
			cpuMask, err := hwloc.TranslateToHwLocCPUSet(cpuSet)
			out.MaybeDieErr(err)

			tunerParams.CPUMask = cpuMask
			y, err := p.LoadVirtualRedpandaYaml(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			var tunerFactory factory.TunersFactory
			if outTuneScriptFile != "" {
				exists, err := afero.Exists(fs, outTuneScriptFile)
				out.MaybeDie(err, "unable to determine if file %q exists: %v", outTuneScriptFile, err)
				if !exists {
					zap.L().Sugar().Debugf("provided output-script file %q does not exists, creating one", outTuneScriptFile)
					_, err = fs.Create(outTuneScriptFile)
					out.MaybeDie(err, "unable to create file %q: %v", outTuneScriptFile, err)
				}
				isDir, err := afero.IsDir(fs, outTuneScriptFile)
				out.MaybeDie(err, "unable to check if %q is a dir or a file: %v", outTuneScriptFile, err)
				if isDir {
					out.Die("please use a filename instead of a directory name in --output-script")
				}
				tunerFactory = factory.NewScriptRenderingTunersFactory(fs, y.Rpk.Tuners, outTuneScriptFile, timeout)
			} else {
				tunerFactory = factory.NewDirectExecutorTunersFactory(fs, y.Rpk.Tuners, timeout)
			}
			exit1, err := tune(y, tuners, tunerFactory, &tunerParams)
			out.MaybeDieErr(err)
			if exit1 {
				os.Exit(1)
			}
		},
	}
	addTunerParamsFlags(cmd, &tunerParams)
	cmd.Flags().StringVar(&cpuSet, "cpu-set", "all", "Set of CPUs for tuners to use in cpuset(7) format; if not specified, tuners will use all available CPUs")
	cmd.Flags().StringVar(&outTuneScriptFile, "output-script", "", "If a filename is provided, it will generate a tuning file that can later be used to tune the system")
	cmd.Flags().DurationVar(&timeout, "timeout", 10*time.Second, "The maximum time to wait for the tune processes to complete (e.g. 300ms, 1.5s, 2h45m)")
	// Deprecated
	cmd.Flags().BoolVar(new(bool), "interactive", false, "Ask for confirmation on every step (e.g. configuration generation)")
	cmd.Flags().MarkDeprecated("interactive", "not needed: tune will use default configuration if config file is not found.")

	cmd.AddCommand(
		newHelpCommand(),
		newListCommand(fs, p),
	)
	return cmd
}

func addTunerParamsFlags(cmd *cobra.Command, tunerParams *factory.TunerParams) {
	cmd.Flags().StringVarP(&tunerParams.Mode, "mode", "m", "", "Operation Mode: one of: [sq, sq_split, mq]")
	cmd.Flags().StringSliceVarP(&tunerParams.Disks, "disks", "d", nil, "Lists of devices to tune f.e. 'sda1'")
	cmd.Flags().StringSliceVarP(&tunerParams.Nics, "nic", "n", nil, "Network Interface Controllers to tune")
	cmd.Flags().StringSliceVarP(&tunerParams.Directories, "dirs", "r", nil, "List of *data* directories or places to store data (e.g. /var/vectorized/redpanda/); usually your XFS filesystem on an NVMe SSD device")
	cmd.Flags().BoolVar(&tunerParams.RebootAllowed, "reboot-allowed", false, "Allow tuners to tune boot parameters and request system reboot")
}

func tune(
	y *config.RedpandaYaml,
	tunerNames []string,
	tunersFactory factory.TunersFactory,
	params *factory.TunerParams,
) (bool, error) {
	params, err := factory.MergeTunerParamsConfig(params, y)
	if err != nil {
		return false, err
	}
	var (
		rebootRequired bool
		includeErr     bool
		exit1          bool
		results        []result
		allDisabled    = true
	)

	for _, tunerName := range tunerNames {
		enabled := factory.IsTunerEnabled(tunerName, y.Rpk.Tuners)
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
		zap.L().Sugar().Debugf("Tuner parameters %+v", params)
		res := tuner.Tune()
		includeErr = includeErr || res.IsFailed()
		rebootRequired = rebootRequired || res.IsRebootRequired()
		errMsg := ""
		if res.IsFailed() {
			errMsg = res.Error().Error()
			exit1 = true
			// A warning is given (instead of error) when the list of MSI IRQs
			// in sysfs is empty due to a known upstream problem introduced in
			// kernel 5.17.
			// See: https://github.com/redpanda-data/redpanda/issues/10838
			if ee := (*irq.EmptyMSIRQError)(nil); errors.As(res.Error(), &ee) {
				exit1 = false
				supported = false
			}
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

	tw := out.NewTable(headers...)
	defer tw.Flush()
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
		tw.PrintStrings(colorRow(c, row)...)
	}
}

func colorRow(c func(...interface{}) string, row []string) []string {
	for i, s := range row {
		row[i] = c(s)
	}
	return row
}
