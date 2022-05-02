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
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/fatih/color"
	tunecmd "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cmd/redpanda/tune"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/ui"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
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

func NewTuneCommand(fs afero.Fs, mgr config.Manager) *cobra.Command {
	tunerParams := factory.TunerParams{}
	var (
		configFile        string
		outTuneScriptFile string
		cpuSet            string
		timeout           time.Duration
		interactive       bool
	)
	baseMsg := "Sets the OS parameters to tune system performance." +
		" Available tuners: all, " +
		strings.Join(factory.AvailableTuners(), ", ")
	command := &cobra.Command{
		Use:   "tune <list of elements to tune>",
		Short: baseMsg,
		Long: baseMsg + ".\n In order to get more information about the" +
			" tuners, run `rpk redpanda tune help <tuner name>`",
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
		RunE: func(cmd *cobra.Command, args []string) error {
			if !tunerParamsEmpty(&tunerParams) && configFile != "" {
				return errors.New("Use either tuner params or redpanda config file")
			}
			var tuners []string
			if args[0] == "all" {
				tuners = factory.AvailableTuners()
			} else {
				tuners = strings.Split(args[0], ",")
			}
			cpuMask, err := hwloc.TranslateToHwLocCPUSet(cpuSet)
			if err != nil {
				return err
			}
			tunerParams.CPUMask = cpuMask
			conf, err := mgr.FindOrGenerate(configFile)
			if err != nil {
				if !interactive {
					return err
				}
				msg := fmt.Sprintf(
					`Couldn't read or generate the config at '%s'.
Would you like to continue with the default configuration?`,
					configFile,
				)
				confirmed, cerr := promptConfirmation(msg, cmd.InOrStdin())
				if cerr != nil {
					return cerr
				}
				if !confirmed {
					return nil
				}
				conf = config.Default()
			}
			var tunerFactory factory.TunersFactory
			if outTuneScriptFile != "" {
				tunerFactory = factory.NewScriptRenderingTunersFactory(
					fs, *conf, outTuneScriptFile, timeout)
			} else {
				tunerFactory = factory.NewDirectExecutorTunersFactory(
					fs, *conf, timeout)
			}
			return tune(fs, conf, tuners, tunerFactory, &tunerParams)
		},
	}
	command.Flags().StringVarP(&tunerParams.Mode,
		"mode", "m", "",
		"Operation Mode: one of: [sq, sq_split, mq]")
	command.Flags().StringVar(&cpuSet,
		"cpu-set",
		"all", "Set of CPUs for tuner to use in cpuset(7) format "+
			"if not specified tuner will use all available CPUs")
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
	command.Flags().StringVar(
		&configFile,
		"config",
		"",
		"Redpanda config file, if not set the file will be searched for"+
			" in the default locations.",
	)
	command.Flags().StringVar(&outTuneScriptFile,
		"output-script", "", "If set tuners will generate tuning file that "+
			"can later be used to tune the system")
	command.Flags().DurationVar(
		&timeout,
		"timeout",
		10000*time.Millisecond,
		"The maximum time to wait for the tune processes to complete. "+
			"The value passed is a sequence of decimal numbers, each with optional "+
			"fraction and a unit suffix, such as '300ms', '1.5s' or '2h45m'. "+
			"Valid time units are 'ns', 'us' (or 'µs'), 'ms', 's', 'm', 'h'",
	)
	command.Flags().BoolVar(
		&interactive,
		"interactive",
		false,
		"Ask for confirmation on every step (e.g. tuner execution,"+
			" configuration generation)",
	)
	command.AddCommand(tunecmd.NewHelpCommand())
	return command
}

func promptConfirmation(msg string, in io.Reader) (bool, error) {
	scanner := bufio.NewScanner(in)
	for {
		log.Info(fmt.Sprintf("%s (y/n/q)", msg))
		scanner.Scan()
		log.Info(scanner.Text())
		if scanner.Err() != nil {
			return false, scanner.Err()
		}
		text := strings.ToLower(scanner.Text())
		if len(text) == 0 {
			log.Infof("Please choose an option")
			continue
		}
		opt := text[0]
		switch opt {
		case 'y':
			return true, nil
		case 'n':
			return false, nil
		case 'q':
			return false, errors.New("user exited")
		default:
			log.Infof("Unrecognized option '%s'", text)
			continue
		}

	}
}

func tune(
	fs afero.Fs,
	conf *config.Config,
	tunerNames []string,
	tunersFactory factory.TunersFactory,
	params *factory.TunerParams,
) error {
	params, err := factory.MergeTunerParamsConfig(params, conf)
	if err != nil {
		return err
	}
	rebootRequired := false

	results := []result{}
	includeErr := false
	allDisabled := true
	for _, tunerName := range tunerNames {
		enabled := factory.IsTunerEnabled(tunerName, conf.Rpk)
		allDisabled = allDisabled && !enabled
		tuner := tunersFactory.CreateTuner(tunerName, params)
		supported, reason := tuner.CheckIfSupported()
		if !enabled || !supported {
			includeErr = includeErr || !supported
			results = append(results, result{tunerName, false, enabled, supported, reason})
			continue
		}
		log.Debugf("Tuner parameters %+v", params)
		res := tuner.Tune()
		includeErr = includeErr || res.IsFailed()
		rebootRequired = rebootRequired || res.IsRebootRequired()
		errMsg := ""
		if res.IsFailed() {
			errMsg = res.Error().Error()
		}
		results = append(results, result{tunerName, !res.IsFailed(), enabled, supported, errMsg})
	}

	if allDisabled {
		log.Warn(
			"All tuners were disabled, so none were applied. You may run " +
				" `rpk redpanda mode prod` to enable the recommended set of tuners " +
				" for non-containerized production use.",
		)
	}

	printTuneResult(results, includeErr)

	if rebootRequired {
		red := color.New(color.FgRed).SprintFunc()
		log.Infof(
			"%s: Reboot system and run 'rpk tune %s' again",
			red("IMPORTANT"),
			strings.Join(tunerNames, ","),
		)
	}
	return nil
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
