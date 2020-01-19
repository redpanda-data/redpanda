package cmd

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"
	"vectorized/pkg/cli"
	"vectorized/pkg/cloud"
	"vectorized/pkg/config"
	"vectorized/pkg/os"
	"vectorized/pkg/redpanda"
	"vectorized/pkg/tuners"
	"vectorized/pkg/tuners/factory"
	"vectorized/pkg/tuners/hwloc"
	"vectorized/pkg/tuners/iotune"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

type prestartConfig struct {
	tuneEnabled  bool
	checkEnabled bool
}

type seastarFlags struct {
	memory           string
	cpuSet           string
	ioPropertiesFile string
	lockMemory       bool
	smp              int
	reserveMemory    string
	hugepages        string
	threadAffinity   bool
	numIoQueues      int
	maxIoRequests    int
	ioProperties     string
	mbind            bool
}

func NewStartCommand(fs afero.Fs) *cobra.Command {
	prestartCfg := prestartConfig{}
	var (
		configFilePathFlag string
		installDirFlag     string
		timeout            time.Duration
		wellKnownIo        string
	)
	sFlags := seastarFlags{}
	sFlagsMap := map[string]interface{}{
		"lock-memory":        sFlags.lockMemory,
		"io-properties-file": sFlags.ioPropertiesFile,
		"cpuset":             sFlags.cpuSet,
		"memory":             sFlags.memory,
		"smp":                sFlags.smp,
		"reserve-memory":     sFlags.reserveMemory,
		"hugepages":          sFlags.hugepages,
		"thread-affinity":    sFlags.threadAffinity,
		"num-io-queues":      sFlags.numIoQueues,
		"max-io-requests":    sFlags.maxIoRequests,
		"io-properties":      sFlags.ioProperties,
		"mbind":              sFlags.mbind,
	}
	command := &cobra.Command{
		Use:   "start",
		Short: "Start redpanda",
		RunE: func(ccmd *cobra.Command, args []string) error {
			configFile, err := cli.GetOrFindConfig(fs, configFilePathFlag)
			if err != nil {
				return err
			}
			conf, err := config.ReadConfigFromPath(fs, configFile)
			if err != nil {
				return err
			}
			installDirectory, err := cli.GetOrFindInstallDir(fs, installDirFlag)
			if err != nil {
				return err
			}
			rpArgs, err := buildRedpandaFlags(
				fs,
				conf,
				sFlags,
				configFile,
				wellKnownIo,
			)
			if err != nil {
				return err
			}
			err = prestart(fs, rpArgs, configFile, conf, prestartCfg, timeout)
			if err != nil {
				return err
			}
			// Override all the defaults when flags are explicitly set
			for flag, val := range sFlagsMap {
				if ccmd.Flags().Changed(flag) {
					rpArgs.SeastarFlags[flag] = fmt.Sprint(val)
				}
			}
			launcher := redpanda.NewLauncher(installDirectory, rpArgs)
			log.Info("Starting redpanda...")
			return launcher.Start()
		},
	}
	command.Flags().StringVar(&configFilePathFlag,
		"redpanda-cfg", "",
		" Redpanda config file, if not set the file will be searched for"+
			"in default locations")
	command.Flags().StringVar(&sFlags.memory,
		"memory", "", "Amount of memory for redpanda to use, "+
			"if not specified redpanda will use all available memory")
	command.Flags().BoolVar(&sFlags.lockMemory,
		"lock-memory", false, "If set, will prevent redpanda from swapping")
	command.Flags().StringVar(&sFlags.cpuSet, "cpuset", "",
		"Set of CPUs for redpanda to use in cpuset(7) format, "+
			"if not specified redpanda will use all available CPUs")
	command.Flags().StringVar(&installDirFlag,
		"install-dir", "",
		"Directory where redpanda has been installed")
	command.Flags().BoolVar(&prestartCfg.tuneEnabled, "tune", false,
		"When present will enable tuning before starting redpanda")
	command.Flags().BoolVar(&prestartCfg.checkEnabled, "check", true,
		"When set to false will disable system checking before starting redpanda")
	command.Flags().IntVar(&sFlags.smp, "smp", 1, "number of threads (default: one per CPU)")
	command.Flags().StringVar(&sFlags.reserveMemory, "reserve-memory", "",
		"memory reserved to OS (if --memory not specified)")
	command.Flags().StringVar(&sFlags.hugepages, "hugepages", "",
		"path to accessible hugetlbfs mount (typically /dev/hugepages/something)")
	command.Flags().BoolVar(&sFlags.threadAffinity, "thread-affinity", true,
		"pin threads to their cpus (disable for overprovisioning)")
	command.Flags().IntVar(&sFlags.numIoQueues, "num-io-queues", 0,
		"Number of IO queues. Each IO unit will be responsible for a fraction "+
			"of the IO requests. Defaults to the number of threads")
	command.Flags().IntVar(&sFlags.maxIoRequests, "max-io-requests", 0,
		"Maximum amount of concurrent requests to be sent to the disk. "+
			"Defaults to 128 times the number of IO queues")
	command.Flags().StringVar(&sFlags.ioPropertiesFile, "io-properties-file", "",
		"path to a YAML file describing the characteristics of the I/O Subsystem")
	command.Flags().StringVar(&sFlags.ioProperties, "io-properties", "",
		"a YAML string describing the characteristics of the I/O Subsystem")
	command.Flags().StringVar(
		&wellKnownIo,
		"well-known-io",
		"",
		"The cloud vendor and VM type, in the format <vendor>:<vm type>:<storage type>")
	command.Flags().BoolVar(&sFlags.mbind, "mbind", true, "enable mbind")
	command.Flags().DurationVar(
		&timeout,
		"timeout",
		10000*time.Millisecond,
		"The maximum time to wait for the checks and tune processes to complete. "+
			"The value passed is a sequence of decimal numbers, each with optional "+
			"fraction and a unit suffix, such as '300ms', '1.5s' or '2h45m'. "+
			"Valid time units are 'ns', 'us' (or 'µs'), 'ms', 's', 'm', 'h'",
	)
	for flag := range sFlagsMap {
		command.Flag(flag).Hidden = true
	}
	return command
}

func prestart(
	fs afero.Fs,
	args *redpanda.RedpandaArgs,
	configFile string,
	conf *config.Config,
	prestartCfg prestartConfig,
	timeout time.Duration,
) error {
	if prestartCfg.checkEnabled {
		err := check(fs, configFile, conf, timeout, checkFailedActions(args))
		if err != nil {
			return err
		}
		log.Info("System check - PASSED")
	}
	if prestartCfg.tuneEnabled {
		err := tuneAll(fs, args.SeastarFlags["cpuset"], conf, timeout)
		if err != nil {
			return err
		}
		log.Info("System tune - PASSED")
	}
	return nil
}

func buildRedpandaFlags(
	fs afero.Fs,
	conf *config.Config,
	sFlags seastarFlags,
	configFile string,
	wellKnownIo string,
) (*redpanda.RedpandaArgs, error) {
	if wellKnownIo != "" && sFlags.ioProperties != "" {
		return nil, errors.New(
			"--well-known-io and --io-properties can't be set at the same time",
		)
	}
	ioPropertiesFile := redpanda.GetIOConfigPath(filepath.Dir(configFile))
	if exists, _ := afero.Exists(fs, ioPropertiesFile); !exists {
		ioPropertiesFile = ""
	}
	lockMemory := conf.Rpk.EnableMemoryLocking || sFlags.lockMemory
	rpArgs := &redpanda.RedpandaArgs{
		ConfigFilePath: configFile,
		SeastarFlags: map[string]string{
			"lock-memory": fmt.Sprintf("%t", lockMemory),
		},
	}
	if ioPropertiesFile != "" {
		rpArgs.SeastarFlags["io-properties-file"] = ioPropertiesFile
		return rpArgs, nil
	}
	ioProps, err := resolveWellKnownIo(conf, wellKnownIo)
	if err == nil {
		yaml, err := iotune.ToYaml(*ioProps)
		if err != nil {
			return nil, err
		}
		rpArgs.SeastarFlags["io-properties"] = fmt.Sprintf("'%s'", yaml)
		return rpArgs, nil
	} else {
		log.Warn(err)
	}
	return rpArgs, nil
}

func resolveWellKnownIo(
	conf *config.Config,
	wellKnownIo string,
) (*iotune.IoProperties, error) {
	var configuredWellKnownIo string
	// The flags take precedence over the config file
	if wellKnownIo != "" {
		configuredWellKnownIo = wellKnownIo
	} else {
		configuredWellKnownIo = conf.Rpk.WellKnownIo
	}
	var ioProps *iotune.IoProperties
	if configuredWellKnownIo != "" {
		wellKnownIoTokens := strings.Split(configuredWellKnownIo, ":")
		if len(wellKnownIoTokens) != 3 {
			err := errors.New(
				"--well-known-io should have the format '<vendor>:<vm type>:<storage type>'",
			)
			return nil, err
		}
		ioProps, err := iotune.DataFor(
			conf.Redpanda.Directory,
			wellKnownIoTokens[0],
			wellKnownIoTokens[1],
			wellKnownIoTokens[2],
		)
		if err != nil {
			// Log the error to let the user know that the data wasn't found
			return nil, err
		}
		return ioProps, nil
	}
	log.Info("Detecting the current cloud vendor and VM")
	vendor, err := cloud.AvailableVendor()
	if err != nil {
		return nil, errors.New("Could not detect the current cloud vendor")
	}
	ioProps, err = iotune.DataForVendor(conf.Redpanda.Directory, vendor)
	if err != nil {
		// Log the error to let the user know that the data wasn't found
		return nil, err
	}
	return ioProps, nil
}

func tuneAll(
	fs afero.Fs,
	cpuSet string,
	conf *config.Config,
	timeout time.Duration,
) error {
	params := &factory.TunerParams{}
	tunerFactory := factory.NewDirectExecutorTunersFactory(fs, *conf, timeout)
	hw := hwloc.NewHwLocCmd(os.NewProc(), timeout)
	if cpuSet == "" {
		cpuMask, err := hw.All()
		if err != nil {
			return err
		}
		params.CpuMask = cpuMask
	} else {
		cpuMask, err := hwloc.TranslateToHwLocCpuSet(cpuSet)
		if err != nil {
			return err
		}
		params.CpuMask = cpuMask
	}

	err := factory.FillTunerParamsWithValuesFromConfig(params, conf)
	if err != nil {
		return err
	}

	for _, tunerName := range factory.AvailableTuners() {
		if !factory.IsTunerEnabled(tunerName, conf.Rpk) {
			log.Infof("Skipping disabled tuner %s", tunerName)
			continue
		}
		tuner := tunerFactory.CreateTuner(tunerName, params)
		if supported, reason := tuner.CheckIfSupported(); supported {
			log.Debugf("Tuner paramters %+v", params)
			result := tuner.Tune()
			if result.IsFailed() {
				return result.Error()
			}
		} else {
			log.Debugf("Tuner '%s' is not supported - %s", tunerName, reason)
		}
	}
	return nil
}

type checkFailedAction func(*tuners.CheckResult)

func checkFailedActions(
	args *redpanda.RedpandaArgs,
) map[tuners.CheckerID]checkFailedAction {
	return map[tuners.CheckerID]checkFailedAction{
		tuners.SwapChecker: func(*tuners.CheckResult) {
			// Do not set --lock-memory flag when swap is disabled
			args.SeastarFlags["lock-memory"] = "false"
		},
	}
}

func check(
	fs afero.Fs,
	configFile string,
	conf *config.Config,
	timeout time.Duration,
	checkFailedActions map[tuners.CheckerID]checkFailedAction,
) error {
	results, err := tuners.Check(fs, configFile, conf, timeout)
	if err != nil {
		return err
	}
	for _, result := range results {
		if !result.IsOk {
			if action, exists := checkFailedActions[result.CheckerId]; exists {
				action(&result)
			}
			msg := fmt.Sprintf("System check '%s' failed. Required: %v, Current %v",
				result.Desc, result.Required, result.Current)
			if result.Severity == tuners.Fatal {
				return fmt.Errorf(msg)
			}
			log.Warn(msg)
		}
	}
	return nil
}
