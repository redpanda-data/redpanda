package cmd

import (
	"fmt"
	"path/filepath"
	"time"
	"vectorized/pkg/checkers"
	"vectorized/pkg/cli"
	"vectorized/pkg/os"
	"vectorized/pkg/redpanda"
	"vectorized/pkg/tuners/factory"
	"vectorized/pkg/tuners/hwloc"
	"vectorized/pkg/utils"

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
		timeoutMs          int
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
			config, err := redpanda.ReadConfigFromPath(fs, configFile)
			if err != nil {
				return err
			}
			installDirectory, err := cli.GetOrFindInstallDir(fs, installDirFlag)
			if err != nil {
				return err
			}
			ioConfigFile := redpanda.GetIOConfigPath(filepath.Dir(configFile))
			if !utils.FileExists(fs, ioConfigFile) {
				ioConfigFile = ""
			}
			rpArgs := &redpanda.RedpandaArgs{
				ConfigFilePath: configFile,
				SeastarFlags: map[string]string{
					"io-properties-file": ioConfigFile,
					"lock-memory":        "false",
				},
			}
			err = prestart(fs, rpArgs, config, prestartCfg, time.Duration(timeoutMs)*time.Millisecond)
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
	// FIXME: Set to false by default as it triggers a 50+ GB core dump on some machines
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
	command.Flags().BoolVar(&sFlags.mbind, "mbind", true, "enable mbind")
	command.Flags().IntVar(&timeoutMs, "timeout", 10000, "The maximum amount of time (in ms) to wait for the checks and tune processes to complete")
	for flag := range sFlagsMap {
		command.Flag(flag).Hidden = true
	}
	return command
}

func prestart(
	fs afero.Fs,
	args *redpanda.RedpandaArgs,
	config *redpanda.Config,
	prestartCfg prestartConfig,
	timeout time.Duration,
) error {
	if prestartCfg.tuneEnabled {
		err := tuneAll(fs, args.SeastarFlags["cpuset"], config, timeout)
		if err != nil {
			return err
		}
		log.Info("System tune - PASSED")
	}
	if prestartCfg.checkEnabled {
		checkersMap, err := redpanda.RedpandaCheckers(fs,
			args.SeastarFlags["io-properties-file"], config, timeout)
		if err != nil {
			return err
		}
		err = check(checkersMap, checkFailedActions(args))
		if err != nil {
			return err
		}
		log.Info("System check - PASSED")
	}
	return nil
}

func tuneAll(
	fs afero.Fs,
	cpuSet string,
	config *redpanda.Config,
	timeout time.Duration,
) error {
	params := &factory.TunerParams{}
	tunerFactory := factory.NewDirectExecutorTunersFactory(fs, timeout)
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

	err := factory.FillTunerParamsWithValuesFromConfig(params, config)
	if err != nil {
		return err
	}

	for _, tunerName := range factory.AvailableTuners() {
		tuner := tunerFactory.CreateTuner(tunerName, params)
		if supported, reason := tuner.CheckIfSupported(); supported {
			log.Debugf("Tuner paramters %+v", params)
			result := tuner.Tune()
			if result.IsFailed() {
				return result.GetError()
			}
		} else {
			log.Debugf("Tuner '%s' is not supported - %s", tunerName, reason)
		}
	}
	return nil
}

type checkFailedAction func(*checkers.CheckResult)

func checkFailedActions(
	args *redpanda.RedpandaArgs,
) map[redpanda.CheckerID]checkFailedAction {
	return map[redpanda.CheckerID]checkFailedAction{
		redpanda.SwapChecker: func(*checkers.CheckResult) {
			// Do not set --lock-memory flag when swap is disabled
			args.SeastarFlags["lock-memory"] = "false"
		},
	}
}

func check(
	checkersMap map[redpanda.CheckerID][]checkers.Checker,
	checkFailedActions map[redpanda.CheckerID]checkFailedAction,
) error {
	for checkerID, checkersSlice := range checkersMap {
		for _, checker := range checkersSlice {
			result := checker.Check()
			if result.Err != nil {
				if checker.GetSeverity() == checkers.Fatal {
					return result.Err
				}
				log.Warnf("System check '%s' failed with non-fatal error '%s'", checker.GetDesc(), result.Err)
			}
			if !result.IsOk {
				if action, exists := checkFailedActions[checkerID]; exists {
					action(result)
				}
				msg := fmt.Sprintf("System check '%s' failed. Required: %v, Current %v",
					checker.GetDesc(), checker.GetRequiredAsString(), result.Current)
				if checker.GetSeverity() == checkers.Fatal {
					return fmt.Errorf(msg)
				}
				log.Warn(msg)
			}
		}
	}
	return nil
}
