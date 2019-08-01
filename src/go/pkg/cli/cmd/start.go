package cmd

import (
	"fmt"
	"path/filepath"
	"vectorized/checkers"
	"vectorized/cli"
	"vectorized/os"
	"vectorized/redpanda"
	"vectorized/tuners/factory"
	"vectorized/tuners/hwloc"
	"vectorized/utils"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

type prestartConfig struct {
	tuneEnabled  bool
	checkEnabled bool
}

func NewStartCommand(fs afero.Fs) *cobra.Command {
	prestartCfg := prestartConfig{}
	var (
		memoryFlag         string
		lockMemoryFlag     bool
		cpuSetFlag         string
		installDirFlag     string
		configFilePathFlag string
	)
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
				IoConfigFile:   ioConfigFile,
				LockMemory:     true,
			}
			err = prestart(fs, rpArgs, config, prestartCfg)
			if err != nil {
				return err
			}
			// Override all the defaults when flags are explicitly set
			if ccmd.Flags().Changed("memory") {
				rpArgs.Memory = memoryFlag
			}
			if ccmd.Flags().Changed("cpuset") {
				rpArgs.CpuSet = cpuSetFlag
			}
			if ccmd.Flags().Changed("lock-memory") {
				rpArgs.LockMemory = lockMemoryFlag
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
	command.Flags().StringVar(&memoryFlag,
		"memory", "", "Amount of memory for redpanda to use, "+
			"if not specified redpanda will use all available memory")
	command.Flags().BoolVar(&lockMemoryFlag,
		"lock-memory", true, "If set, will prevent redpanda from swapping")
	command.Flags().StringVar(&cpuSetFlag, "cpuset", "",
		"Set of CPUs for redpanda to use in cpuset(7) format, "+
			"if not specified redpanda will use all available CPUs")
	command.Flags().StringVar(&installDirFlag,
		"install-dir", "",
		"Directory where redpanda has been installed")
	command.Flags().BoolVar(&prestartCfg.tuneEnabled, "tune", false,
		"When present will enable tuning before starting redpanda")
	command.Flags().BoolVar(&prestartCfg.checkEnabled, "check", true,
		"When set to false will disable system checking before starting redpanda")
	return command
}

func prestart(
	fs afero.Fs,
	args *redpanda.RedpandaArgs,
	config *redpanda.Config,
	prestartCfg prestartConfig,
) error {
	if prestartCfg.tuneEnabled {
		err := tuneAll(fs, args.CpuSet, config)
		if err != nil {
			return err
		}
		log.Info("System tune - PASSED")
	}
	if prestartCfg.checkEnabled {
		err := check(fs, args.IoConfigFile, config)
		if err != nil {
			return err
		}
		log.Info("System check - PASSED")
	}
	return nil
}

func tuneAll(fs afero.Fs, cpuSet string, config *redpanda.Config) error {
	params := &factory.TunerParams{}
	tunerFactory := factory.NewTunersFactory(fs)
	hw := hwloc.NewHwLocCmd(os.NewProc())
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

	for _, tunerName := range tunerFactory.AvailableTuners() {
		tuner := tunerFactory.CreateTuner(tunerName, params)
		if supported, reason := tuner.CheckIfSupported(); supported == true {
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

func check(fs afero.Fs, ioConfigFile string, config *redpanda.Config) error {
	checkersMap := checkers.RedpandaCheckers(fs, ioConfigFile, config)
	for _, checker := range checkersMap {
		result := checker.Check()
		if result.Err != nil {
			return result.Err
		}
		if !result.IsOk {
			msg := fmt.Sprintf("System check '%s' failed. Required: %v, Current %v",
				checker.GetDesc(), checker.GetRequiredAsString(), result.Current)
			if checker.GetSeverity() == checkers.Fatal {
				return fmt.Errorf(msg)
			}
			log.Warn(msg)
		}
	}
	return nil
}
