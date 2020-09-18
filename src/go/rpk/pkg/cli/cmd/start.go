package cmd

import (
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"time"
	"vectorized/pkg/api"
	"vectorized/pkg/cli"
	"vectorized/pkg/cloud"
	"vectorized/pkg/config"
	vos "vectorized/pkg/os"
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
		configFile     string
		installDirFlag string
		timeout        time.Duration
		wellKnownIo    string
	)
	sFlags := seastarFlags{}
	sFlagsMap := map[string]interface{}{
		"lock-memory":        &sFlags.lockMemory,
		"io-properties-file": &sFlags.ioPropertiesFile,
		"cpuset":             &sFlags.cpuSet,
		"memory":             &sFlags.memory,
		"smp":                &sFlags.smp,
		"reserve-memory":     &sFlags.reserveMemory,
		"hugepages":          &sFlags.hugepages,
		"thread-affinity":    &sFlags.threadAffinity,
		"num-io-queues":      &sFlags.numIoQueues,
		"max-io-requests":    &sFlags.maxIoRequests,
		"io-properties":      &sFlags.ioProperties,
		"mbind":              &sFlags.mbind,
	}
	command := &cobra.Command{
		Use:   "start",
		Short: "Start redpanda",
		RunE: func(ccmd *cobra.Command, args []string) error {
			conf, err := config.ReadOrGenerate(fs, configFile)
			if err != nil {
				return err
			}
			config.CheckAndPrintNotice(conf.LicenseKey)
			env := api.EnvironmentPayload{}
			installDirectory, err := cli.GetOrFindInstallDir(fs, installDirFlag)
			if err != nil {
				sendEnv(fs, env, conf, err)
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
				sendEnv(fs, env, conf, err)
				return err
			}
			checkPayloads, tunerPayloads, err := prestart(
				fs,
				rpArgs,
				configFile,
				conf,
				prestartCfg,
				timeout,
			)
			env.Checks = checkPayloads
			env.Tuners = tunerPayloads
			if err != nil {
				sendEnv(fs, env, conf, err)
				return err
			}
			// Override all the defaults when flags are explicitly set
			for flag, val := range sFlagsMap {
				if ccmd.Flags().Changed(flag) {
					// Reflection is needed since sFlagsMap
					// is a map[string]interface{} - its
					// values are pointers to int, bool &
					// string - and Go doesn't allow
					// dereferencing interface{}.
					v := reflect.ValueOf(val).Elem()
					rpArgs.SeastarFlags[flag] = fmt.Sprint(v)
				}
			}

			sendEnv(fs, env, conf, nil)
			rpArgs.SeastarFlags = mergeFlags(rpArgs.SeastarFlags, conf.Rpk.AdditionalStartFlags)
			launcher := redpanda.NewLauncher(installDirectory, rpArgs)
			log.Info(feedbackMsg)
			log.Info("Starting redpanda...")
			return launcher.Start()
		},
	}
	command.Flags().StringVar(
		&configFile,
		"config",
		config.DefaultConfig().ConfigFile,
		"Redpanda config file, if not set the file will be searched for"+
			" in the default locations",
	)
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
	command.Flags().IntVar(&sFlags.smp, "smp", 1, "Restrict redpanda to"+
		" the given number of CPUs. This option does not mandate a"+
		" specific placement of CPUs. See --cpuset if you need to do so.")
	command.Flags().StringVar(&sFlags.reserveMemory, "reserve-memory", "",
		"Memory reserved for the OS (if --memory isn't specified)")
	command.Flags().StringVar(&sFlags.hugepages, "hugepages", "",
		"Path to accessible hugetlbfs mount (typically /dev/hugepages/something)")
	command.Flags().BoolVar(&sFlags.threadAffinity, "thread-affinity", true,
		"Pin threads to their cpus (disable for overprovisioning)")
	command.Flags().IntVar(&sFlags.numIoQueues, "num-io-queues", 0,
		"Number of IO queues. Each IO unit will be responsible for a fraction "+
			"of the IO requests. Defaults to the number of threads")
	command.Flags().IntVar(&sFlags.maxIoRequests, "max-io-requests", 0,
		"Maximum amount of concurrent requests to be sent to the disk. "+
			"Defaults to 128 times the number of IO queues")
	command.Flags().StringVar(&sFlags.ioPropertiesFile, "io-properties-file", "",
		"Path to a YAML file describing the characteristics of the I/O Subsystem")
	command.Flags().StringVar(&sFlags.ioProperties, "io-properties", "",
		"A YAML string describing the characteristics of the I/O Subsystem")
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
) ([]api.CheckPayload, []api.TunerPayload, error) {
	var err error
	checkPayloads := []api.CheckPayload{}
	tunerPayloads := []api.TunerPayload{}
	if prestartCfg.checkEnabled {
		checkPayloads, err = check(fs, configFile, conf, timeout, checkFailedActions(args))
		if err != nil {
			return checkPayloads, tunerPayloads, err
		}
		log.Info("System check - PASSED")
	}
	if prestartCfg.tuneEnabled {
		tunerPayloads, err = tuneAll(fs, args.SeastarFlags["cpuset"], conf, timeout)
		if err != nil {
			return checkPayloads, tunerPayloads, err
		}
		log.Info("System tune - PASSED")
	}
	return checkPayloads, tunerPayloads, nil
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

func mergeFlags(
	current map[string]string, overrides []string,
) map[string]string {
	overridesMap := map[string]string{}
	for _, o := range overrides {
		pattern := regexp.MustCompile(`[\s=]+`)
		parts := pattern.Split(o, 2)
		flagName := strings.ReplaceAll(parts[0], "--", "")
		if len(parts) == 2 {
			overridesMap[flagName] = parts[1]
		} else {
			overridesMap[flagName] = ""
		}
	}
	for k, v := range overridesMap {
		current[k] = v
	}
	return current
}

func resolveWellKnownIo(
	conf *config.Config, wellKnownIo string,
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
	fs afero.Fs, cpuSet string, conf *config.Config, timeout time.Duration,
) ([]api.TunerPayload, error) {
	params := &factory.TunerParams{}
	tunerFactory := factory.NewDirectExecutorTunersFactory(fs, *conf, timeout)
	hw := hwloc.NewHwLocCmd(vos.NewProc(), timeout)
	if cpuSet == "" {
		cpuMask, err := hw.All()
		if err != nil {
			return []api.TunerPayload{}, err
		}
		params.CpuMask = cpuMask
	} else {
		cpuMask, err := hwloc.TranslateToHwLocCpuSet(cpuSet)
		if err != nil {
			return []api.TunerPayload{}, err
		}
		params.CpuMask = cpuMask
	}

	err := factory.FillTunerParamsWithValuesFromConfig(params, conf)
	if err != nil {
		return []api.TunerPayload{}, err
	}

	availableTuners := factory.AvailableTuners()
	tunerPayloads := make([]api.TunerPayload, len(availableTuners))

	for _, tunerName := range availableTuners {
		enabled := factory.IsTunerEnabled(tunerName, conf.Rpk)
		tuner := tunerFactory.CreateTuner(tunerName, params)
		supported, reason := tuner.CheckIfSupported()
		payload := api.TunerPayload{
			Name:      tunerName,
			Enabled:   enabled,
			Supported: supported,
		}
		if !enabled {
			log.Infof("Skipping disabled tuner %s", tunerName)
			tunerPayloads = append(tunerPayloads, payload)
			continue
		}
		if !supported {
			log.Debugf("Tuner '%s' is not supported - %s", tunerName, reason)
			tunerPayloads = append(tunerPayloads, payload)
			continue
		}
		log.Debugf("Tuner parameters %+v", params)
		result := tuner.Tune()
		if result.IsFailed() {
			payload.ErrorMsg = result.Error().Error()
			tunerPayloads = append(tunerPayloads, payload)
			return tunerPayloads, result.Error()
		}
	}
	return tunerPayloads, nil
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
) ([]api.CheckPayload, error) {
	payloads := make([]api.CheckPayload, 0)
	results, err := tuners.Check(fs, configFile, conf, timeout)
	if err != nil {
		return payloads, err
	}
	for _, result := range results {
		payload := api.CheckPayload{
			Name:     result.Desc,
			Current:  result.Current,
			Required: result.Required,
		}
		if result.Err != nil {
			payload.ErrorMsg = result.Err.Error()
		}
		payloads = append(payloads, payload)
		if !result.IsOk {
			if action, exists := checkFailedActions[result.CheckerId]; exists {
				action(&result)
			}
			msg := fmt.Sprintf("System check '%s' failed. Required: %v, Current %v",
				result.Desc, result.Required, result.Current)
			if result.Severity == tuners.Fatal {
				return payloads, fmt.Errorf(msg)
			}
			log.Warn(msg)
		}
	}
	return payloads, nil
}

func sendEnv(
	fs afero.Fs, env api.EnvironmentPayload, conf *config.Config, err error,
) {
	if err != nil {
		env.ErrorMsg = err.Error()
	}
	// The config.Config struct holds only a subset of everything that can
	// go in the YAML config file, so try to read the file directly to
	// send everything.
	confJSON, err := config.ReadAsJSON(fs, conf.ConfigFile)
	if err != nil {
		log.Warnf(
			"Couldn't send latest config at '%s' due to: %s",
			conf.ConfigFile,
			err,
		)
		confBytes, err := json.Marshal(conf)
		if err != nil {
			log.Warnf(
				"Couldn't marshal the loaded config: %s",
				err,
			)
		}
		confJSON = string(confBytes)
	}
	err = api.SendEnvironment(env, *conf, confJSON)
	if err != nil {
		log.Warnf("couldn't send environment data: %v", err)
	}
}
