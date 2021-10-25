// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// +build linux

package redpanda

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/api"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/cli"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/cli/cmd/common"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/cloud"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/config"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/net"
	vos "github.com/vectorizedio/redpanda/src/go/rpk/pkg/os"
	rp "github.com/vectorizedio/redpanda/src/go/rpk/pkg/redpanda"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/tuners"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/tuners/factory"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/tuners/hwloc"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/tuners/iotune"
)

type prestartConfig struct {
	tuneEnabled  bool
	checkEnabled bool
}

type seastarFlags struct {
	memory           string
	lockMemory       bool
	reserveMemory    string
	hugepages        string
	cpuSet           string
	ioPropertiesFile string
	ioProperties     string
	smp              int
	threadAffinity   bool
	numIoQueues      int
	maxIoRequests    int
	mbind            bool
	overprovisioned  bool
}

const (
	configFlag           = "config"
	memoryFlag           = "memory"
	lockMemoryFlag       = "lock-memory"
	reserveMemoryFlag    = "reserve-memory"
	hugepagesFlag        = "hugepages"
	cpuSetFlag           = "cpuset"
	ioPropertiesFileFlag = "io-properties-file"
	ioPropertiesFlag     = "io-properties"
	wellKnownIOFlag      = "well-known-io"
	smpFlag              = "smp"
	threadAffinityFlag   = "thread-affinity"
	numIoQueuesFlag      = "num-io-queues"
	maxIoRequestsFlag    = "max-io-requests"
	mbindFlag            = "mbind"
	overprovisionedFlag  = "overprovisioned"
	nodeIDFlag           = "node-id"
	setConfigFlag        = "set"
)

func updateConfigWithFlags(conf *config.Config, flags *pflag.FlagSet) {
	if flags.Changed(configFlag) {
		conf.ConfigFile, _ = flags.GetString(configFlag)
	}
	if flags.Changed(lockMemoryFlag) {
		conf.Rpk.EnableMemoryLocking, _ = flags.GetBool(lockMemoryFlag)
	}
	if flags.Changed(wellKnownIOFlag) {
		conf.Rpk.WellKnownIo, _ = flags.GetString(wellKnownIOFlag)
	}
	if flags.Changed(overprovisionedFlag) {
		conf.Rpk.Overprovisioned, _ = flags.GetBool(overprovisionedFlag)
	}
	if flags.Changed(nodeIDFlag) {
		conf.Redpanda.Id, _ = flags.GetInt(nodeIDFlag)
	}
}

func parseConfigKvs(args []string) ([]string, []string) {
	setFlag := fmt.Sprintf("--%s", setConfigFlag)
	kvs := []string{}
	i := 0
	for i < len(args)-1 {
		if args[i] == setFlag {
			kvs = append(kvs, args[i+1])
			args = append(args[:i], args[i+2:]...)
			continue
		}
		i++
	}
	return kvs, args
}

func NewStartCommand(
	fs afero.Fs, mgr config.Manager, launcher rp.Launcher,
) *cobra.Command {
	prestartCfg := prestartConfig{}
	var (
		configFile      string
		nodeID          int
		seeds           []string
		kafkaAddr       []string
		proxyAddr       []string
		schemaRegAddr   []string
		rpcAddr         string
		advertisedKafka []string
		advertisedProxy []string
		advertisedRPC   string
		installDirFlag  string
		timeout         time.Duration
		wellKnownIo     string
	)
	sFlags := seastarFlags{}

	command := &cobra.Command{
		Use:   "start",
		Short: "Start redpanda",
		FParseErrWhitelist: cobra.FParseErrWhitelist{
			// Allow unknown flags so that arbitrary flags can be passed
			// through to redpanda/seastar without the need to pass '--'
			// (POSIX standard)
			UnknownFlags: true,
		},
		RunE: func(ccmd *cobra.Command, args []string) error {
			// --set flags have to be parsed by hand because pflag (the
			// underlying flag-parsing lib used by cobra) uses a CSV parser
			// for list flags, and since JSON often contains commas, it
			// blows up when there's a JSON object.
			configKvs, filteredArgs := parseConfigKvs(os.Args)
			conf, err := mgr.FindOrGenerate(configFile)
			if err != nil {
				return err
			}

			if len(configKvs) > 0 {
				conf, err = setConfig(mgr, configKvs)
				if err != nil {
					return err
				}
			}

			updateConfigWithFlags(conf, ccmd.Flags())

			env := api.EnvironmentPayload{}
			if len(seeds) == 0 {
				// If --seeds wasn't passed, fall back to the
				// env var.
				envSeeds := os.Getenv("REDPANDA_SEEDS")
				if envSeeds != "" {
					seeds = strings.Split(
						envSeeds,
						",",
					)
				}
			}
			seedServers, err := parseSeeds(seeds)
			if err != nil {
				sendEnv(fs, mgr, env, conf, !prestartCfg.checkEnabled, err)
				return err
			}
			if len(seedServers) != 0 {
				conf.Redpanda.SeedServers = seedServers
			}

			kafkaAddr = stringSliceOr(
				kafkaAddr,
				strings.Split(
					os.Getenv("REDPANDA_KAFKA_ADDRESS"),
					",",
				),
			)
			kafkaApi, err := parseNamedAddresses(
				kafkaAddr,
				config.DefaultKafkaPort,
			)
			if err != nil {
				sendEnv(fs, mgr, env, conf, !prestartCfg.checkEnabled, err)
				return err
			}
			if kafkaApi != nil && len(kafkaApi) > 0 {
				conf.Redpanda.KafkaApi = kafkaApi
			}

			proxyAddr = stringSliceOr(
				proxyAddr,
				strings.Split(
					os.Getenv("REDPANDA_PANDAPROXY_ADDRESS"),
					",",
				),
			)
			proxyApi, err := parseNamedAddresses(
				proxyAddr,
				config.DefaultProxyPort,
			)
			if err != nil {
				sendEnv(fs, mgr, env, conf, !prestartCfg.checkEnabled, err)
				return err
			}
			if proxyApi != nil && len(proxyApi) > 0 {
				if conf.Pandaproxy == nil {
					conf.Pandaproxy = config.Default().Pandaproxy
				}
				conf.Pandaproxy.PandaproxyAPI = proxyApi
			}

			schemaRegAddr = stringSliceOr(
				schemaRegAddr,
				strings.Split(
					os.Getenv("REDPANDA_SCHEMA_REGISTRY_ADDRESS"),
					",",
				),
			)
			schemaRegApi, err := parseNamedAddresses(
				schemaRegAddr,
				config.DefaultSchemaRegPort,
			)
			if err != nil {
				sendEnv(fs, mgr, env, conf, !prestartCfg.checkEnabled, err)
				return err
			}
			if schemaRegApi != nil && len(schemaRegApi) > 0 {
				if conf.SchemaRegistry == nil {
					conf.SchemaRegistry = config.Default().SchemaRegistry
				}
				conf.SchemaRegistry.SchemaRegistryAPI = schemaRegApi
			}

			rpcAddr = stringOr(
				rpcAddr,
				os.Getenv("REDPANDA_RPC_ADDRESS"),
			)
			rpcServer, err := parseAddress(
				rpcAddr,
				config.Default().Redpanda.RPCServer.Port,
			)
			if err != nil {
				sendEnv(fs, mgr, env, conf, !prestartCfg.checkEnabled, err)
				return err
			}
			if rpcServer != nil {
				conf.Redpanda.RPCServer = *rpcServer
			}

			advertisedKafka = stringSliceOr(
				advertisedKafka,
				strings.Split(
					os.Getenv("REDPANDA_ADVERTISE_KAFKA_ADDRESS"),
					",",
				),
			)
			advKafkaApi, err := parseNamedAddresses(
				advertisedKafka,
				config.DefaultKafkaPort,
			)
			if err != nil {
				sendEnv(fs, mgr, env, conf, !prestartCfg.checkEnabled, err)
				return err
			}
			if advKafkaApi != nil {
				conf.Redpanda.AdvertisedKafkaApi = advKafkaApi
			}

			advertisedProxy = stringSliceOr(
				advertisedProxy,
				strings.Split(
					os.Getenv("REDPANDA_ADVERTISE_PANDAPROXY_ADDRESS"),
					",",
				),
			)
			advProxyApi, err := parseNamedAddresses(
				advertisedProxy,
				config.DefaultProxyPort,
			)
			if err != nil {
				sendEnv(fs, mgr, env, conf, !prestartCfg.checkEnabled, err)
				return err
			}
			if advProxyApi != nil {
				if conf.Pandaproxy == nil {
					conf.Pandaproxy = config.Default().Pandaproxy
				}
				conf.Pandaproxy.AdvertisedPandaproxyAPI = advProxyApi
			}

			advertisedRPC = stringOr(
				advertisedRPC,
				os.Getenv("REDPANDA_ADVERTISE_RPC_ADDRESS"),
			)
			advRPCApi, err := parseAddress(
				advertisedRPC,
				config.Default().Redpanda.RPCServer.Port,
			)
			if err != nil {
				sendEnv(fs, mgr, env, conf, !prestartCfg.checkEnabled, err)
				return err
			}
			if advRPCApi != nil {
				conf.Redpanda.AdvertisedRPCAPI = advRPCApi
			}
			installDirectory, err := cli.GetOrFindInstallDir(fs, installDirFlag)
			if err != nil {
				sendEnv(fs, mgr, env, conf, !prestartCfg.checkEnabled, err)
				return err
			}
			rpArgs, err := buildRedpandaFlags(
				fs,
				conf,
				filteredArgs,
				sFlags,
				ccmd.Flags(),
				!prestartCfg.checkEnabled,
			)
			if err != nil {
				sendEnv(fs, mgr, env, conf, !prestartCfg.checkEnabled, err)
				return err
			}
			checkPayloads, tunerPayloads, err := prestart(
				fs,
				rpArgs,
				conf,
				prestartCfg,
				timeout,
			)
			env.Checks = checkPayloads
			env.Tuners = tunerPayloads
			if err != nil {
				sendEnv(fs, mgr, env, conf, !prestartCfg.checkEnabled, err)
				return err
			}

			err = mgr.Write(conf)
			if err != nil {
				sendEnv(fs, mgr, env, conf, !prestartCfg.checkEnabled, err)
				return err
			}

			sendEnv(fs, mgr, env, conf, !prestartCfg.checkEnabled, nil)
			rpArgs.ExtraArgs = args
			log.Info(common.FeedbackMsg)
			log.Info("Starting redpanda...")
			return launcher.Start(installDirectory, rpArgs)
		},
	}
	command.Flags().StringVar(
		&configFile,
		configFlag,
		"",
		"Redpanda config file, if not set the file will be searched for"+
			" in the default locations",
	)
	command.Flags().IntVar(
		&nodeID,
		nodeIDFlag,
		0,
		"The node ID. Must be an integer and must be unique"+
			" within a cluster",
	)
	command.Flags().StringSliceVarP(
		&seeds,
		"seeds",
		"s",
		[]string{},
		"A comma-separated list of seed node addresses"+
			" (<host>[:<port>]) to connect to",
	)
	command.Flags().StringSliceVar(
		&kafkaAddr,
		"kafka-addr",
		[]string{},
		"A comma-separated list of Kafka listener addresses to bind to (<name>://<host>:<port>)",
	)
	command.Flags().StringSliceVar(
		&proxyAddr,
		"pandaproxy-addr",
		[]string{},
		"A comma-separated list of Pandaproxy listener addresses to bind to (<name>://<host>:<port>)",
	)
	command.Flags().StringSliceVar(
		&schemaRegAddr,
		"schema-registry-addr",
		[]string{},
		"A comma-separated list of Schema Registry listener addresses to bind to (<name>://<host>:<port>)",
	)
	command.Flags().StringVar(
		&rpcAddr,
		"rpc-addr",
		"",
		"The RPC address to bind to (<host>:<port>)",
	)
	command.Flags().StringSliceVar(
		&advertisedKafka,
		"advertise-kafka-addr",
		[]string{},
		"A comma-separated list of Kafka addresses to advertise (<name>://<host>:<port>)",
	)
	command.Flags().StringSliceVar(
		&advertisedProxy,
		"advertise-pandaproxy-addr",
		[]string{},
		"A comma-separated list of Pandaproxy addresses to advertise (<name>://<host>:<port>)",
	)
	command.Flags().StringVar(
		&advertisedRPC,
		"advertise-rpc-addr",
		"",
		"The advertised RPC address (<host>:<port>)",
	)
	command.Flags().StringVar(&sFlags.memory,
		memoryFlag, "", "Amount of memory for redpanda to use, "+
			"if not specified redpanda will use all available memory")
	command.Flags().BoolVar(&sFlags.lockMemory,
		lockMemoryFlag, false, "If set, will prevent redpanda from swapping")
	command.Flags().StringVar(&sFlags.cpuSet, cpuSetFlag, "",
		"Set of CPUs for redpanda to use in cpuset(7) format, "+
			"if not specified redpanda will use all available CPUs")
	command.Flags().StringVar(&installDirFlag,
		"install-dir", "",
		"Directory where redpanda has been installed")
	command.Flags().BoolVar(&prestartCfg.tuneEnabled, "tune", false,
		"When present will enable tuning before starting redpanda")
	command.Flags().BoolVar(&prestartCfg.checkEnabled, "check", true,
		"When set to false will disable system checking before starting redpanda")
	command.Flags().IntVar(&sFlags.smp, smpFlag, 0, "Restrict redpanda to"+
		" the given number of CPUs. This option does not mandate a"+
		" specific placement of CPUs. See --cpuset if you need to do so.")
	command.Flags().StringVar(&sFlags.reserveMemory, reserveMemoryFlag, "",
		"Memory reserved for the OS (if --memory isn't specified)")
	command.Flags().StringVar(&sFlags.hugepages, hugepagesFlag, "",
		"Path to accessible hugetlbfs mount (typically /dev/hugepages/something)")
	command.Flags().BoolVar(&sFlags.threadAffinity, threadAffinityFlag, true,
		"Pin threads to their cpus (disable for overprovisioning)")
	command.Flags().IntVar(&sFlags.numIoQueues, numIoQueuesFlag, 0,
		"Number of IO queues. Each IO unit will be responsible for a fraction "+
			"of the IO requests. Defaults to the number of threads")
	command.Flags().IntVar(&sFlags.maxIoRequests, maxIoRequestsFlag, 0,
		"Maximum amount of concurrent requests to be sent to the disk. "+
			"Defaults to 128 times the number of IO queues")
	command.Flags().StringVar(&sFlags.ioPropertiesFile, ioPropertiesFileFlag, "",
		"Path to a YAML file describing the characteristics of the I/O Subsystem")
	command.Flags().StringVar(&sFlags.ioProperties, ioPropertiesFlag, "",
		"A YAML string describing the characteristics of the I/O Subsystem")
	command.Flags().StringVar(
		&wellKnownIo,
		wellKnownIOFlag,
		"",
		"The cloud vendor and VM type, in the format <vendor>:<vm type>:<storage type>")
	command.Flags().BoolVar(&sFlags.mbind, mbindFlag, true, "enable mbind")
	command.Flags().BoolVar(
		&sFlags.overprovisioned,
		overprovisionedFlag,
		false,
		"Enable overprovisioning",
	)
	command.Flags().DurationVar(
		&timeout,
		"timeout",
		10000*time.Millisecond,
		"The maximum time to wait for the checks and tune processes to complete. "+
			"The value passed is a sequence of decimal numbers, each with optional "+
			"fraction and a unit suffix, such as '300ms', '1.5s' or '2h45m'. "+
			"Valid time units are 'ns', 'us' (or 'µs'), 'ms', 's', 'm', 'h'",
	)
	for flag := range flagsMap(sFlags) {
		command.Flag(flag).Hidden = true
	}
	return command
}

func flagsMap(sFlags seastarFlags) map[string]interface{} {
	return map[string]interface{}{
		memoryFlag:           sFlags.memory,
		lockMemoryFlag:       sFlags.lockMemory,
		reserveMemoryFlag:    sFlags.reserveMemory,
		ioPropertiesFileFlag: sFlags.ioPropertiesFile,
		ioPropertiesFlag:     sFlags.ioProperties,
		cpuSetFlag:           sFlags.cpuSet,
		smpFlag:              sFlags.smp,
		hugepagesFlag:        sFlags.hugepages,
		threadAffinityFlag:   sFlags.threadAffinity,
		numIoQueuesFlag:      sFlags.numIoQueues,
		maxIoRequestsFlag:    sFlags.maxIoRequests,
		mbindFlag:            sFlags.mbind,
		overprovisionedFlag:  sFlags.overprovisioned,
	}
}

func prestart(
	fs afero.Fs,
	args *rp.RedpandaArgs,
	conf *config.Config,
	prestartCfg prestartConfig,
	timeout time.Duration,
) ([]api.CheckPayload, []api.TunerPayload, error) {
	var err error
	checkPayloads := []api.CheckPayload{}
	tunerPayloads := []api.TunerPayload{}
	if prestartCfg.checkEnabled {
		checkPayloads, err = check(fs, conf, timeout, checkFailedActions(args))
		if err != nil {
			return checkPayloads, tunerPayloads, err
		}
		log.Info("System check - PASSED")
	}
	if prestartCfg.tuneEnabled {
		cpuset := fmt.Sprint(args.SeastarFlags[cpuSetFlag])
		tunerPayloads, err = tuneAll(fs, cpuset, conf, timeout)
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
	args []string,
	sFlags seastarFlags,
	flags *pflag.FlagSet,
	skipChecks bool,
) (*rp.RedpandaArgs, error) {
	wellKnownIOSet := conf.Rpk.WellKnownIo != ""
	ioPropsSet := flags.Changed(ioPropertiesFileFlag) || flags.Changed(ioPropertiesFlag)
	if wellKnownIOSet && ioPropsSet {
		return nil, errors.New(
			"--well-known-io or (rpk.well_known_io) and" +
				" --io-properties (or --io-properties-file)" +
				" can't be set at the same time",
		)
	}

	if !ioPropsSet {
		// If --io-properties-file and --io-properties weren't set, try
		// finding an IO props file in the default location.
		sFlags.ioPropertiesFile = rp.GetIOConfigPath(
			filepath.Dir(conf.ConfigFile),
		)
		if exists, _ := afero.Exists(fs, sFlags.ioPropertiesFile); !exists {
			sFlags.ioPropertiesFile = ""
		}
		// Otherwise, try to deduce the IO props.
		if sFlags.ioPropertiesFile == "" {
			ioProps, err := resolveWellKnownIo(conf, skipChecks)
			if err != nil {
				log.Warn(err)
			} else if ioProps != nil {
				yaml, err := iotune.ToYaml(*ioProps)
				if err != nil {
					return nil, err
				}
				sFlags.ioProperties = fmt.Sprintf("'%s'", yaml)
			}
		}
	}
	flagsMap := flagsMap(sFlags)
	for flag := range flagsMap {
		if !flags.Changed(flag) {
			delete(flagsMap, flag)
		}
	}
	flagsMap = flagsFromConf(conf, flagsMap, flags)
	finalFlags := mergeMaps(
		parseFlags(conf.Rpk.AdditionalStartFlags),
		extraFlags(flags, args),
	)
	for n, v := range flagsMap {
		if _, alreadyPresent := finalFlags[n]; alreadyPresent {
			return nil, fmt.Errorf(
				"Configuration conflict. Flag '--%s'"+
					" is also present in"+
					" 'rpk.additional_start_flags' in"+
					" configuration file '%s'. Please"+
					" remove it and pass '--%s' directly"+
					" to `rpk start`.",
				n,
				conf.ConfigFile,
				n,
			)
		}
		finalFlags[n] = fmt.Sprint(v)
	}
	return &rp.RedpandaArgs{
		ConfigFilePath: conf.ConfigFile,
		SeastarFlags:   finalFlags,
	}, nil
}

func flagsFromConf(
	conf *config.Config, flagsMap map[string]interface{}, flags *pflag.FlagSet,
) map[string]interface{} {
	flagsMap[overprovisionedFlag] = conf.Rpk.Overprovisioned
	flagsMap[lockMemoryFlag] = conf.Rpk.EnableMemoryLocking
	// Setting SMP to 0 doesn't make sense.
	if !flags.Changed(smpFlag) && conf.Rpk.SMP != nil && *conf.Rpk.SMP != 0 {
		flagsMap[smpFlag] = *conf.Rpk.SMP
	}
	return flagsMap
}

func mergeFlags(
	current map[string]interface{}, overrides []string,
) map[string]interface{} {
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

func setConfig(mgr config.Manager, configKvs []string) (*config.Config, error) {
	for _, rawKv := range configKvs {
		parts := strings.SplitN(rawKv, "=", 2)
		if len(parts) < 2 {
			return nil, fmt.Errorf(
				"key-value pair '%s' is not formatted as expected (k=v)",
				rawKv,
			)
		}
		err := mgr.Set(parts[0], parts[1], "")
		if err != nil {
			return nil, err
		}
	}
	return mgr.Get()
}

func resolveWellKnownIo(
	conf *config.Config, skipChecks bool,
) (*iotune.IoProperties, error) {
	var ioProps *iotune.IoProperties
	if conf.Rpk.WellKnownIo != "" {
		wellKnownIoTokens := strings.Split(conf.Rpk.WellKnownIo, ":")
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
	// Skip detecting the cloud vendor if skipChecks is true
	if skipChecks {
		return nil, nil
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
	args *rp.RedpandaArgs,
) map[tuners.CheckerID]checkFailedAction {
	return map[tuners.CheckerID]checkFailedAction{
		tuners.SwapChecker: func(*tuners.CheckResult) {
			// Do not set --lock-memory flag when swap is disabled
			args.SeastarFlags[lockMemoryFlag] = "false"
		},
	}
}

func check(
	fs afero.Fs,
	conf *config.Config,
	timeout time.Duration,
	checkFailedActions map[tuners.CheckerID]checkFailedAction,
) ([]api.CheckPayload, error) {
	payloads := make([]api.CheckPayload, 0)
	results, err := tuners.Check(fs, conf, timeout)
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

func parseFlags(flags []string) map[string]string {
	parsed := map[string]string{}
	for i := 0; i < len(flags); i++ {
		f := flags[i]
		isFlag := strings.HasPrefix(f, "-")
		trimmed := strings.Trim(f, " -")

		// Filter out elements that aren't flags or are empty.
		if !isFlag || trimmed == "" {
			continue
		}

		// Check if it's in name=value format
		// Split only into 2 tokens, since some flags can have multiple '='
		// in them, like --logger-log-level=archival=debug:cloud_storage=debug
		parts := strings.SplitN(trimmed, "=", 2)
		if len(parts) >= 2 {
			name := strings.Trim(parts[0], " ")
			value := strings.Trim(parts[1], " ")
			parsed[name] = value
			continue
		}
		// Otherwise, it can be a boolean flag (i.e. -v) or in
		// name<space>value format

		if i == len(flags)-1 {
			// We've reached the last element, so it's a single flag
			parsed[trimmed] = "true"
			continue
		}

		// Check if the next element starts with a hyphen
		// If it does, it's another flag, and the current element is a
		// boolean flag
		next := flags[i+1]
		if strings.HasPrefix(next, "-") {
			parsed[trimmed] = "true"
			continue
		}

		// Otherwise, the current element is the name of the flag and
		// the next one is its value
		parsed[trimmed] = next
		i += 1
	}
	return parsed
}

func parseSeeds(seeds []string) ([]config.SeedServer, error) {
	seedServers := []config.SeedServer{}
	defaultPort := config.Default().Redpanda.RPCServer.Port
	for _, s := range seeds {
		addr, err := parseAddress(s, defaultPort)
		if err != nil {
			return seedServers, fmt.Errorf(
				"Couldn't parse seed '%s': %v",
				s,
				err,
			)
		}
		if addr == nil {
			return seedServers, fmt.Errorf(
				"Couldn't parse seed '%s': empty address",
				s,
			)
		}
		seedServers = append(
			seedServers,
			config.SeedServer{Host: *addr},
		)
	}
	return seedServers, nil
}

func parseAddress(addr string, defaultPort int) (*config.SocketAddress, error) {
	named, err := parseNamedAddress(addr, defaultPort)
	if err != nil {
		return nil, err
	}
	if named == nil {
		return nil, nil
	}
	return &named.SocketAddress, nil
}

func parseNamedAddresses(
	addrs []string, defaultPort int,
) ([]config.NamedSocketAddress, error) {
	as := make([]config.NamedSocketAddress, 0, len(addrs))
	for _, addr := range addrs {
		a, err := parseNamedAddress(addr, defaultPort)
		if err != nil {
			return nil, err
		}
		if a != nil {
			as = append(as, *a)
		}
	}
	return as, nil
}

func parseNamedAddress(
	addr string, defaultPort int,
) (*config.NamedSocketAddress, error) {
	if addr == "" {
		return nil, nil
	}
	scheme, hostport, err := net.ParseHostMaybeScheme(addr)
	if err != nil {
		return nil, err
	}
	host, port := net.SplitHostPortDefault(hostport, defaultPort)

	return &config.NamedSocketAddress{
		SocketAddress: config.SocketAddress{
			Address: host,
			Port:    port,
		},
		Name: scheme,
	}, nil
}

func sendEnv(
	fs afero.Fs,
	mgr config.Manager,
	env api.EnvironmentPayload,
	conf *config.Config,
	skipChecks bool,
	err error,
) {
	if err != nil {
		env.ErrorMsg = err.Error()
	}
	// The config.Config struct holds only a subset of everything that can
	// go in the YAML config file, so try to read the file directly to
	// send everything.
	confJSON, err := mgr.ReadAsJSON(conf.ConfigFile)
	if err != nil {
		log.Warnf(
			"Couldn't parse latest config at '%s' due to: %s",
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
	err = api.SendEnvironment(fs, env, *conf, confJSON, skipChecks)
	if err != nil {
		log.Debugf("couldn't send environment data: %v", err)
	}
}

func stringOr(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

func stringSliceOr(a, b []string) []string {
	if a != nil && len(a) != 0 {
		return a
	}
	return b
}

// Returns the set of unknown flags passed.
func extraFlags(flags *pflag.FlagSet, args []string) map[string]string {
	allFlagsMap := parseFlags(args)
	extra := map[string]string{}

	for k, v := range allFlagsMap {
		var f *pflag.Flag
		if len(k) == 1 {
			f = flags.ShorthandLookup(k)
		} else {
			f = flags.Lookup(k)
		}
		// It isn't a "known" flag, so it must be an extra one.
		if f == nil {
			extra[k] = v
		}
	}
	return extra
}

// Merges b into a.
func mergeMaps(a, b map[string]string) map[string]string {
	for kb, vb := range b {
		a[kb] = vb
	}
	return a
}
