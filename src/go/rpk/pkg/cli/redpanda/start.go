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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cloud"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/net"
	vos "github.com/redpanda-data/redpanda/src/go/rpk/pkg/os"
	rp "github.com/redpanda-data/redpanda/src/go/rpk/pkg/redpanda"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/factory"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/hwloc"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/iotune"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"go.uber.org/zap"
)

type prestartConfig struct {
	tuneEnabled  bool
	checkEnabled bool
}

type seastarFlags struct {
	memory            string
	lockMemory        bool
	reserveMemory     string
	hugepages         string
	cpuSet            string
	ioPropertiesFile  string
	ioProperties      string
	smp               int
	threadAffinity    bool
	numIoQueues       int
	maxIoRequests     int
	mbind             bool
	overprovisioned   bool
	unsafeBypassFsync bool
}

const (
	memoryFlag            = "memory"
	lockMemoryFlag        = "lock-memory"
	reserveMemoryFlag     = "reserve-memory"
	hugepagesFlag         = "hugepages"
	cpuSetFlag            = "cpuset"
	ioPropertiesFileFlag  = "io-properties-file"
	ioPropertiesFlag      = "io-properties"
	wellKnownIOFlag       = "well-known-io"
	smpFlag               = "smp"
	threadAffinityFlag    = "thread-affinity"
	numIoQueuesFlag       = "num-io-queues"
	maxIoRequestsFlag     = "max-io-requests"
	mbindFlag             = "mbind"
	overprovisionedFlag   = "overprovisioned"
	unsafeBypassFsyncFlag = "unsafe-bypass-fsync"
	nodeIDFlag            = "node-id"
	setConfigFlag         = "set"
	modeFlag              = "mode"
	checkFlag             = "check"
)

func updateConfigWithFlags(conf *config.Config, flags *pflag.FlagSet) {
	if flags.Changed(lockMemoryFlag) {
		conf.Rpk.Tuners.EnableMemoryLocking, _ = flags.GetBool(lockMemoryFlag)
	}
	if flags.Changed(wellKnownIOFlag) {
		conf.Rpk.Tuners.WellKnownIo, _ = flags.GetString(wellKnownIOFlag)
	}
	if flags.Changed(overprovisionedFlag) {
		conf.Rpk.Tuners.Overprovisioned, _ = flags.GetBool(overprovisionedFlag)
	}
	if flags.Changed(nodeIDFlag) {
		conf.Redpanda.ID = new(int)
		*conf.Redpanda.ID, _ = flags.GetInt(nodeIDFlag)
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

func NewStartCommand(fs afero.Fs, p *config.Params, launcher rp.Launcher) *cobra.Command {
	prestartCfg := prestartConfig{}
	var (
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
		mode            string
	)
	sFlags := seastarFlags{}

	cmd := &cobra.Command{
		Use:   "start",
		Short: "Start redpanda",
		FParseErrWhitelist: cobra.FParseErrWhitelist{
			// Allow unknown flags so that arbitrary flags can be passed
			// through to redpanda/seastar without the need to pass '--'
			// (POSIX standard)
			UnknownFlags: true,
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			// --set flags have to be parsed by hand because pflag (the
			// underlying flag-parsing lib used by cobra) uses a CSV parser
			// for list flags, and since JSON often contains commas, it
			// blows up when there's a JSON object.
			configKvs, filteredArgs := parseConfigKvs(os.Args)
			cfg, err := p.Load(fs)
			if err != nil {
				return fmt.Errorf("unable to load config file: %s", err)
			}
			// We set fields in the raw file without writing rpk specific env
			// or flag overrides. This command itself has all redpanda specific
			// flags installed, and handles redpanda specific env vars itself.
			// The magic `--set` flag is what modifies any redpanda.yaml fields.
			// Thus, we can ignore any env / flags that would come from rpk
			// configuration itself.
			cfg = cfg.FileOrDefaults()
			if cfg.Redpanda.DeveloperMode && len(mode) == 0 {
				mode = "dev-container"
			}
			switch mode {
			case "dev-container":
				fmt.Fprintln(os.Stderr, "WARNING: This is a setup for development purposes only; in this mode your clusters may run unrealistically fast and data can be corrupted any time your computer shuts down uncleanly.")
				setContainerModeFlags(cmd)
				setContainerModeCfgFields(cfg)
			case "help":
				fmt.Println(helpMode)
				return nil
			case "":
				// do nothing.
			default:
				return fmt.Errorf("unrecognized mode %q; use --mode help for more info", mode)
			}

			if len(configKvs) > 0 {
				if err = setConfig(cfg, configKvs); err != nil {
					return err
				}
			}

			updateConfigWithFlags(cfg, cmd.Flags())

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
				return err
			}
			if len(seedServers) != 0 {
				cfg.Redpanda.SeedServers = seedServers
			}

			kafkaAddr = stringSliceOr(
				kafkaAddr,
				strings.Split(
					os.Getenv("REDPANDA_KAFKA_ADDRESS"),
					",",
				),
			)
			kafkaAPI, err := parseNamedAuthNAddresses(
				kafkaAddr,
				config.DefaultKafkaPort,
			)
			if err != nil {
				return err
			}
			if len(kafkaAPI) > 0 {
				cfg.Redpanda.KafkaAPI = kafkaAPI
			}

			proxyAddr = stringSliceOr(
				proxyAddr,
				strings.Split(
					os.Getenv("REDPANDA_PANDAPROXY_ADDRESS"),
					",",
				),
			)
			proxyAPI, err := parseNamedAuthNAddresses(
				proxyAddr,
				config.DefaultProxyPort,
			)
			if err != nil {
				return err
			}
			if len(proxyAPI) > 0 {
				if cfg.Pandaproxy == nil {
					cfg.Pandaproxy = config.DevDefault().Pandaproxy
				}
				cfg.Pandaproxy.PandaproxyAPI = proxyAPI
			}

			schemaRegAddr = stringSliceOr(
				schemaRegAddr,
				strings.Split(
					os.Getenv("REDPANDA_SCHEMA_REGISTRY_ADDRESS"),
					",",
				),
			)
			schemaRegAPI, err := parseNamedAuthNAddresses(
				schemaRegAddr,
				config.DefaultSchemaRegPort,
			)
			if err != nil {
				return err
			}
			if len(schemaRegAPI) > 0 {
				if cfg.SchemaRegistry == nil {
					cfg.SchemaRegistry = config.DevDefault().SchemaRegistry
				}
				cfg.SchemaRegistry.SchemaRegistryAPI = schemaRegAPI
			}

			rpcAddr = stringOr(
				rpcAddr,
				os.Getenv("REDPANDA_RPC_ADDRESS"),
			)
			rpcServer, err := parseAddress(
				rpcAddr,
				config.DevDefault().Redpanda.RPCServer.Port,
			)
			if err != nil {
				return err
			}
			if rpcServer != nil {
				cfg.Redpanda.RPCServer = *rpcServer
			}

			advertisedKafka = stringSliceOr(
				advertisedKafka,
				strings.Split(
					os.Getenv("REDPANDA_ADVERTISE_KAFKA_ADDRESS"),
					",",
				),
			)
			advKafkaAPI, err := parseNamedAddresses(
				advertisedKafka,
				config.DefaultKafkaPort,
			)
			if err != nil {
				return err
			}

			if len(advKafkaAPI) > 0 {
				cfg.Redpanda.AdvertisedKafkaAPI = advKafkaAPI
			}

			advertisedProxy = stringSliceOr(
				advertisedProxy,
				strings.Split(
					os.Getenv("REDPANDA_ADVERTISE_PANDAPROXY_ADDRESS"),
					",",
				),
			)
			advProxyAPI, err := parseNamedAddresses(
				advertisedProxy,
				config.DefaultProxyPort,
			)
			if err != nil {
				return err
			}
			if len(advProxyAPI) > 0 {
				if cfg.Pandaproxy == nil {
					cfg.Pandaproxy = config.DevDefault().Pandaproxy
				}
				cfg.Pandaproxy.AdvertisedPandaproxyAPI = advProxyAPI
			}

			advertisedRPC = stringOr(
				advertisedRPC,
				os.Getenv("REDPANDA_ADVERTISE_RPC_ADDRESS"),
			)
			advRPCApi, err := parseAddress(
				advertisedRPC,
				config.DevDefault().Redpanda.RPCServer.Port,
			)
			if err != nil {
				return err
			}
			if advRPCApi != nil {
				cfg.Redpanda.AdvertisedRPCAPI = advRPCApi
			}
			installDirectory, err := getOrFindInstallDir(fs, installDirFlag)
			if err != nil {
				return err
			}
			rpArgs, err := buildRedpandaFlags(
				fs,
				cfg,
				filteredArgs,
				sFlags,
				cmd.Flags(),
				!prestartCfg.checkEnabled,
				resolveWellKnownIo,
			)
			if err != nil {
				return err
			}

			if cfg.Redpanda.Directory == "" {
				cfg.Redpanda.Directory = config.DevDefault().Redpanda.Directory
			}

			err = prestart(fs, rpArgs, cfg, prestartCfg, timeout)
			if err != nil {
				return err
			}

			err = cfg.Write(fs)
			if err != nil {
				return err
			}
			rpArgs.ExtraArgs = args
			fmt.Print(`We'd love to hear about your experience with Redpanda:
https://redpanda.com/feedback
`)
			fmt.Println("Starting redpanda...")
			return launcher.Start(installDirectory, rpArgs)
		},
	}

	f := cmd.Flags()
	f.IntVar(&nodeID, nodeIDFlag, -1, "The node ID. Must be an integer and must be unique within a cluster. If unset, Redpanda will assign one automatically")
	f.MarkHidden(nodeIDFlag)

	f.StringSliceVarP(&seeds, "seeds", "s", nil, "A comma-separated list of seed nodes to connect to (scheme://host:port|name)")
	f.StringSliceVar(&kafkaAddr, "kafka-addr", nil, "A comma-separated list of Kafka listener addresses to bind to (scheme://host:port|name)")
	f.StringSliceVar(&proxyAddr, "pandaproxy-addr", nil, "A comma-separated list of Pandaproxy listener addresses to bind to (scheme://host:port|name)")
	f.StringSliceVar(&schemaRegAddr, "schema-registry-addr", nil, "A comma-separated list of Schema Registry listener addresses to bind to (scheme://host:port|name)")
	f.StringVar(&rpcAddr, "rpc-addr", "", "The RPC address to bind to (host:port)")
	f.StringSliceVar(&advertisedKafka, "advertise-kafka-addr", nil, "A comma-separated list of Kafka addresses to advertise (scheme://host:port|name)")
	f.StringSliceVar(&advertisedProxy, "advertise-pandaproxy-addr", nil, "A comma-separated list of Pandaproxy addresses to advertise (scheme://host:port|name)")
	f.StringVar(&advertisedRPC, "advertise-rpc-addr", "", "The advertised RPC address (host:port)")
	f.StringVar(&sFlags.memory, memoryFlag, "", "Amount of memory for redpanda to use, if not specified redpanda will use all available memory")
	f.BoolVar(&sFlags.lockMemory, lockMemoryFlag, false, "If set, will prevent redpanda from swapping")
	f.StringVar(&sFlags.cpuSet, cpuSetFlag, "", "Set of CPUs for redpanda to use in cpuset(7) format, if not specified redpanda will use all available CPUs")
	f.StringVar(&installDirFlag, "install-dir", "", "Directory where redpanda has been installed")
	f.BoolVar(&prestartCfg.tuneEnabled, "tune", false, "When present will enable tuning before starting redpanda")
	f.BoolVar(&prestartCfg.checkEnabled, checkFlag, true, "When set to false will disable system checking before starting redpanda")
	f.IntVar(&sFlags.smp, smpFlag, 0, "Restrict redpanda to the given number of CPUs. This option does not mandate a specific placement of CPUs. See --cpuset if you need to do so.")
	f.StringVar(&sFlags.reserveMemory, reserveMemoryFlag, "", "Memory reserved for the OS (if --memory isn't specified)")
	f.StringVar(&sFlags.hugepages, hugepagesFlag, "", "Path to accessible hugetlbfs mount (typically /dev/hugepages/something)")
	f.BoolVar(&sFlags.threadAffinity, threadAffinityFlag, true, "Pin threads to their cpus (disable for overprovisioning)")
	f.IntVar(&sFlags.numIoQueues, numIoQueuesFlag, 0, "Number of IO queues. Each IO unit will be responsible for a fraction of the IO requests. Defaults to the number of threads")
	f.IntVar(&sFlags.maxIoRequests, maxIoRequestsFlag, 0, "Maximum amount of concurrent requests to be sent to the disk. Defaults to 128 times the number of IO queues")
	f.StringVar(&sFlags.ioPropertiesFile, ioPropertiesFileFlag, "", "Path to a YAML file describing the characteristics of the I/O Subsystem")
	f.StringVar(&sFlags.ioProperties, ioPropertiesFlag, "", "A YAML string describing the characteristics of the I/O Subsystem")
	f.StringVar(&wellKnownIo, wellKnownIOFlag, "", "The cloud vendor and VM type, in the format <vendor>:<vm type>:<storage type>")
	f.BoolVar(&sFlags.mbind, mbindFlag, true, "Enable mbind")
	f.BoolVar(&sFlags.overprovisioned, overprovisionedFlag, false, "Enable overprovisioning")
	f.BoolVar(&sFlags.unsafeBypassFsync, unsafeBypassFsyncFlag, false, "Enable unsafe-bypass-fsync")
	f.StringVar(&mode, modeFlag, "", "Mode sets well-known configuration properties for development or test environments; use --mode help for more info")

	f.DurationVar(&timeout, "timeout", 10000*time.Millisecond, "The maximum time to wait for the checks and tune processes to complete (e.g. 300ms, 1.5s, 2h45m)")
	for flag := range flagsMap(sFlags) {
		cmd.Flag(flag).Hidden = true
	}
	return cmd
}

func flagsMap(sFlags seastarFlags) map[string]interface{} {
	return map[string]interface{}{
		memoryFlag:            sFlags.memory,
		lockMemoryFlag:        sFlags.lockMemory,
		reserveMemoryFlag:     sFlags.reserveMemory,
		ioPropertiesFileFlag:  sFlags.ioPropertiesFile,
		ioPropertiesFlag:      sFlags.ioProperties,
		cpuSetFlag:            sFlags.cpuSet,
		smpFlag:               sFlags.smp,
		hugepagesFlag:         sFlags.hugepages,
		threadAffinityFlag:    sFlags.threadAffinity,
		numIoQueuesFlag:       sFlags.numIoQueues,
		maxIoRequestsFlag:     sFlags.maxIoRequests,
		mbindFlag:             sFlags.mbind,
		overprovisionedFlag:   sFlags.overprovisioned,
		unsafeBypassFsyncFlag: sFlags.unsafeBypassFsync,
	}
}

func prestart(
	fs afero.Fs,
	args *rp.RedpandaArgs,
	conf *config.Config,
	prestartCfg prestartConfig,
	timeout time.Duration,
) error {
	if prestartCfg.checkEnabled {
		err := check(fs, conf, timeout, checkFailedActions(args))
		if err != nil {
			return err
		}
		fmt.Println("System check - PASSED")
	}
	if prestartCfg.tuneEnabled {
		cpuset := fmt.Sprint(args.SeastarFlags[cpuSetFlag])
		err := tuneAll(fs, cpuset, conf, timeout)
		if err != nil {
			return err
		}
		fmt.Println("System tune - PASSED")
	}
	return nil
}

func buildRedpandaFlags(
	fs afero.Fs,
	conf *config.Config,
	args []string,
	sFlags seastarFlags,
	flags *pflag.FlagSet,
	skipChecks bool,
	ioResolver func(*config.Config, bool) (*iotune.IoProperties, error),
) (*rp.RedpandaArgs, error) {
	wellKnownIOSet := conf.Rpk.Tuners.WellKnownIo != ""
	ioPropsSet := flags.Changed(ioPropertiesFileFlag) || flags.Changed(ioPropertiesFlag)
	if wellKnownIOSet && ioPropsSet {
		return nil, errors.New(
			"--well-known-io or (rpk.well_known_io) and" +
				" --io-properties (or --io-properties-file)" +
				" can't be set at the same time",
		)
	}

	// We want to preserve the IOProps flags in case we find them either by
	// finding the file in the default location or by resolving to a well known
	// IO.
	preserve := make(map[string]bool, 2)
	if !ioPropsSet {
		// If --io-properties-file and --io-properties weren't set, try
		// finding an IO props file in the default location.
		sFlags.ioPropertiesFile = rp.GetIOConfigPath(filepath.Dir(conf.FileLocation()))
		preserve[ioPropertiesFileFlag] = true

		if exists, _ := afero.Exists(fs, sFlags.ioPropertiesFile); !exists {
			sFlags.ioPropertiesFile = ""
			preserve[ioPropertiesFileFlag] = false

			// If the file is not located in the default location either, we try
			// to deduce the IO props.
			ioProps, err := ioResolver(conf, skipChecks)
			if err != nil {
				zap.L().Sugar().Warn(err)
			} else if ioProps != nil {
				json, err := iotune.ToJSON(*ioProps)
				if err != nil {
					return nil, err
				}
				sFlags.ioProperties = json
				preserve[ioPropertiesFlag] = true
			}
		}
	}
	flagsMap := flagsMap(sFlags)
	for flag := range flagsMap {
		if !flags.Changed(flag) && !preserve[flag] {
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
				"configuration conflict. Flag '--%s'"+
					" is also present in"+
					" 'rpk.additional_start_flags' in"+
					" configuration file '%s'. Please"+
					" remove it and pass '--%s' directly"+
					" to `rpk start`",
				n,
				conf.FileLocation(),
				n,
			)
		}
		finalFlags[n] = fmt.Sprint(v)
	}
	return &rp.RedpandaArgs{
		ConfigFilePath: conf.FileLocation(),
		SeastarFlags:   finalFlags,
	}, nil
}

func flagsFromConf(
	conf *config.Config, flagsMap map[string]interface{}, flags *pflag.FlagSet,
) map[string]interface{} {
	flagsMap[overprovisionedFlag] = conf.Rpk.Tuners.Overprovisioned
	flagsMap[lockMemoryFlag] = conf.Rpk.Tuners.EnableMemoryLocking
	// Setting SMP to 0 doesn't make sense.
	if !flags.Changed(smpFlag) && conf.Rpk.Tuners.SMP != nil && *conf.Rpk.Tuners.SMP != 0 {
		flagsMap[smpFlag] = *conf.Rpk.Tuners.SMP
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

func setConfig(cfg *config.Config, configKvs []string) error {
	for _, rawKv := range configKvs {
		parts := strings.SplitN(rawKv, "=", 2)
		if len(parts) < 2 {
			return fmt.Errorf(
				"key-value pair '%s' is not formatted as expected (k=v)",
				rawKv,
			)
		}
		err := cfg.Set(parts[0], parts[1], "")
		if err != nil {
			return err
		}
	}
	return nil
}

func resolveWellKnownIo(
	conf *config.Config, skipChecks bool,
) (*iotune.IoProperties, error) {
	var ioProps *iotune.IoProperties
	if conf.Rpk.Tuners.WellKnownIo != "" {
		wellKnownIoTokens := strings.Split(conf.Rpk.Tuners.WellKnownIo, ":")
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
	fmt.Println("Detecting the current cloud vendor and VM")
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
) error {
	params := &factory.TunerParams{}
	tunerFactory := factory.NewDirectExecutorTunersFactory(fs, conf.Rpk.Tuners, timeout)
	hw := hwloc.NewHwLocCmd(vos.NewProc(), timeout)
	if cpuSet == "" {
		cpuMask, err := hw.All()
		if err != nil {
			return err
		}
		params.CPUMask = cpuMask
	} else {
		cpuMask, err := hwloc.TranslateToHwLocCPUSet(cpuSet)
		if err != nil {
			return err
		}
		params.CPUMask = cpuMask
	}

	err := factory.FillTunerParamsWithValuesFromConfig(params, conf)
	if err != nil {
		return err
	}

	availableTuners := factory.AvailableTuners()

	for _, tunerName := range availableTuners {
		enabled := factory.IsTunerEnabled(tunerName, conf.Rpk.Tuners)
		tuner := tunerFactory.CreateTuner(tunerName, params)
		supported, reason := tuner.CheckIfSupported()
		if !enabled {
			fmt.Printf("Skipping disabled tuner %s\n", tunerName)
			continue
		}
		if !supported {
			zap.L().Sugar().Debugf("Tuner '%s' is not supported - %s", tunerName, reason)
			continue
		}
		zap.L().Sugar().Debugf("Tuner parameters %+v", params)
		result := tuner.Tune()
		if result.IsFailed() {
			return result.Error()
		}
	}
	return nil
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
) error {
	results, err := tuners.Check(fs, conf, timeout)
	if err != nil {
		return err
	}
	for _, result := range results {
		if !result.IsOk {
			if action, exists := checkFailedActions[result.CheckerID]; exists {
				action(&result)
			}
			msg := fmt.Sprintf("System check '%s' failed. Required: %v, Current %v",
				result.Desc, result.Required, result.Current)
			if result.Severity == tuners.Fatal {
				return fmt.Errorf(msg)
			}
			fmt.Println(msg)
		}
	}
	return nil
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
			parsed[trimmed] = ""
			continue
		}

		// Check if the next element starts with a hyphen
		// If it does, it's another flag, and the current element is a
		// boolean flag
		next := flags[i+1]
		if strings.HasPrefix(next, "-") {
			parsed[trimmed] = ""
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
	defaultPort := config.DevDefault().Redpanda.RPCServer.Port
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
	return &config.SocketAddress{
		Address: named.Address,
		Port:    named.Port,
	}, nil
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
		Address: host,
		Port:    port,
		Name:    scheme,
	}, nil
}

func parseNamedAuthNAddresses(
	addrs []string, defaultPort int,
) ([]config.NamedAuthNSocketAddress, error) {
	as := make([]config.NamedAuthNSocketAddress, 0, len(addrs))
	for _, addr := range addrs {
		a, err := parseNamedAuthNAddress(addr, defaultPort)
		if err != nil {
			return nil, err
		}
		if a != nil {
			as = append(as, *a)
		}
	}
	return as, nil
}

func parseNamedAuthNAddress(
	addrAuthn string, defaultPort int,
) (*config.NamedAuthNSocketAddress, error) {
	if addrAuthn == "" {
		return nil, nil
	}
	addr, authn, err := splitAddressAuthN(addrAuthn)
	if err != nil {
		return nil, err
	}
	scheme, hostport, err := net.ParseHostMaybeScheme(addr)
	if err != nil {
		return nil, err
	}
	host, port := net.SplitHostPortDefault(hostport, defaultPort)

	return &config.NamedAuthNSocketAddress{
		Address: host,
		Port:    port,
		Name:    scheme,
		AuthN:   authn,
	}, nil
}

func splitAddressAuthN(str string) (addr string, authn *string, err error) {
	bits := strings.Split(str, "|")
	if len(bits) > 2 {
		err = fmt.Errorf(`invalid format for listener, at most one "|" can be present: %q`, str)
		return
	}
	addr = bits[0]
	if len(bits) == 2 {
		authn = &bits[1]
	}
	return
}

func stringOr(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

func stringSliceOr(a, b []string) []string {
	if len(a) != 0 {
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

// setContainerModeFlags sets flags bundled into --mode dev-container flag.
func setContainerModeFlags(cmd *cobra.Command) {
	devMap := map[string]string{
		overprovisionedFlag:   "true",
		reserveMemoryFlag:     "0M",
		checkFlag:             "false",
		unsafeBypassFsyncFlag: "true",
	}
	// We don't override the values set during command execution, e.g:
	//   rpk redpanda start --mode dev-container --smp 2
	// will apply all dev flags, but smp will be 2.
	for k, v := range devMap {
		if !cmd.Flags().Changed(k) {
			cmd.Flags().Set(k, v)
		}
	}
}

func setContainerModeCfgFields(cfg *config.Config) {
	cfg.Redpanda.DeveloperMode = true

	// cluster properties:
	if cfg.Redpanda.Other == nil {
		cfg.Redpanda.Other = make(map[string]interface{})
	}
	cfg.Redpanda.Other["auto_create_topics_enabled"] = true
	cfg.Redpanda.Other["group_topic_partitions"] = 3
	cfg.Redpanda.Other["storage_min_free_bytes"] = 10485760
	cfg.Redpanda.Other["topic_partitions_per_shard"] = 1000
	cfg.Redpanda.Other["fetch_reads_debounce_timeout"] = 10
	cfg.Redpanda.Other["group_initial_rebalance_delay"] = 0
	cfg.Redpanda.Other["log_segment_size_min"] = 1
}

func getOrFindInstallDir(fs afero.Fs, installDir string) (string, error) {
	if installDir != "" {
		return installDir, nil
	}
	foundConfig, err := rp.FindInstallDir(fs)
	if err != nil {
		return "", fmt.Errorf("unable to find redpanda installation. Please provide the install directory with flag --install-dir")
	}
	return foundConfig, nil
}

const helpMode = `Mode uses well-known configuration properties for development or tests 
environments:

--mode dev-container
    Bundled flags:
        * --overprovisioned
        * --reserve-memory 0M
        * --check=false
        * --unsafe-bypass-fsync
    Bundled cluster properties:
        * auto_create_topics_enabled: true
        * group_topic_partitions: 3
        * storage_min_free_bytes: 10485760 (10MiB)
        * topic_partitions_per_shard: 1000
        * fetch_reads_debounce_timeout: 10
        * group_initial_rebalance_delay: 0
        * log_segment_size_min: 1

After redpanda starts you can modify the cluster properties using:
    rpk config set <key> <value>`
