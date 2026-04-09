// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

//go:build linux

package redpanda

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cloud"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/netutil"
	vos "github.com/redpanda-data/redpanda/src/go/rpk/pkg/osutil"
	rp "github.com/redpanda-data/redpanda/src/go/rpk/pkg/redpanda"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/factory"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/hwloc"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/iotune"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/network"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
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

func updateConfigWithFlags(y *config.RedpandaYaml, flags *pflag.FlagSet) {
	if flags.Changed(lockMemoryFlag) {
		y.Rpk.EnableMemoryLocking, _ = flags.GetBool(lockMemoryFlag)
	}
	if flags.Changed(wellKnownIOFlag) {
		y.Rpk.Tuners.WellKnownIo, _ = flags.GetString(wellKnownIOFlag)
	}
	if flags.Changed(overprovisionedFlag) {
		y.Rpk.Overprovisioned, _ = flags.GetBool(overprovisionedFlag)
	}
	if flags.Changed(nodeIDFlag) {
		y.Redpanda.ID = new(int)
		*y.Redpanda.ID, _ = flags.GetInt(nodeIDFlag)
	}
}

func parseConfigKvs(args []string) (parsedKVs []string, parsedArgs []string) {
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
	nodeTunerStatePath := ""
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
			y := cfg.ActualRedpandaYamlOrDefaults()
			if y.Redpanda.DeveloperMode && len(mode) == 0 {
				mode = "dev-container"
			}
			switch mode {
			case "dev-container":
				fmt.Fprintln(os.Stderr, "WARNING: This is a setup for development purposes only; in this mode your clusters may run unrealistically fast and data can be corrupted any time your computer shuts down uncleanly.")
				setContainerModeFlags(cmd)
				setContainerModeCfgFields(y)
			case "help":
				fmt.Println(helpMode)
				return nil
			case "":
				// do nothing.
			default:
				return fmt.Errorf("unrecognized mode %q; use --mode help for more info", mode)
			}

			if len(configKvs) > 0 {
				if err = setConfig(y, configKvs); err != nil {
					return err
				}
			}

			updateConfigWithFlags(y, cmd.Flags())

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
				y.Redpanda.SeedServers = seedServers
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
				y.Redpanda.KafkaAPI = kafkaAPI
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
				if y.Pandaproxy == nil {
					y.Pandaproxy = config.DevDefault().Pandaproxy
				}
				y.Pandaproxy.PandaproxyAPI = proxyAPI
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
				if y.SchemaRegistry == nil {
					y.SchemaRegistry = config.DevDefault().SchemaRegistry
				}
				y.SchemaRegistry.SchemaRegistryAPI = schemaRegAPI
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
				y.Redpanda.RPCServer = *rpcServer
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
				y.Redpanda.AdvertisedKafkaAPI = advKafkaAPI
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
				if y.Pandaproxy == nil {
					y.Pandaproxy = config.DevDefault().Pandaproxy
				}
				y.Pandaproxy.AdvertisedPandaproxyAPI = advProxyAPI
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
				y.Redpanda.AdvertisedRPCAPI = advRPCApi
			}
			installDirectory, err := getOrFindInstallDir(fs, installDirFlag)
			if err != nil {
				return err
			}
			rpArgs, err := buildRedpandaFlags(
				fs,
				y,
				filteredArgs,
				sFlags,
				cmd.Flags(),
				!prestartCfg.checkEnabled,
				resolveWellKnownIo,
				nodeTunerStatePath,
			)
			if err != nil {
				return err
			}

			if y.Redpanda.Directory == "" {
				y.Redpanda.Directory = config.DevDefault().Redpanda.Directory
			}

			err = prestart(fs, rpArgs, y, prestartCfg, timeout)
			if err != nil {
				return err
			}

			err = y.Write(fs)
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
	f.StringVar(&wellKnownIo, wellKnownIOFlag, "", "The cloud provider and VM type, in the format <provider>:<vm type>:<storage type>")
	f.BoolVar(&sFlags.mbind, mbindFlag, true, "Enable mbind")
	f.BoolVar(&sFlags.overprovisioned, overprovisionedFlag, false, "Enable overprovisioning")
	f.BoolVar(&sFlags.unsafeBypassFsync, unsafeBypassFsyncFlag, false, "Enable unsafe-bypass-fsync")
	f.StringVar(&mode, modeFlag, "", "Mode sets well-known configuration properties for development or test environments; use --mode help for more info")
	f.StringVar(&nodeTunerStatePath, "node-tuner-state-path", "", "Alternative path to read the node tuner state file from (if exists)")

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
	y *config.RedpandaYaml,
	prestartCfg prestartConfig,
	timeout time.Duration,
) error {
	if prestartCfg.checkEnabled {
		fmt.Println("System check - STARTED")
		err := check(fs, y, timeout, checkFailedActions(args))
		if err != nil {
			return fmt.Errorf("unable to check if system meets redpanda requirements: %v; to override this check you can use --check=false", err)
		}
		fmt.Println("System check - PASSED")
	}
	if prestartCfg.tuneEnabled {
		fmt.Println("System tune - STARTED")
		cpuset := fmt.Sprint(args.SeastarFlags[cpuSetFlag])
		err := tuneAll(fs, cpuset, y, timeout)
		if err != nil {
			return fmt.Errorf("unable to tune your system: %v; to avoid tuning your machine you can use --tune=false", err)
		}
		fmt.Println("System tune - PASSED")
	}
	return nil
}

// Tries to read the tuner config file and extract the RedpandaCpuset from it.
// Returns empty string in case no cpuset/file was found.
func readTunerConfigCpuset(fs afero.Fs, configFilePath string) (*tuners.CpusetConfig, error) {
	filePath := network.DefaultNodeTunerStateFile
	if configFilePath != "" {
		filePath = configFilePath
	}
	exists, err := afero.Exists(fs, filePath)
	if err != nil {
		return nil, err
	}

	if !exists {
		if filePath != network.DefaultNodeTunerStateFile {
			return nil, fmt.Errorf("--tuner-config-path specified but file %s not found", filePath)
		}
		return nil, nil
	}

	content, err := afero.ReadFile(fs, filePath)
	if err != nil {
		return nil, err
	}

	// We allow an empty file to mean no cpuset. k8s will create an empty file
	// when mounting in FileOrCreate mode and no file on the host exists.
	if len(content) == 0 {
		fmt.Println("Net tuner config file found but empty")
		return nil, nil
	}

	config := tuners.NodeTunerState{}
	err = yaml.Unmarshal(content, &config)
	if err != nil {
		return nil, err
	}

	if config.Cpusets == nil {
		fmt.Println("Net tuner config file found but no cpusets (v1) section")
		return nil, nil
	}

	return config.Cpusets, nil
}

func checkTunerConfigCpusetCompatibilityAndUpdateFlags(
	finalFlags map[string]string,
	cpusets *tuners.CpusetConfig,
) error {
	if finalFlags[cpuSetFlag] != "" {
		return errors.New(
			"cpuset is set both via --cpuset flag and via tuner config file which is incompatible; " +
				"either use MQ tuner mode or remove the cpuset flag from additional_start_flags",
		)
	}

	finalFlags[cpuSetFlag] = cpusets.RedpandaCpuset

	if finalFlags[smpFlag] != "" {
		wantSmp, err := strconv.Atoi(finalFlags[smpFlag])
		if err != nil {
			return fmt.Errorf("unable to parse smp flag value %q: %w", finalFlags[smpFlag], err)
		}

		// We are lenient in regards to --smp in rpk:start, the system is already tuned so we just need to adapt.
		// All the hard checks are already done on the tuner side.

		if wantSmp > cpusets.RedpandaCpusetSize {
			// if the user has --smp configured larger than the cpuset size we
			// are just lowering it. Otherwise seastar will fail to start. We
			// know this is the optimal config we have tuned for anyway. This
			// scenario is likely in cases where people hardcode --smp=N on an N
			// core machine (the chart helm does this for example).

			fmt.Printf("Lowering smp from %d to tuner config cpuset size %d\n", wantSmp, cpusets.RedpandaCpusetSize)
			finalFlags[smpFlag] = strconv.Itoa(cpusets.RedpandaCpusetSize)
		}
	}

	return nil
}

func buildRedpandaFlags(
	fs afero.Fs,
	y *config.RedpandaYaml,
	args []string,
	sFlags seastarFlags,
	flags *pflag.FlagSet,
	skipChecks bool,
	ioResolver func(*config.RedpandaYaml, bool) (*iotune.IoProperties, error),
	nodeTunerStatePath string,
) (*rp.RedpandaArgs, error) {
	wellKnownIOSet := y.Rpk.Tuners.WellKnownIo != ""
	ioPropsSet := flags.Changed(ioPropertiesFileFlag) || flags.Changed(ioPropertiesFlag)
	if wellKnownIOSet && ioPropsSet {
		return nil, errors.New(
			"--well-known-io or (rpk.well_known_io) and" +
				" --io-properties (or --io-properties-file)" +
				" can't be set at the same time",
		)
	}

	preserve := make(map[string]bool, 1)

	// We want to preserve the IOProps flags in case we find them either by
	// finding the file in the default location or by resolving to a well known
	// IO.
	if !ioPropsSet {
		// If --io-properties-file and --io-properties weren't set, try
		// finding an IO props file in the default location.
		sFlags.ioPropertiesFile = rp.GetIOConfigPath(filepath.Dir(y.FileLocation()))
		preserve[ioPropertiesFileFlag] = true

		if exists, _ := afero.Exists(fs, sFlags.ioPropertiesFile); !exists {
			sFlags.ioPropertiesFile = ""
			preserve[ioPropertiesFileFlag] = false

			// If the file is not located in the default location either, we try
			// to deduce the IO props.
			ioProps, err := ioResolver(y, skipChecks)
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
	flagsMap = flagsFromConf(y, flagsMap, flags)
	finalFlags := mergeMaps(
		config.ParseAdditionalStartFlags(y.Rpk.AdditionalStartFlags),
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
				y.FileLocation(),
				n,
			)
		}
		finalFlags[n] = fmt.Sprint(v)
	}

	// Check if tuner config file exists and read redpanda cpuset from it
	tunerConfigCpuset, err := readTunerConfigCpuset(fs, nodeTunerStatePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read tuner config file: %w", err)
	}

	if tunerConfigCpuset != nil {
		err := checkTunerConfigCpusetCompatibilityAndUpdateFlags(finalFlags, tunerConfigCpuset)
		if err != nil {
			return nil, err
		}
		fmt.Printf("Using cpuset from tuner config: %s\n", tunerConfigCpuset.RedpandaCpuset)
	}

	return &rp.RedpandaArgs{
		ConfigFilePath: y.FileLocation(),
		SeastarFlags:   finalFlags,
	}, nil
}

func flagsFromConf(
	y *config.RedpandaYaml, flagsMap map[string]interface{}, flags *pflag.FlagSet,
) map[string]interface{} {
	flagsMap[overprovisionedFlag] = y.Rpk.Overprovisioned
	flagsMap[lockMemoryFlag] = y.Rpk.EnableMemoryLocking
	// Setting SMP to 0 doesn't make sense.
	if !flags.Changed(smpFlag) && y.Rpk.SMP != nil && *y.Rpk.SMP != 0 {
		flagsMap[smpFlag] = *y.Rpk.SMP
	}
	return flagsMap
}

func setConfig(y *config.RedpandaYaml, configKvs []string) error {
	for _, rawKv := range configKvs {
		parts := strings.SplitN(rawKv, "=", 2)
		if len(parts) < 2 {
			return fmt.Errorf(
				"key-value pair '%s' is not formatted as expected (k=v)",
				rawKv,
			)
		}
		err := config.Set(y, parts[0], parts[1])
		if err != nil {
			return err
		}
	}
	return nil
}

func resolveWellKnownIo(
	y *config.RedpandaYaml, skipChecks bool,
) (*iotune.IoProperties, error) {
	var ioProps *iotune.IoProperties
	if y.Rpk.Tuners.WellKnownIo != "" {
		wellKnownIoTokens := strings.Split(y.Rpk.Tuners.WellKnownIo, ":")
		if len(wellKnownIoTokens) != 3 {
			err := errors.New(
				"--well-known-io should have the format '<provider>:<vm type>:<storage type>'",
			)
			return nil, err
		}
		ioProps, err := iotune.DataFor(
			y.Redpanda.Directory,
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
	// Skip detecting the cloud provider if skipChecks is true
	if skipChecks {
		return nil, nil
	}
	fmt.Println("Detecting the current cloud provider and VM")
	provider, err := cloud.AvailableProviders()
	if err != nil {
		return nil, errors.New("could not detect the current cloud provider")
	}
	ioProps, err = iotune.DataForProvider(y.Redpanda.Directory, provider)
	if err != nil {
		// Log the error to let the user know that the data wasn't found
		return nil, err
	}
	return ioProps, nil
}

func tuneAll(
	fs afero.Fs, cpuSet string, y *config.RedpandaYaml, timeout time.Duration,
) error {
	params := &factory.TunerParams{}
	tunerFactory := factory.NewDirectExecutorTunersFactory(fs, y.Rpk, timeout)
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

	err := factory.FillTunerParamsWithValuesFromConfig(params, y)
	if err != nil {
		return err
	}

	availableTuners := factory.AvailableTuners()

	for _, tunerName := range availableTuners {
		enabled := factory.IsTunerEnabled(tunerName, y.Rpk.Tuners)
		tuner := tunerFactory.CreateTuner(tunerName, params)
		supported, reason := tuner.CheckIfSupported()
		if !enabled {
			fmt.Printf("Skipping disabled tuner %s\n", tunerName)
			continue
		}
		if !supported {
			zap.L().Sugar().Debugf("Tuner %q is not supported - %s", tunerName, reason)
			continue
		}
		zap.L().Sugar().Debugf("Tuner parameters %+v", params)
		result := tuner.Tune()
		if result.IsFailed() {
			return fmt.Errorf("failed to tune %q: %v", tunerName, result.Error())
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
	y *config.RedpandaYaml,
	timeout time.Duration,
	checkFailedActions map[tuners.CheckerID]checkFailedAction,
) error {
	results, err := tuners.Check(fs, y, timeout)
	if err != nil {
		return err
	}
	for _, result := range results {
		if !result.IsOk {
			if action, exists := checkFailedActions[result.CheckerID]; exists {
				action(&result)
			}
			msg := fmt.Sprintf("System check '%s' failed. Required: %v, Current %v, Error: %v, Severity: %v",
				result.Desc, result.Required, result.Current, result.Err, result.Severity)
			if result.Severity == tuners.Fatal {
				return errors.New(msg)
			}
			zap.L().Sugar().Warn(msg)
		}
	}
	return nil
}

func parseSeeds(seeds []string) ([]config.SeedServer, error) {
	seedServers := []config.SeedServer{}
	defaultPort := config.DevDefault().Redpanda.RPCServer.Port
	for _, s := range seeds {
		addr, err := parseAddress(s, defaultPort)
		if err != nil {
			return seedServers, fmt.Errorf(
				"unable to parse seed '%s': %v",
				s,
				err,
			)
		}
		if addr == nil {
			return seedServers, fmt.Errorf(
				"unable to parse seed '%s': empty address",
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
	scheme, hostport, err := netutil.ParseHostMaybeScheme(addr)
	if err != nil {
		return nil, err
	}
	host, port := netutil.SplitHostPortDefault(hostport, defaultPort)

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
	scheme, hostport, err := netutil.ParseHostMaybeScheme(addr)
	if err != nil {
		return nil, err
	}
	host, port := netutil.SplitHostPortDefault(hostport, defaultPort)

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
	allFlagsMap := config.ParseAdditionalStartFlags(args)
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

func setContainerModeCfgFields(y *config.RedpandaYaml) {
	y.Redpanda.DeveloperMode = true

	// cluster properties:
	if y.Redpanda.Other == nil {
		y.Redpanda.Other = make(map[string]interface{})
	}
	y.Redpanda.Other["auto_create_topics_enabled"] = true
	y.Redpanda.Other["group_topic_partitions"] = 3
	y.Redpanda.Other["storage_min_free_bytes"] = 10485760
	y.Redpanda.Other["topic_partitions_per_shard"] = 1000
	y.Redpanda.Other["fetch_reads_debounce_timeout"] = 10
	y.Redpanda.Other["group_initial_rebalance_delay"] = 0
	y.Redpanda.Other["log_segment_size_min"] = 1
	y.Redpanda.Other["write_caching_default"] = "true"
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
        * write_caching_default: true

After redpanda starts you can modify the cluster properties using:
    rpk config set <key> <value>`
