package cmd

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
	"vectorized/pkg/cli/ui"
	"vectorized/pkg/config"
	"vectorized/pkg/tuners/factory"
	"vectorized/pkg/tuners/hwloc"
	"vectorized/pkg/utils"

	"github.com/fatih/color"
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

func NewTuneCommand(fs afero.Fs) *cobra.Command {
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
			" tuners, run `rpk tune help <tuner name>`",
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
			cpuMask, err := hwloc.TranslateToHwLocCpuSet(cpuSet)
			if err != nil {
				return err
			}
			tunerParams.CpuMask = cpuMask
			conf, err := config.ReadOrGenerate(fs, configFile)
			if err != nil {
				if !interactive {
					return err
				}
				msg := fmt.Sprintf(
					`Couldn't read or generate the config at %s.
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
				defaultConf := config.DefaultConfig()
				conf = &defaultConf
			}
			config.CheckAndPrintNotice(conf.LicenseKey)
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
		[]string{}, "List of *data* directories. or places to store data."+
			" i.e.: '/var/vectorized/redpanda/',"+
			" usually your XFS filesystem on an NVMe SSD device")
	command.Flags().BoolVar(&tunerParams.RebootAllowed,
		"reboot-allowed", false, "If set will allow tuners to tune boot paramters "+
			" and request system reboot")
	command.Flags().StringVar(
		&configFile,
		"config",
		config.DefaultConfig().ConfigFile,
		"Redpanda config file, if not set the file will be searched for"+
			" in the default locations",
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
	command.AddCommand(newHelpCommand())
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

func newHelpCommand() *cobra.Command {
	tunersHelp := map[string]string{
		"cpu":        cpuTunerHelp,
		"disk_irq":   diskIrqTunerHelp,
		"disk_sched": diskSchedulerTunerHelp,
		"net":        netTunerHelp,
	}

	return &cobra.Command{
		Use:   "help <tuner>",
		Short: "Display detailed infromation about the tuner",
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return errors.New("requires the tuner name")
			}
			tuner := args[0]
			tunerList := strings.Join(
				utils.GetKeysFromStringMap(tunersHelp),
				", ",
			)
			if _, contains := tunersHelp[tuner]; !contains {
				return fmt.Errorf(
					"No help found for tuner '%s'."+
						" Available: %s.",
					tuner,
					tunerList,
				)
			}
			return nil
		},
		Run: func(ccmd *cobra.Command, args []string) {
			tuner := args[0]
			fmt.Printf("'%s' tuner description", tuner)
			fmt.Println()
			fmt.Print(tunersHelp[tuner])
		},
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
	for _, tunerName := range tunerNames {
		enabled := factory.IsTunerEnabled(tunerName, conf.Rpk)
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

const cpuTunerHelp = `
This tuner optimizes CPU settings to achieve best performance
in I/O intensive workloads. It sets the GRUB kernel boot options and CPU governor.
After this tuner execution system CPUs will operate with maximum
'non turbo' frequency.

This tuner performs the following operations:

- Disable Hyper Threading
- Sets the ACPI-cpufreq governor to ‘performance’

Additionaly if system reboot is allowed:
- Disable Hyper Threading via Kernel boot parameter
- Disable Intel P-States
- Disable Intel C-States
- Disable Turbo Boost

Important: If '--reboot-allowed' flag is passed as an option, it is required
to reboot the system after first pass of this tuner`

const netTunerHelp = `
This tuner distributes the NIC IRQs and queues according to the specified mode.
For the RPS and IRQs distribution the tuner uses only those CPUs that
are allowed by the provided CPU masks.

This tuner performs the following operations:

	- Setup NIC IRQs affinity
	- Setup NIC RPS and RFS
	- Setup NIC XPS
	- Increase socket listen backlog
	- Increase number of remembered connection requests
	- Ban the IRQ Balance service from moving distributed IRQs

Modes description:

	sq - set all IRQs of a given NIC to CPU0 and configure RPS
		to spreads NAPIs' handling between other CPUs.

	sq_split - divide all IRQs of a given NIC between CPU0 and its HT siblings
			and configure RPS to spread NAPIs' handling between other CPUs.

	mq - distribute NIC's IRQs among all CPUs instead of binding
		them all to CPU0. In this mode RPS is always enabled to
		spread NAPIs' handling between all CPUs.

If there isn't any mode given script will use a default mode:

	- If number of physical CPU cores per Rx HW queue
	  is greater than 4 - use the 'sq-split' mode.
	- Otherwise, if number of hyper-threads per Rx HW queue
	  is greater than 4 - use the 'sq' mode.
	- Otherwise use the 'mq' mode.`

const diskSchedulerTunerHelp = `
This tuner sets the preferred I/O scheduler for given block devices and disables
I/O operation merging. It can work using both the device name or a directory,
then the device where directory is stored will be optimized.
The tuner sets either ‘none’ or ‘noop’ scheduler if supported.

Schedulers:

	none - used with modern NVMe devices to
	   	   bypass OS I/O scheduler and minimize latency
	noop - used when ‘none’ is not available,
		   it is preferred for non-NVME devices, this scheduler uses simple FIFO
		   queue where all I/O operations are first stored
		   and then handled by the driver`

const diskIrqTunerHelp = `
This tuner distributes block devices IRQs according to the specified mode.
It can work using both the device name or a directory,
then the device where directory is stored will be optimized.
For the IRQs distribution the tuner uses only those CPUs that are allowed by
the provided CPU masks. IRQs of non-NVMe devices are distributed across the
cores that are calculated based on the mode provided,
NVMe devices IRQs are distributed across all available cores allowed by CPU mask.

This tuner performs the following operations:
	- Setup disks IRQs affinity
	- Ban the IRQ Balance service from moving distributed IRQs

Modes description:

	sq - set all IRQs of a given non-NVME devices to CPU0,
		NVME devices IRQs are distributed across all available CPUs

	sq_split - divide all IRQs of a non-NVME devices between CPU0
			and its HT siblings, NVME devices IRQs are distributed
			across all available CPUs

	mq - distribute all devices IRQs across all available CPUs

If there isn't any mode given script will use a default mode:

	- If there are no non-NVME devices use ‘mq’ mode
	- Otherwise, if number of hyper-threads
	  is lower than 4 - use the ‘mq’ mode
	- Otherwise, if number of physical CPU cores
	  is lower than 4 - use the 'sq' mode.
	- Otherwise use the ‘sq-split’ mode.`
