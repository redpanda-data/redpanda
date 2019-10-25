package cmd

import (
	"errors"
	"fmt"
	"strings"
	"time"
	"vectorized/pkg/cli"
	"vectorized/pkg/redpanda"
	"vectorized/pkg/tuners/factory"
	"vectorized/pkg/tuners/hwloc"
	"vectorized/pkg/utils"

	"github.com/fatih/color"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func NewTuneCommand(fs afero.Fs) *cobra.Command {

	tunerParams := factory.TunerParams{}
	var redpandaConfigFile string
	var outTuneScriptFile string
	var cpuSet string
	var timeoutMs int
	command := &cobra.Command{
		Use: "tune <list_of_elements_to_tune>",
		Short: `Sets the OS parameters to tune system performance
		available tuners: all, ` + fmt.Sprintf("%#q", factory.AvailableTuners()),
		Long: "In order to get more information about the tuner run: " +
			"rpk tune <tuner_name> --help",
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
		RunE: func(ccmd *cobra.Command, args []string) error {
			var tunerFactory factory.TunersFactory
			timeout := time.Duration(timeoutMs) * time.Millisecond
			if outTuneScriptFile != "" {
				tunerFactory = factory.NewScriptRenderingTunersFactory(
					fs, outTuneScriptFile, timeout)
			} else {
				tunerFactory = factory.NewDirectExecutorTunersFactory(fs, timeout)
			}
			if !tunerParamsEmpty(&tunerParams) && redpandaConfigFile != "" {
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
			return tune(fs, tuners, tunerFactory, &tunerParams, redpandaConfigFile)
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
	command.Flags().StringVar(&redpandaConfigFile,
		"redpanda-cfg", "", "If set, pointed redpanda config file will be used "+
			"to populate tuner parameters")
	command.Flags().StringVar(&outTuneScriptFile,
		"output-script", "", "If set tuners will generate tuning file that "+
			"can later be used to tune the system")
	command.Flags().IntVar(&timeoutMs, "timeout", 10000, "The maximum amount of time (in ms) to wait for the tune processes to complete")
	command.AddCommand(newHelpCommand())
	return command
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
			if _, contains := tunersHelp[tuner]; !contains {
				return fmt.Errorf("invalid tuner name '%s' "+
					"only %s are supported",
					tuner, utils.GetKeysFromStringMap(tunersHelp))
			}
			return nil
		},
		Run: func(ccmd *cobra.Command, args []string) {
			tuner := args[0]
			fmt.Printf("'%s' tuner descrtiption", tuner)
			fmt.Println()
			fmt.Print(tunersHelp[tuner])
		},
	}
}

func tune(
	fs afero.Fs,
	elementsToTune []string,
	tunersFactory factory.TunersFactory,
	params *factory.TunerParams,
	redpandaConfigFile string,
) error {

	if tunerParamsEmpty(params) {
		configFile, err := cli.GetOrFindConfig(fs, redpandaConfigFile)
		if err != nil {
			return err
		}
		config, err := redpanda.ReadConfigFromPath(fs, configFile)
		if err != nil {
			return err
		}
		log.Infof("Tuning using redpanda config file '%s'", configFile)
		err = factory.FillTunerParamsWithValuesFromConfig(params, config)
		if err != nil {
			return err
		}
	}
	var rebootRequired = false
	for _, tunerName := range elementsToTune {
		tuner := tunersFactory.CreateTuner(tunerName, params)
		if supported, reason := tuner.CheckIfSupported(); supported == true {
			log.Debugf("Tuner paramters %+v", params)
			log.Infof("Running '%s' tuner...", tunerName)
			result := tuner.Tune()
			if result.IsFailed() {
				return result.GetError()
			}
			if result.IsRebootRequired() {
				rebootRequired = true
			}
		} else {
			log.Infof("Tuner '%s' is not supported - %s", tunerName, reason)
		}
	}
	if rebootRequired {
		red := color.New(color.FgRed).SprintFunc()
		log.Infof("%s: Reboot system and run 'rpk tune <tuner>' again",
			red("IMPORTANT"))
	}
	return nil
}

func tunerParamsEmpty(params *factory.TunerParams) bool {
	return len(params.Directories) == 0 &&
		len(params.Disks) == 0 &&
		len(params.Nics) == 0
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
