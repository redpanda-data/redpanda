package cmd

import (
	"errors"
	"fmt"
	"strings"
	"vectorized/tuners/factory"
	"vectorized/utils"

	"github.com/fatih/color"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func NewTuneCommand() *cobra.Command {
	fs := afero.OsFs{}
	tunerFactory := factory.NewTunersFactory(fs)
	tunerParams := factory.TunerParams{}
	command := &cobra.Command{
		Use: "tune <list_of_elements_to_tune>",
		Short: `Sets the OS parameters to tune system performance
		available tuners: ` + fmt.Sprintf("%#q", tunerFactory.AvailableTuners()),
		Long: "In order to get more information about the tuner run: " +
			"rpk tune <tuner_name> --help",
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) < 1 {
				return errors.New("requires the list of elements to tune")
			}
			for _, toTune := range strings.Split(args[0], ",") {
				if !tunerFactory.IsTunerAvailable(toTune) {
					return fmt.Errorf("invalid element to tune '%s' "+
						"only %s are supported",
						args[0], tunerFactory.AvailableTuners())
				}
			}
			return nil
		},
		RunE: func(ccmd *cobra.Command, args []string) error {
			return tune(strings.Split(args[0], ","), tunerFactory, &tunerParams)
		},
	}
	command.Flags().StringVarP(&tunerParams.Mode,
		"mode", "m", "",
		"Operation Mode: one of: [sq, sq_split, mq]")
	command.Flags().StringVarP(&tunerParams.CpuMask,
		"cpu-mask", "c",
		"all", "CPU Mask (32-bit hexadecimal integer))")
	command.Flags().StringSliceVarP(&tunerParams.Disks,
		"disks", "d",
		[]string{}, "Lists of devices to tune f.e. 'sda1'")
	command.Flags().StringVarP(&tunerParams.Nic,
		"nic", "n",
		"eth0", "Network Interface Controller to tune")
	command.Flags().StringSliceVarP(&tunerParams.Directories,
		"dirs", "r",
		[]string{}, "List of *data* directories. or places to store data."+
			" i.e.: '/var/vectorized/redpanda/',"+
			" usually your XFS filesystem on an NVMe SSD device")
	command.Flags().BoolVar(&tunerParams.RebootAllowed,
		"reboot-allowed", false, "If set will allow tuners to tune boot paramters "+
			" and request system reboot")
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
	elementsToTune []string,
	factory factory.TunersFactory,
	params *factory.TunerParams,
) error {
	var rebootRequired = false
	for _, tunerName := range elementsToTune {
		tuner := factory.CreateTuner(tunerName, params)
		if supported, reason := tuner.CheckIfSupported(); supported == true {
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
