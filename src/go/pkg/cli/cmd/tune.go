package cmd

import (
	"errors"
	"fmt"
	"runtime"
	"strings"
	"vectorized/os"
	"vectorized/tuners"
	"vectorized/tuners/cpu"
	"vectorized/tuners/disk"
	"vectorized/tuners/hwloc"
	"vectorized/tuners/irq"
	"vectorized/tuners/network"
	"vectorized/utils"

	"github.com/safchain/ethtool"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

type tunerParams struct {
	mode         string
	cpuMask      string
	thingsToTune []string
	disks        []string
	directories  []string
	nic          string
}

type tunersFactory struct {
	fs                afero.Fs
	irqDeviceInfo     irq.DeviceInfo
	cpuMasks          irq.CpuMasks
	irqBalanceService irq.BalanceService
	irqProcFile       irq.ProcFile
	diskInfoProvider  disk.InfoProvider
	proc              os.Proc
	grub              os.Grub
	tuners            map[string]func(*tunersFactory, *tunerParams) tuners.Tunable
}

func NewTuneCommand() *cobra.Command {

	fs := afero.OsFs{}
	irqProcFile := irq.NewProcFile(fs)
	proc := os.NewProc()
	tunerFactory := tunersFactory{
		tuners: map[string]func(*tunersFactory, *tunerParams) tuners.Tunable{
			"disk_irq":   (*tunersFactory).newDiskIrqTuner,
			"disk_sched": (*tunersFactory).newDiskSchedulerTuner,
			"net":        (*tunersFactory).newNetworkTuner,
			"cpu":        (*tunersFactory).newCpuTuner,
		},
		fs:                fs,
		irqProcFile:       irqProcFile,
		irqDeviceInfo:     irq.NewDeviceInfo(fs, irqProcFile),
		cpuMasks:          irq.NewCpuMasks(fs, hwloc.NewHwLocCmd(proc)),
		irqBalanceService: irq.NewBalanceService(fs, proc),
		diskInfoProvider:  disk.NewDiskInfoProvider(fs, proc),
		grub:              os.NewGrub(os.NewCommands(proc), proc, fs),
		proc:              proc,
	}
	tunerParams := tunerParams{}
	command := &cobra.Command{
		Use: "tune <list_of_elements_to_tune>",
		Short: `Sets the OS parameters to tune system performance
		available tuners: ` + fmt.Sprintf("%#q", tunerFactory.getTunerNames()),
		Long: "In order to get more information about the tuner run: " +
			"rpk tune <tuner_name> --help",
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) < 1 {
				return errors.New("requires the list of elements to tune")
			}
			for _, toTune := range strings.Split(args[0], ",") {
				if tunerFactory.tuners[toTune] == nil {
					return fmt.Errorf("invalid element to tune '%s' "+
						"only %s are supported",
						args[0], tunerFactory.getTunerNames())
				}
			}
			return nil
		},
		RunE: func(ccmd *cobra.Command, args []string) error {
			return tune(strings.Split(args[0], ","), &tunerFactory, &tunerParams)
		},
	}
	command.Flags().StringVarP(&tunerParams.mode,
		"mode", "m", "",
		"Operation Mode: one of: [sq, sq_split, mq]")
	command.Flags().StringVarP(&tunerParams.cpuMask,
		"cpu-mask", "c",
		"all", "CPU Mask (32-bit hexadecimal integer))")
	command.Flags().StringSliceVarP(&tunerParams.disks,
		"disks", "d",
		[]string{}, "Lists of devices to tune f.e. 'sda1'")
	command.Flags().StringVarP(&tunerParams.nic,
		"nic", "n",
		"eth0", "Network Interface Controller to tune")
	command.Flags().StringSliceVarP(&tunerParams.directories,
		"dirs", "r",
		[]string{}, "List of *data* directories. or places to store data."+
			" i.e.: '/var/vectorized/redpanda/',"+
			" usually your XFS filesystem on an NVMe SSD device")
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

func (factory *tunersFactory) getTunerNames() []string {
	var keys []string
	for key := range factory.tuners {
		keys = append(keys, key)
	}
	return keys
}

func (factory *tunersFactory) newTuner(
	tunerName string, tunerParams *tunerParams,
) tuners.Tunable {
	return factory.tuners[tunerName](factory, tunerParams)
}

func (factory *tunersFactory) newDiskIrqTuner(
	params *tunerParams,
) tuners.Tunable {

	return disk.NewDiskIrqTuner(
		irq.ModeFromString(params.mode),
		params.cpuMask,
		params.directories,
		params.disks,
		factory.irqDeviceInfo,
		factory.cpuMasks,
		factory.irqBalanceService,
		factory.irqProcFile,
		factory.diskInfoProvider,
		runtime.NumCPU(),
	)
}

func (factory *tunersFactory) newDiskSchedulerTuner(
	params *tunerParams,
) tuners.Tunable {
	return disk.NewSchedulerTuner(
		params.directories,
		params.disks,
		factory.diskInfoProvider,
		factory.fs)
}

func (factory *tunersFactory) newNetworkTuner(
	params *tunerParams,
) tuners.Tunable {
	return network.NewNetTuner(
		irq.ModeFromString(params.mode),
		params.cpuMask,
		params.nic,
		factory.fs,
		factory.irqDeviceInfo,
		factory.cpuMasks,
		factory.irqBalanceService,
		factory.irqProcFile,
		&ethtool.Ethtool{},
	)
}

func (factory *tunersFactory) newCpuTuner(params *tunerParams) tuners.Tunable {
	return cpu.NewCpuTuner(
		factory.cpuMasks,
		factory.grub,
		factory.fs,
	)
}

func tune(
	elementsToTune []string, tunersMap *tunersFactory, params *tunerParams,
) error {
	for _, tunerName := range elementsToTune {
		tuner := tunersMap.newTuner(tunerName, params)
		if supported, reason := tuner.CheckIfSupported(); supported == true {
			log.Infof("Running '%s' tuner...", tunerName)
			err := tuner.Tune()
			if err != nil {
				return err
			}
		} else {
			log.Infof("Tuner '%s' is not supported - %s", tunerName, reason)
		}
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
- Disable Intel P-States 
- Disable Intel C-States
- Disable Turbo Boost
- Sets the ACPI-cpufreq governor to ‘performance’

Important: System needs to be restarted after first run of this tuner. 
Then the tuner need to be executed after each system reboot.`

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
