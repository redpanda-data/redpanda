package tune

import (
	"errors"
	"fmt"
	"strings"
	"vectorized/pkg/utils"

	"github.com/spf13/cobra"
)

func NewHelpCommand() *cobra.Command {
	tunersHelp := map[string]string{
		"cpu":            cpuTunerHelp,
		"disk_irq":       diskIrqTunerHelp,
		"disk_scheduler": diskSchedulerTunerHelp,
		"net":            netTunerHelp,
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
