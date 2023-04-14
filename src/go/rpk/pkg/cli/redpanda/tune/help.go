// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

//go:build linux

package tune

import (
	"errors"
	"fmt"
	"strings"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/utils"
	"github.com/spf13/cobra"
)

func newHelpCommand() *cobra.Command {
	tunersHelp := map[string]string{
		"cpu":                   cpuTunerHelp,
		"disk_irq":              diskIrqTunerHelp,
		"disk_scheduler":        diskSchedulerTunerHelp,
		"net":                   netTunerHelp,
		"swappiness":            swappinessTunerHelp,
		"fstrim":                fstrimTunerHelp,
		"aio_events":            aioEventsTunerHelp,
		"transparent_hugepages": transparentHugepagesTunerHelp,
		"clocksource":           clocksourceTunerHelp,
		"nomerges":              nomergesTunerHelp,
	}

	return &cobra.Command{
		Use:   "help [TUNER]",
		Short: "Display detailed information about the tuner",
		Args: func(_ *cobra.Command, args []string) error {
			if len(args) != 1 {
				return errors.New("requires the tuner name")
			}
			tuner := args[0]
			tunerList := strings.Join(
				utils.GetKeysFromStringMap(tunersHelp),
				", ",
			)
			if _, contains := tunersHelp[tuner]; !contains {
				return fmt.Errorf("no help found for tuner '%s'. Available: %s", tuner, tunerList)
			}
			return nil
		},
		Run: func(_ *cobra.Command, args []string) {
			tuner := args[0]
			fmt.Printf("%q tuner description\n%s\n", tuner, tunersHelp[tuner])
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

Additionally if system reboot is allowed:
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

const swappinessTunerHelp = `
Tunes the kernel to keep process data in-memory for as long as possible, instead
of swapping it out to disk.
`

const fstrimTunerHelp = `
Will start the default 'fstrim' systemd service, which runs in the background on
a weekly basis and "trims" or "wipes" blocks which are not in use by the
filesystem. If the current OS doesn't have an 'fstrim' service by default, the
tuner will install an equivalent one and start it. If systemd isn't available at
all, it will do nothing.

This is desirable for SSDs - the recommended storage drive for Redpanda - because
they require wiping the space where new data will be written. Over time this can
significantly degrade the drive's performance because once free space starts
becoming scarce, subsequent writes will trigger a synchronous erasure to be able
to write the new data.

For more information see 'man fstrim'.
`

const aioEventsTunerHelp = `
Increases the maximum number of outstanding asynchronous IO operations if the
current value is below a certain threshold. This allows redpanda to make as many
simultaneous IO requests as possible, increasing throughput.
`

const transparentHugepagesTunerHelp = `
Enables Transparent Hugepages. This allows the kernel to index larger pages
(2MB, as opposed to the standard 4KB) in the CPU's TLB (if it supports it, which
is the case for most current CPUs). This results in fewer cache misses, which
means less time is spent searching and loading pages.
`

const clocksourceTunerHelp = `
Sets the clock source to TSC (Time Stamp Counter) to get the time more
efficiently via the Virtual Dynamic Shared Object. Most VMs run on Xen, with
'xen' as the default clock source, which doesn't support reading the time in
userspace via the vDSO, requiring making an actual syscall with the overhead it
entails.
`

const nomergesTunerHelp = `
Disables merging adjacent IO requests, which would require checking outstanding
IO requests to batch them where possible, incurring in some CPU overhead.
`
