package cmd

import (
	"errors"
	"fmt"
	"runtime"
	"strings"
	"vectorized/os"
	"vectorized/tuners"
	"vectorized/tuners/disk"
	"vectorized/tuners/hwloc"
	"vectorized/tuners/irq"
	"vectorized/tuners/network"

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
		},
		fs:                fs,
		irqProcFile:       irqProcFile,
		irqDeviceInfo:     irq.NewDeviceInfo(fs, irqProcFile),
		cpuMasks:          irq.NewCpuMasks(fs, hwloc.NewHwLocCmd(proc)),
		irqBalanceService: irq.NewBalanceService(fs, proc),
		diskInfoProvider:  disk.NewDiskInfoProvider(fs, proc),
		proc:              proc,
	}
	tunerParams := tunerParams{}
	command := &cobra.Command{
		Use:   "tune <list_of_elements_to_tune>",
		Short: "Sets the OS parameters to tune system performance",
		Long: `Modes description:
		sq - set all IRQs of a given NIC to CPU0 and configure RPS
			 to spreads NAPIs' handling between other CPUs.
		sq_split - divide all IRQs of a given NIC between CPU0 and its HT siblings and configure RPS
			 to spreads NAPIs' handling between other CPUs.
		mq - distribute NIC's IRQs among all CPUs instead of binding
			 them all to CPU0. In this mode RPS is always enabled to
			 spreads NAPIs' handling between all CPUs.
			 
		If there isn't any mode given script will use a default mode:
			 - If number of physical CPU cores per Rx HW queue is greater than 4 - use the 'sq-split' mode.
			 - Otherwise, if number of hyper-threads per Rx HW queue is greater than 4 - use the 'sq' mode.
			 - Otherwise use the 'mq' mode.`,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) < 1 {
				return errors.New("requires the list of elements to tune")
			}
			for _, toTune := range strings.Split(args[0], ",") {
				if tunerFactory.tuners[toTune] == nil {
					return fmt.Errorf("invalid element to tune '%s' only %s are supported",
						args[0], tunerFactory.getTunerNames())
				}
			}
			return nil
		},
		RunE: func(ccmd *cobra.Command, args []string) error {
			return tune(strings.Split(args[0], ","), &tunerFactory, &tunerParams)
		},
	}

	command.PersistentFlags().StringVarP(&tunerParams.mode, "mode", "m", "",
		"Operation Mode: one of: [sq, sq_split, mq]")

	command.PersistentFlags().StringSliceP("tunables", "t",
		tunerParams.thingsToTune, fmt.Sprintf("Comma separated list of elements to tune f.e. 'disk,network' "+
			"- available tuners %s", tunerFactory.getTunerNames()))

	command.PersistentFlags().StringVarP(&tunerParams.cpuMask, "cpu-mask", "c",
		"all", "CPU Mask (32-bit hexadecimal integer))")
	command.PersistentFlags().StringSliceVarP(&tunerParams.disks, "disks", "d",
		[]string{}, "Lists of devices to tune f.e. 'sda1'")
	command.PersistentFlags().StringVarP(&tunerParams.nic, "nic", "n",
		"eth0", "Network Interface Controller to tune")
	command.PersistentFlags().StringSliceVarP(&tunerParams.directories, "dirs", "r",
		[]string{}, "List of *data* directories. or places to store data. i.e.: '/var/vectorized/redpanda/',"+
			" usually your XFS filesystem on an NVMe SSD device")
	return command
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
