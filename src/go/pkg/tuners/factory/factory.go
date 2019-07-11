package factory

import (
	"runtime"
	"vectorized/os"
	"vectorized/tuners"
	"vectorized/tuners/cpu"
	"vectorized/tuners/disk"
	"vectorized/tuners/hwloc"
	"vectorized/tuners/irq"
	"vectorized/tuners/network"

	"github.com/safchain/ethtool"
	"github.com/spf13/afero"
)

type TunerParams struct {
	Mode          string
	CpuMask       string
	RebootAllowed bool
	Disks         []string
	Directories   []string
	Nic           string
}

type TunersFactory interface {
	CreateTuner(tunerType string, params *TunerParams) tuners.Tunable
	AvailableTuners() []string
	IsTunerAvailable(tuner string) bool
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
	tuners            map[string]func(*tunersFactory, *TunerParams) tuners.Tunable
}

func NewTunersFactory(fs afero.Fs) TunersFactory {
	irqProcFile := irq.NewProcFile(fs)
	proc := os.NewProc()
	return &tunersFactory{
		tuners: map[string]func(*tunersFactory, *TunerParams) tuners.Tunable{
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
}

func (factory *tunersFactory) AvailableTuners() []string {
	var keys []string
	for key := range factory.tuners {
		keys = append(keys, key)
	}
	return keys
}

func (factory *tunersFactory) IsTunerAvailable(tuner string) bool {
	if factory.tuners[tuner] != nil {
		return true
	}
	return false
}

func (factory *tunersFactory) CreateTuner(
	tunerName string, tunerParams *TunerParams,
) tuners.Tunable {
	return factory.tuners[tunerName](factory, tunerParams)
}

func (factory *tunersFactory) newDiskIrqTuner(
	params *TunerParams,
) tuners.Tunable {

	return disk.NewDiskIrqTuner(
		irq.ModeFromString(params.Mode),
		params.CpuMask,
		params.Directories,
		params.Disks,
		factory.irqDeviceInfo,
		factory.cpuMasks,
		factory.irqBalanceService,
		factory.irqProcFile,
		factory.diskInfoProvider,
		runtime.NumCPU(),
	)
}

func (factory *tunersFactory) newDiskSchedulerTuner(
	params *TunerParams,
) tuners.Tunable {
	return disk.NewSchedulerTuner(
		params.Directories,
		params.Disks,
		factory.diskInfoProvider,
		factory.fs)
}

func (factory *tunersFactory) newNetworkTuner(
	params *TunerParams,
) tuners.Tunable {
	return network.NewNetTuner(
		irq.ModeFromString(params.Mode),
		params.CpuMask,
		params.Nic,
		factory.fs,
		factory.irqDeviceInfo,
		factory.cpuMasks,
		factory.irqBalanceService,
		factory.irqProcFile,
		&ethtool.Ethtool{},
	)
}

func (factory *tunersFactory) newCpuTuner(params *TunerParams) tuners.Tunable {
	return cpu.NewCpuTuner(
		factory.cpuMasks,
		factory.grub,
		factory.fs,
		params.RebootAllowed,
	)
}
