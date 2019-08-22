package factory

import (
	"runtime"
	"vectorized/pkg/net"
	"vectorized/pkg/os"
	"vectorized/pkg/redpanda"
	"vectorized/pkg/tuners"
	"vectorized/pkg/tuners/cpu"
	"vectorized/pkg/tuners/disk"
	"vectorized/pkg/tuners/hwloc"
	"vectorized/pkg/tuners/irq"
	"vectorized/pkg/tuners/network"
	"vectorized/pkg/tuners/sys"

	"github.com/safchain/ethtool"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
)

type TunerParams struct {
	Mode          string
	CpuMask       string
	RebootAllowed bool
	Disks         []string
	Directories   []string
	Nics          []string
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
	blockDevices      disk.BlockDevices
	proc              os.Proc
	grub              os.Grub
	tuners            map[string]func(*tunersFactory, *TunerParams) tuners.Tunable
}

func NewTunersFactory(fs afero.Fs) TunersFactory {
	irqProcFile := irq.NewProcFile(fs)
	proc := os.NewProc()
	irqDeviceInfo := irq.NewDeviceInfo(fs, irqProcFile)
	return &tunersFactory{
		tuners: map[string]func(*tunersFactory, *TunerParams) tuners.Tunable{
			"disk_irq":       (*tunersFactory).newDiskIRQTuner,
			"disk_scheduler": (*tunersFactory).newDiskSchedulerTuner,
			"disk_nomerges":  (*tunersFactory).newDiskNomergesTuner,
			"net":            (*tunersFactory).newNetworkTuner,
			"cpu":            (*tunersFactory).newCpuTuner,
			"aio_events":     (*tunersFactory).newMaxAIOEventsTuner,
		},
		fs:                fs,
		irqProcFile:       irqProcFile,
		irqDeviceInfo:     irqDeviceInfo,
		cpuMasks:          irq.NewCpuMasks(fs, hwloc.NewHwLocCmd(proc)),
		irqBalanceService: irq.NewBalanceService(fs, proc),
		blockDevices:      disk.NewBlockDevices(fs, irqDeviceInfo, irqProcFile, proc),
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

func (factory *tunersFactory) newDiskIRQTuner(
	params *TunerParams,
) tuners.Tunable {

	return disk.NewDiskIRQTuner(
		factory.fs,
		irq.ModeFromString(params.Mode),
		params.CpuMask,
		params.Directories,
		params.Disks,
		factory.irqDeviceInfo,
		factory.cpuMasks,
		factory.irqBalanceService,
		factory.irqProcFile,
		factory.blockDevices,
		runtime.NumCPU(),
	)
}

func (factory *tunersFactory) newDiskSchedulerTuner(
	params *TunerParams,
) tuners.Tunable {
	return disk.NewSchedulerTuner(
		factory.fs,
		params.Directories,
		params.Disks,
		factory.blockDevices,
	)
}

func (factory *tunersFactory) newDiskNomergesTuner(
	params *TunerParams,
) tuners.Tunable {
	return disk.NewNomergesTuner(
		factory.fs,
		params.Directories,
		params.Disks,
		factory.blockDevices,
	)
}

func (factory *tunersFactory) newNetworkTuner(
	params *TunerParams,
) tuners.Tunable {
	ethtool, err := ethtool.NewEthtool()
	if err != nil {
		panic(err)
	}
	return network.NewNetTuner(
		irq.ModeFromString(params.Mode),
		params.CpuMask,
		params.Nics,
		factory.fs,
		factory.irqDeviceInfo,
		factory.cpuMasks,
		factory.irqBalanceService,
		factory.irqProcFile,
		ethtool,
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

func (factory *tunersFactory) newMaxAIOEventsTuner(
	params *TunerParams,
) tuners.Tunable {
	return sys.NewMaxAIOEventsTuner(factory.fs)
}

func FillTunerParamsWithValuesFromConfig(
	params *TunerParams, config *redpanda.Config,
) error {
	nics, err := net.GetInterfacesByIp(config.Ip)
	if err != nil {
		return err
	}
	params.Nics = nics
	log.Infof("Redpanda uses '%v' NICs", params.Nics)
	log.Infof("Redpanda data directory '%s'", config.Directory)
	params.Directories = []string{config.Directory}
	return nil
}
