package factory

import (
	"runtime"
	"vectorized/pkg/net"
	"vectorized/pkg/os"
	"vectorized/pkg/redpanda"
	"vectorized/pkg/system"
	"vectorized/pkg/tuners"
	"vectorized/pkg/tuners/cpu"
	"vectorized/pkg/tuners/disk"
	"vectorized/pkg/tuners/ethtool"
	"vectorized/pkg/tuners/executors"
	"vectorized/pkg/tuners/hwloc"
	"vectorized/pkg/tuners/irq"
	"vectorized/pkg/tuners/network"
	"vectorized/pkg/tuners/sys"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
)

var (
	allTuners = map[string]func(*tunersFactory, *TunerParams) tuners.Tunable{
		"disk_irq":       (*tunersFactory).newDiskIRQTuner,
		"disk_scheduler": (*tunersFactory).newDiskSchedulerTuner,
		"disk_nomerges":  (*tunersFactory).newDiskNomergesTuner,
		"net":            (*tunersFactory).newNetworkTuner,
		"cpu":            (*tunersFactory).newCpuTuner,
		"aio_events":     (*tunersFactory).newMaxAIOEventsTuner,
		"clocksource":    (*tunersFactory).newClockSourceTuner,
	}
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
}

type tunersFactory struct {
	fs                afero.Fs
	irqDeviceInfo     irq.DeviceInfo
	cpuMasks          irq.CpuMasks
	irqBalanceService irq.BalanceService
	irqProcFile       irq.ProcFile
	blockDevices      disk.BlockDevices
	proc              os.Proc
	grub              system.Grub
	executor          executors.Executor
}

func NewDirectExecutorTunersFactory(fs afero.Fs) TunersFactory {
	irqProcFile := irq.NewProcFile(fs)
	proc := os.NewProc()
	irqDeviceInfo := irq.NewDeviceInfo(fs, irqProcFile)
	executor := executors.NewDirectExecutor()
	return newTunersFactory(fs, irqProcFile, proc, irqDeviceInfo, executor)
}

func NewScriptRenderingTunersFactory(fs afero.Fs, out string) TunersFactory {
	irqProcFile := irq.NewProcFile(fs)
	proc := os.NewProc()
	irqDeviceInfo := irq.NewDeviceInfo(fs, irqProcFile)
	executor := executors.NewScriptRenderingExecutor(fs, out)
	return newTunersFactory(fs, irqProcFile, proc, irqDeviceInfo, executor)
}

func newTunersFactory(
	fs afero.Fs,
	irqProcFile irq.ProcFile,
	proc os.Proc,
	irqDeviceInfo irq.DeviceInfo,
	executor executors.Executor,
) TunersFactory {

	return &tunersFactory{
		fs:                fs,
		irqProcFile:       irqProcFile,
		irqDeviceInfo:     irqDeviceInfo,
		cpuMasks:          irq.NewCpuMasks(fs, hwloc.NewHwLocCmd(proc), executor),
		irqBalanceService: irq.NewBalanceService(fs, proc, executor),
		blockDevices:      disk.NewBlockDevices(fs, irqDeviceInfo, irqProcFile, proc),
		grub:              system.NewGrub(os.NewCommands(proc), proc, fs, executor),
		proc:              proc,
		executor:          executor,
	}
}

func AvailableTuners() []string {
	var keys []string
	for key := range allTuners {
		keys = append(keys, key)
	}
	return keys
}

func IsTunerAvailable(tuner string) bool {
	if allTuners[tuner] != nil {
		return true
	}
	return false
}

func (factory *tunersFactory) CreateTuner(
	tunerName string, tunerParams *TunerParams,
) tuners.Tunable {
	return allTuners[tunerName](factory, tunerParams)
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
		factory.executor,
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
		factory.executor,
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
		factory.executor,
	)
}

func (factory *tunersFactory) newNetworkTuner(
	params *TunerParams,
) tuners.Tunable {
	ethtool, err := ethtool.NewEthtoolWrapper()
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
		factory.executor,
	)
}

func (factory *tunersFactory) newCpuTuner(params *TunerParams) tuners.Tunable {
	return cpu.NewCpuTuner(
		factory.cpuMasks,
		factory.grub,
		factory.fs,
		params.RebootAllowed,
		factory.executor,
	)
}

func (factory *tunersFactory) newMaxAIOEventsTuner(
	params *TunerParams,
) tuners.Tunable {
	return sys.NewMaxAIOEventsTuner(factory.fs, factory.executor)
}

func (factory *tunersFactory) newClockSourceTuner(
	params *TunerParams,
) tuners.Tunable {
	return sys.NewClockSourceTuner(factory.fs, factory.executor)
}

func FillTunerParamsWithValuesFromConfig(
	params *TunerParams, config *redpanda.Config,
) error {
	nics, err := net.GetInterfacesByIps(
		config.KafkaApi.Address, config.RPCServer.Address)
	if err != nil {
		return err
	}
	params.Nics = nics
	log.Infof("Redpanda uses '%v' NICs", params.Nics)
	log.Infof("Redpanda data directory '%s'", config.Directory)
	params.Directories = []string{config.Directory}
	return nil
}
