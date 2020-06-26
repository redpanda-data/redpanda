package factory

import (
	"runtime"
	"time"
	"vectorized/pkg/config"
	"vectorized/pkg/net"
	"vectorized/pkg/os"
	"vectorized/pkg/system"
	"vectorized/pkg/tuners"
	"vectorized/pkg/tuners/coredump"
	"vectorized/pkg/tuners/cpu"
	"vectorized/pkg/tuners/disk"
	"vectorized/pkg/tuners/ethtool"
	"vectorized/pkg/tuners/executors"
	"vectorized/pkg/tuners/hwloc"
	"vectorized/pkg/tuners/irq"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
)

var (
	allTuners = map[string]func(*tunersFactory, *TunerParams) tuners.Tunable{
		"disk_irq":              (*tunersFactory).newDiskIRQTuner,
		"disk_scheduler":        (*tunersFactory).newDiskSchedulerTuner,
		"disk_nomerges":         (*tunersFactory).newDiskNomergesTuner,
		"net":                   (*tunersFactory).newNetworkTuner,
		"cpu":                   (*tunersFactory).newCpuTuner,
		"aio_events":            (*tunersFactory).newMaxAIOEventsTuner,
		"clocksource":           (*tunersFactory).newClockSourceTuner,
		"swappiness":            (*tunersFactory).newSwappinessTuner,
		"transparent_hugepages": (*tunersFactory).newTHPTuner,
		"coredump":              (*tunersFactory).newCoredumpTuner,
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
	conf              config.Config
	irqDeviceInfo     irq.DeviceInfo
	cpuMasks          irq.CpuMasks
	irqBalanceService irq.BalanceService
	irqProcFile       irq.ProcFile
	blockDevices      disk.BlockDevices
	proc              os.Proc
	grub              system.Grub
	executor          executors.Executor
}

func NewDirectExecutorTunersFactory(
	fs afero.Fs, conf config.Config, timeout time.Duration,
) TunersFactory {
	irqProcFile := irq.NewProcFile(fs)
	proc := os.NewProc()
	irqDeviceInfo := irq.NewDeviceInfo(fs, irqProcFile)
	executor := executors.NewDirectExecutor()
	return newTunersFactory(fs, conf, irqProcFile, proc, irqDeviceInfo, executor, timeout)
}

func NewScriptRenderingTunersFactory(
	fs afero.Fs, conf config.Config, out string, timeout time.Duration,
) TunersFactory {
	irqProcFile := irq.NewProcFile(fs)
	proc := os.NewProc()
	irqDeviceInfo := irq.NewDeviceInfo(fs, irqProcFile)
	executor := executors.NewScriptRenderingExecutor(fs, out)
	return newTunersFactory(fs, conf, irqProcFile, proc, irqDeviceInfo, executor, timeout)
}

func newTunersFactory(
	fs afero.Fs,
	conf config.Config,
	irqProcFile irq.ProcFile,
	proc os.Proc,
	irqDeviceInfo irq.DeviceInfo,
	executor executors.Executor,
	timeout time.Duration,
) TunersFactory {
	return &tunersFactory{
		fs:                fs,
		conf:              conf,
		irqProcFile:       irqProcFile,
		irqDeviceInfo:     irqDeviceInfo,
		cpuMasks:          irq.NewCpuMasks(fs, hwloc.NewHwLocCmd(proc, timeout), executor),
		irqBalanceService: irq.NewBalanceService(fs, proc, executor, timeout),
		blockDevices:      disk.NewBlockDevices(fs, irqDeviceInfo, irqProcFile, proc, timeout),
		grub:              system.NewGrub(os.NewCommands(proc), proc, fs, executor, timeout),
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
	return allTuners[tuner] != nil
}

func IsTunerEnabled(tuner string, rpkConfig *config.RpkConfig) bool {
	switch tuner {
	case "disk_irq":
		return rpkConfig.TuneDiskIrq
	case "disk_scheduler":
		return rpkConfig.TuneDiskScheduler
	case "disk_nomerges":
		return rpkConfig.TuneNomerges
	case "net":
		return rpkConfig.TuneNetwork
	case "cpu":
		return rpkConfig.TuneCpu
	case "aio_events":
		return rpkConfig.TuneAioEvents
	case "clocksource":
		return rpkConfig.TuneClocksource
	case "swappiness":
		return rpkConfig.TuneSwappiness
	case "transparent_hugepages":
		return rpkConfig.TuneTransparentHugePages
	case "coredump":
		return rpkConfig.TuneCoredump
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

	return tuners.NewDiskIRQTuner(
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
	return tuners.NewSchedulerTuner(
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
	return tuners.NewNomergesTuner(
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
	return tuners.NewNetTuner(
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
	return tuners.NewMaxAIOEventsTuner(factory.fs, factory.executor)
}

func (factory *tunersFactory) newClockSourceTuner(
	params *TunerParams,
) tuners.Tunable {
	return tuners.NewClockSourceTuner(factory.fs, factory.executor)
}

func (factory *tunersFactory) newSwappinessTuner(
	params *TunerParams,
) tuners.Tunable {
	return tuners.NewSwappinessTuner(factory.fs, factory.executor)
}

func (factory *tunersFactory) newTHPTuner(_ *TunerParams) tuners.Tunable {
	return tuners.NewEnableTHPTuner(factory.fs, factory.executor)
}

func (factory *tunersFactory) newCoredumpTuner(
	params *TunerParams,
) tuners.Tunable {
	return coredump.NewCoredumpTuner(factory.fs, factory.conf, factory.executor)
}

func MergeTunerParamsConfig(
	params *TunerParams, conf *config.Config,
) (*TunerParams, error) {
	if len(params.Nics) == 0 {
		nics, err := net.GetInterfacesByIps(
			conf.Redpanda.KafkaApi.Address,
			conf.Redpanda.RPCServer.Address,
		)
		if err != nil {
			return params, err
		}
		params.Nics = nics
	}
	if len(params.Directories) == 0 {
		params.Directories = []string{conf.Redpanda.Directory}
	}
	return params, nil
}

func FillTunerParamsWithValuesFromConfig(
	params *TunerParams, conf *config.Config,
) error {
	nics, err := net.GetInterfacesByIps(
		conf.Redpanda.KafkaApi.Address, conf.Redpanda.RPCServer.Address)
	if err != nil {
		return err
	}
	params.Nics = nics
	log.Infof("Redpanda uses '%v' NICs", params.Nics)
	log.Infof("Redpanda data directory '%s'", conf.Redpanda.Directory)
	params.Directories = []string{conf.Redpanda.Directory}
	return nil
}
