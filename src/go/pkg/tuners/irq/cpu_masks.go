package irq

import (
	"fmt"
	"strings"
	"vectorized/pkg/tuners/hwloc"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
)

type CpuMasks interface {
	BaseCpuMask(cpuMask string) (string, error)
	CpuMaskForComputations(mode Mode, cpuMask string) (string, error)
	CpuMaskForIRQs(mode Mode, cpuMask string) (string, error)
	SetMask(path string, mask string) error
	DistributeIRQs(irqs []int, cpuMask string) error
	GetDistributionMasks(count uint) ([]string, error)
	GetNumberOfCores(mask string) (uint, error)
	GetNumberOfPUs(mask string) (uint, error)
	GetAllCpusMask() (string, error)
	GetLogicalCoreIdsFromPhysCore(core uint) ([]uint, error)
	IsSupported() bool
}

func NewCpuMasks(fs afero.Fs, hwloc hwloc.HwLoc) CpuMasks {
	return &cpuMasks{
		fs:    fs,
		hwloc: hwloc,
	}
}

type cpuMasks struct {
	CpuMasks
	hwloc hwloc.HwLoc
	fs    afero.Fs
}

func (masks *cpuMasks) BaseCpuMask(cpuMask string) (string, error) {
	if cpuMask == "all" {
		return masks.hwloc.All()
	}

	return masks.hwloc.CalcSingle(cpuMask)
}

func (masks *cpuMasks) IsSupported() bool {
	return masks.hwloc.IsSupported()
}

func (masks *cpuMasks) CpuMaskForComputations(
	mode Mode, cpuMask string,
) (string, error) {
	log.Debugf("Computing CPU mask for '%s' mode and input CPU mask '%s'", mode, cpuMask)
	var computationsMask = ""
	var err error
	if mode == Sq {
		// all but CPU0
		computationsMask, err = masks.hwloc.Calc(cpuMask, "~PU:0")
	} else if mode == SqSplit {
		// all but CPU0 and its HT siblings
		computationsMask, err = masks.hwloc.Calc(cpuMask, "~core:0")
	} else if mode == Mq {
		// all available cores
		computationsMask = cpuMask
	} else {
		err = fmt.Errorf("Unsupported mode: '%s'", mode)
	}

	if masks.hwloc.CheckIfMaskIsEmpty(computationsMask) {
		err = fmt.Errorf("Bad configuration mode '%s' and cpu-mask value '%s':"+
			" this results in a zero-mask for 'computations'", mode, cpuMask)
	}
	log.Debugf("Computations CPU mask '%s'", computationsMask)
	return computationsMask, err
}

func (masks *cpuMasks) CpuMaskForIRQs(
	mode Mode, cpuMask string,
) (string, error) {
	log.Debugf("Computing IRQ CPU mask for '%s' mode and input CPU mask '%s'",
		mode, cpuMask)
	var err error
	var maskForIRQs string
	if mode != Mq {
		maskForComputations, err := masks.CpuMaskForComputations(mode, cpuMask)
		maskForIRQs, err = masks.hwloc.Calc(cpuMask, fmt.Sprintf("~%s", maskForComputations))
		if err != nil {
			return maskForIRQs, err
		}
	} else {
		maskForIRQs = cpuMask
	}
	if masks.hwloc.CheckIfMaskIsEmpty(maskForIRQs) {
		return "", fmt.Errorf("bad configuration mode '%s' and cpu-mask value '%s':"+
			" this results in a zero-mask for IRQs", mode, cpuMask)
	}
	log.Debugf("IQRs CPU mask '%s'", maskForIRQs)
	return maskForIRQs, err
}

func (masks *cpuMasks) SetMask(path string, mask string) error {
	if _, err := masks.fs.Stat(path); err != nil {
		return fmt.Errorf("configure file '%s' to set mask does not exist", path)
	}
	var formattedMask = strings.Replace(mask, "0x", "", -1)
	for strings.Contains(formattedMask, ",,") {
		formattedMask = strings.Replace(formattedMask, ",,", ",0,", -1)
	}

	log.Infof("Setting mask '%s' in '%s'", formattedMask, path)
	err := afero.WriteFile(masks.fs, path, []byte(formattedMask), 0644)
	if err != nil {
		return err
	}
	return nil
}

func (masks *cpuMasks) GetDistributionMasks(count uint) ([]string, error) {
	return masks.hwloc.Distribute(count)
}

func (masks *cpuMasks) DistributeIRQs(irqs []int, cpuMask string) error {
	if len(irqs) == 0 {
		return nil
	}
	irqsDistribution, err := masks.hwloc.DistributeRestrict(uint(len(irqs)), cpuMask)
	if err != nil {
		return err
	}
	log.Infof("Distributing IRQs '%v' over cpu masks '%s'", irqs, irqsDistribution)
	for i, mask := range irqsDistribution {
		err := masks.SetMask(fmt.Sprintf("/proc/irq/%d/smp_affinity", irqs[i]), mask)
		if err != nil {
			return err
		}
	}
	return nil
}

func (masks *cpuMasks) GetNumberOfCores(mask string) (uint, error) {
	return masks.hwloc.GetNumberOfCores(mask)
}

func (masks *cpuMasks) GetNumberOfPUs(mask string) (uint, error) {
	return masks.hwloc.GetNumberOfPUs(mask)
}

func (masks *cpuMasks) GetLogicalCoreIdsFromPhysCore(
	core uint,
) ([]uint, error) {
	return masks.hwloc.GetPhysIntersection("PU", fmt.Sprintf("core:%d", core))
}

func (masks *cpuMasks) GetAllCpusMask() (string, error) {
	return masks.hwloc.All()
}
