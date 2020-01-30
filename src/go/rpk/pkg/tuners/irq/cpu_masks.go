package irq

import (
	"fmt"
	"strconv"
	"strings"
	"vectorized/pkg/tuners/executors"
	"vectorized/pkg/tuners/executors/commands"
	"vectorized/pkg/tuners/hwloc"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
)

type CpuMasks interface {
	BaseCpuMask(cpuMask string) (string, error)
	CpuMaskForComputations(mode Mode, cpuMask string) (string, error)
	CpuMaskForIRQs(mode Mode, cpuMask string) (string, error)
	SetMask(path string, mask string) error
	ReadMask(path string) (string, error)
	ReadIRQMask(IRQ int) (string, error)
	DistributeIRQs(irqsDistribution map[int]string) error
	GetDistributionMasks(count uint) ([]string, error)
	GetIRQsDistributionMasks(IRQs []int, cpuMask string) (map[int]string, error)
	GetNumberOfCores(mask string) (uint, error)
	GetNumberOfPUs(mask string) (uint, error)
	GetAllCpusMask() (string, error)
	GetLogicalCoreIdsFromPhysCore(core uint) ([]uint, error)
	IsSupported() bool
}

func NewCpuMasks(
	fs afero.Fs, hwloc hwloc.HwLoc, executor executors.Executor,
) CpuMasks {
	return &cpuMasks{
		fs:       fs,
		hwloc:    hwloc,
		executor: executor,
	}
}

type cpuMasks struct {
	hwloc    hwloc.HwLoc
	fs       afero.Fs
	executor executors.Executor
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
		if err != nil {
			return "", err
		}
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
	log.Debugf("IRQs CPU mask '%s'", maskForIRQs)
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
	err := masks.executor.Execute(
		commands.NewWriteFileCmd(masks.fs, path, formattedMask))
	if err != nil {
		return err
	}
	return nil
}

func (masks *cpuMasks) GetDistributionMasks(count uint) ([]string, error) {
	return masks.hwloc.Distribute(count)
}

func (masks *cpuMasks) GetIRQsDistributionMasks(
	IRQs []int, cpuMask string,
) (map[int]string, error) {
	distribMasks, err := masks.hwloc.DistributeRestrict(uint(len(IRQs)), cpuMask)
	if err != nil {
		return nil, err
	}
	irqsDistribution := make(map[int]string)
	for i, mask := range distribMasks {
		irqsDistribution[IRQs[i]] = mask
	}
	return irqsDistribution, nil
}

func (masks *cpuMasks) DistributeIRQs(irqsDistribution map[int]string) error {
	log.Infof("Distributing IRQs '%v' ", irqsDistribution)
	for IRQ, mask := range irqsDistribution {
		err := masks.SetMask(irqAffinityPath(IRQ), mask)
		if err != nil {
			return err
		}
	}
	return nil
}

func irqAffinityPath(IRQ int) string {
	return fmt.Sprintf("/proc/irq/%d/smp_affinity", IRQ)
}

func (masks *cpuMasks) ReadMask(path string) (string, error) {
	content, err := afero.ReadFile(masks.fs, path)
	if err != nil {
		return "", err
	}
	rawMask := strings.TrimSpace(string(content))

	rawMask = strings.Replace(rawMask, ",0,", ",,", -1)
	parts := strings.Split(rawMask, ",")
	var newMaskParts []string
	for _, part := range parts {
		if part != "" {
			newMaskParts = append(newMaskParts, "0x"+part)
		} else {
			newMaskParts = append(newMaskParts, part)
		}
	}
	return strings.Join(newMaskParts, ","), nil
}

func (masks *cpuMasks) ReadIRQMask(IRQ int) (string, error) {
	return masks.ReadMask(irqAffinityPath(IRQ))
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

func MasksEqual(a, b string) (bool, error) {
	aParts := strings.Split(a, ",")
	bParts := strings.Split(a, ",")

	if len(aParts) != len(bParts) {
		return false, nil
	}
	for i, aPart := range aParts {
		bPart := bParts[i]
		aNumeric, err := parseMask(aPart)
		if err != nil {
			return false, err
		}
		bNumeric, err := parseMask(bPart)
		if err != nil {
			return false, err
		}
		if aNumeric != bNumeric {
			return false, nil
		}
	}
	return true, nil
}

func parseMask(mask string) (int, error) {
	if mask == "" {
		return 0, nil
	}
	maskNum, err := strconv.ParseInt(strings.ReplaceAll(mask, "0x", ""), 16, 32)
	return int(maskNum), err
}
