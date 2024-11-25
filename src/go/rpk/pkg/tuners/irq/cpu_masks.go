// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package irq

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/executors"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/executors/commands"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/hwloc"
	"github.com/spf13/afero"
	"go.uber.org/zap"
)

type CPUMasks interface {
	BaseCPUMask(cpuMask string) (string, error)
	CPUMaskForComputations(mode Mode, cpuMask string) (string, error)
	CPUMaskForIRQs(mode Mode, cpuMask string) (string, error)
	SetMask(path string, mask string) error
	ReadMask(path string) (string, error)
	ReadIRQMask(IRQ int) (string, error)
	DistributeIRQs(irqsDistribution map[int]string)
	GetDistributionMasks(count uint) ([]string, error)
	GetIRQsDistributionMasks(IRQs []int, cpuMask string) (map[int]string, error)
	GetNumberOfCores(mask string) (uint, error)
	GetNumberOfPUs(mask string) (uint, error)
	GetAllCpusMask() (string, error)
	GetLogicalCoreIDsFromPhysCore(core uint) ([]uint, error)
	IsSupported() bool
}

func NewCPUMasks(
	fs afero.Fs, hwloc hwloc.HwLoc, executor executors.Executor,
) CPUMasks {
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

func (masks *cpuMasks) BaseCPUMask(cpuMask string) (string, error) {
	if cpuMask == "all" {
		return masks.hwloc.All()
	}

	return masks.hwloc.CalcSingle(cpuMask)
}

func (masks *cpuMasks) IsSupported() bool {
	return masks.hwloc.IsSupported()
}

func (masks *cpuMasks) CPUMaskForComputations(
	mode Mode, cpuMask string,
) (string, error) {
	zap.L().Sugar().Debugf("Computing CPU mask for '%s' mode and input CPU mask '%s'", mode, cpuMask)
	computationsMask := ""
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
		err = fmt.Errorf("bad configuration mode '%s' and cpu-mask value '%s':"+
			" this results in a zero-mask for 'computations'", mode, cpuMask)
	}
	zap.L().Sugar().Debugf("Computations CPU mask '%s'", computationsMask)
	return computationsMask, err
}

func (masks *cpuMasks) CPUMaskForIRQs(
	mode Mode, cpuMask string,
) (string, error) {
	zap.L().Sugar().Debugf("Computing IRQ CPU mask for '%s' mode and input CPU mask '%s'",
		mode, cpuMask)
	var err error
	var maskForIRQs string
	if mode != Mq {
		maskForComputations, err := masks.CPUMaskForComputations(mode, cpuMask)
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
	zap.L().Sugar().Debugf("IRQs CPU mask '%s'", maskForIRQs)
	return maskForIRQs, err
}

func (masks *cpuMasks) SetMask(path string, mask string) error {
	if _, err := masks.fs.Stat(path); err != nil {
		return fmt.Errorf("SMP affinity file '%s' not exist", path)
	}
	formattedMask := strings.Replace(mask, "0x", "", -1)
	for strings.Contains(formattedMask, ",,") {
		formattedMask = strings.Replace(formattedMask, ",,", ",0,", -1)
	}

	zap.L().Sugar().Debugf("Setting mask '%s' in '%s'", formattedMask, path)
	err := masks.executor.Execute(
		commands.NewWriteFileModeCmd(masks.fs, path, formattedMask, 0o555))
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

func (masks *cpuMasks) DistributeIRQs(irqsDistribution map[int]string) {
	zap.L().Sugar().Debugf("Distributing IRQs '%v' ", irqsDistribution)
	errMsg := "An IRQ's affinity couldn't be set. This might be because the" +
		" IRQ isn't IO-APIC compatible, or because the IRQ is managed" +
		" by the kernel, and can be safely ignored."
	for IRQ, mask := range irqsDistribution {
		err := masks.SetMask(irqAffinityPath(IRQ), mask)
		// IRQ SMP affinity is tuned on a best-effort basis. Most
		// IO-APIC compatible IRQs allow their affinity to be set, but
		// there are exceptions (such as IRQ 0, which is the timer IRQ).
		// Likewise, if an IRQ isn't marked as IO-APIC-compatible, it
		// doesn't mean its affinity can't be set. Therefore the errors
		// are logged but otherwise ignored.
		if err != nil {
			zap.L().Sugar().Debug(err)
			zap.L().Sugar().Debug(errMsg)
		}
	}
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

func (masks *cpuMasks) GetLogicalCoreIDsFromPhysCore(
	core uint,
) ([]uint, error) {
	return masks.hwloc.GetPhysIntersection("PU", fmt.Sprintf("core:%d", core))
}

func (masks *cpuMasks) GetAllCpusMask() (string, error) {
	return masks.hwloc.All()
}

func MasksEqual(a, b string) (bool, error) {
	aParts := strings.Split(a, ",")
	bParts := strings.Split(b, ",")

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

func parseMask(mask string) (uint, error) {
	if mask == "" {
		return 0, nil
	}
	s := strings.ReplaceAll(mask, "0x", "")
	num, err := strconv.ParseUint(s, 16, 32)
	return uint(num), err
}
