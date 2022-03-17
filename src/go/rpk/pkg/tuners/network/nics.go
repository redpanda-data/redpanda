// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package network

import (
	"fmt"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/irq"
	log "github.com/sirupsen/logrus"
)

func getDefaultMode(
	nic Nic, cpuMask string, cpuMasks irq.CpuMasks,
) (irq.Mode, error) {

	if nic.IsHwInterface() {
		rxQueuesCount, err := nic.GetRxQueueCount()
		if err != nil {
			return "", err
		}
		log.Debugf("Calculating default mode for '%s'", nic.Name())
		numOfCores, err := cpuMasks.GetNumberOfCores(cpuMask)
		if err != nil {
			return "", err
		}
		numOfPUs, err := cpuMasks.GetNumberOfPUs(cpuMask)
		if err != nil {
			return "", err
		}
		log.Debugf("Considering '%d' cores and '%d' PUs", numOfCores, numOfPUs)

		if numOfPUs <= 4 || rxQueuesCount == int(numOfPUs) {
			return irq.Mq, nil
		} else if numOfCores <= 4 {
			return irq.Sq, nil
		} else {
			return irq.SqSplit, nil
		}
	}

	if nic.IsBondIface() {
		defaultMode := irq.Mq
		slaves, err := nic.Slaves()
		if err != nil {
			return "", err
		}
		for _, slave := range slaves {
			slaveDefaultMode, err := getDefaultMode(slave, cpuMask, cpuMasks)
			if err != nil {
				return "", err
			}
			if slaveDefaultMode == irq.Sq {
				defaultMode = irq.Sq
			} else if slaveDefaultMode == irq.SqSplit && defaultMode == irq.Mq {
				defaultMode = irq.SqSplit
			}
		}
		return defaultMode, nil
	}
	return "", fmt.Errorf("Virutal device %s is not supported", nic.Name())
}

func GetRpsCPUMask(
	nic Nic, mode irq.Mode, cpuMask string, cpuMasks irq.CpuMasks,
) (string, error) {
	effectiveCPUMask, err := cpuMasks.BaseCpuMask(cpuMask)
	if err != nil {
		return "", err
	}
	var effectiveMode = mode
	if mode == irq.Default {
		effectiveMode, err = getDefaultMode(nic, effectiveCPUMask, cpuMasks)
		if err != nil {
			return "", err
		}
	}
	computationsCPUMask, err := cpuMasks.CpuMaskForComputations(
		effectiveMode, effectiveCPUMask)
	if err != nil {
		return "", err
	}
	return computationsCPUMask, nil
}

func GetHwInterfaceIRQsDistribution(
	nic Nic, mode irq.Mode, cpuMask string, cpuMasks irq.CpuMasks,
) (map[int]string, error) {
	effectiveCPUMask, err := cpuMasks.BaseCpuMask(cpuMask)
	if err != nil {
		return nil, err
	}
	var effectiveMode = mode
	if mode == irq.Default {
		effectiveMode, err = getDefaultMode(nic, effectiveCPUMask, cpuMasks)
		if err != nil {
			return nil, err
		}
	}

	maxRxQueues, err := nic.GetMaxRxQueueCount()
	if err != nil {
		return nil, err
	}
	allIRQs, err := nic.GetIRQs()
	if err != nil {
		return nil, err
	}

	irqCPUMask, err := cpuMasks.CpuMaskForIRQs(effectiveMode, effectiveCPUMask)
	if err != nil {
		return nil, err
	}

	if maxRxQueues >= len(allIRQs) {
		log.Debugf("Calculating distribution '%s' IRQs", nic.Name())
		IRQsDistribution, err := cpuMasks.GetIRQsDistributionMasks(
			allIRQs, irqCPUMask)
		if err != nil {
			return nil, err
		}
		return IRQsDistribution, nil
	}

	rxQueues, err := nic.GetRxQueueCount()
	if err != nil {
		return nil, err
	}
	log.Debugf("Number of Rx queues for '%s' = '%d'", nic.Name(), rxQueues)
	log.Infof("Distributing '%s' IRQs handling Rx queues", nic.Name())
	IRQsDistribution, err := cpuMasks.GetIRQsDistributionMasks(
		allIRQs[0:rxQueues], irqCPUMask)
	if err != nil {
		return nil, err
	}
	log.Infof("Distributing rest of '%s' IRQs", nic.Name())
	restIRQsDistribution, err := cpuMasks.GetIRQsDistributionMasks(
		allIRQs[rxQueues:], irqCPUMask)
	if err != nil {
		return nil, err
	}
	for irq, mask := range restIRQsDistribution {
		IRQsDistribution[irq] = mask
	}
	return IRQsDistribution, nil
}

func CollectIRQs(nic Nic) ([]int, error) {
	var IRQs []int
	if nic.IsHwInterface() {
		nicIRQs, err := nic.GetIRQs()
		if err != nil {
			return nil, err
		}
		IRQs = append(IRQs, nicIRQs...)
	}
	if nic.IsBondIface() {
		slaves, err := nic.Slaves()
		if err != nil {
			return nil, err
		}
		for _, slave := range slaves {
			slaveIRQs, err := CollectIRQs(slave)
			if err != nil {
				return nil, err
			}
			IRQs = append(IRQs, slaveIRQs...)

		}
	}
	return IRQs, nil
}

func OneRPSQueueLimit(limits []string) int {
	return RfsTableSize / len(limits)
}
