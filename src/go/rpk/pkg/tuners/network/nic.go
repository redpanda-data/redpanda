// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

//go:build !windows

package network

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/ethtool"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/irq"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/utils"
	"github.com/spf13/afero"
	"go.uber.org/zap"
)

var driverMaxRssQueues = map[string]int{
	"ixgbe":   16,
	"ixgbevf": 4,
	"i40e":    64,
	"i40evf":  16,
}

type NTupleStatus int

const (
	NTupleEnabled NTupleStatus = iota
	NTupleDisabled
	NTupleNotSupported
)

type Nic interface {
	IsHwInterface() bool
	IsBondIface() bool
	Slaves() ([]Nic, error)
	GetIRQs() ([]int, error)
	GetMaxRxQueueCount() (int, error)
	GetRxQueueCount() (int, error)
	GetRpsCPUFiles() ([]string, error)
	GetXpsCPUFiles() ([]string, error)
	GetRpsLimitFiles() ([]string, error)
	GetNTupleStatus() (NTupleStatus, error)
	Name() string
}

type nic struct {
	fs            afero.Fs
	irqProcFile   irq.ProcFile
	irqDeviceInfo irq.DeviceInfo
	ethtool       ethtool.EthtoolWrapper
	name          string
}

func NewNic(
	fs afero.Fs,
	irqProcFile irq.ProcFile,
	irqDeviceInfo irq.DeviceInfo,
	ethtool ethtool.EthtoolWrapper,
	name string,
) Nic {
	return &nic{
		name:          name,
		fs:            fs,
		irqDeviceInfo: irqDeviceInfo,
		irqProcFile:   irqProcFile,
		ethtool:       ethtool,
	}
}

func (n *nic) Name() string {
	return n.name
}

func (n *nic) IsBondIface() bool {
	zap.L().Sugar().Debugf("Checking if '%s' is bond interface", n.name)
	if exists, _ := afero.Exists(n.fs, "/sys/class/net/bond_masters"); !exists {
		return false
	}
	lines, _ := utils.ReadFileLines(n.fs, "/sys/class/net/bond_masters")
	for _, line := range lines {
		if strings.Contains(line, n.name) {
			zap.L().Sugar().Debugf("'%s' is bond interface", n.name)
			return true
		}
	}
	return false
}

func (n *nic) Slaves() ([]Nic, error) {
	slaves := []Nic{}
	if n.IsBondIface() {
		var slaveNames []string
		zap.L().Sugar().Debugf("Reading slaves of '%s'", n.name)
		lines, err := utils.ReadFileLines(n.fs,
			fmt.Sprintf("/sys/class/net/%s/bond/slaves", n.name))
		if err != nil {
			return nil, err
		}
		for _, line := range lines {
			slaveNames = append(slaveNames, strings.Split(line, " ")...)
		}

		for _, name := range slaveNames {
			slaves = append(slaves, NewNic(n.fs, n.irqProcFile, n.irqDeviceInfo, n.ethtool, name))
		}
	}
	return slaves, nil
}

func (n *nic) IsHwInterface() bool {
	zap.L().Sugar().Debugf("Checking if '%s' is HW interface", n.name)
	exists, _ := afero.Exists(n.fs, fmt.Sprintf("/sys/class/net/%s/device", n.name))
	return exists
}

func (n *nic) GetIRQs() ([]int, error) {
	zap.L().Sugar().Debugf("Getting NIC '%s' IRQs", n.name)
	IRQs, err := n.irqDeviceInfo.GetIRQs(fmt.Sprintf("/sys/class/net/%s/device", n.name),
		n.name)
	if err != nil {
		return nil, err
	}
	procFileLines, err := n.irqProcFile.GetIRQProcFileLinesMap()
	if err != nil {
		return nil, err
	}
	fastPathIRQsPattern := regexp.MustCompile(`-TxRx-|-fp-|-Tx-Rx-|mlx\d+-\d+@`)
	var fastPathIRQs []int
	for _, irq := range IRQs {
		if fastPathIRQsPattern.MatchString(procFileLines[irq]) {
			fastPathIRQs = append(fastPathIRQs, irq)
		}
	}
	if len(fastPathIRQs) > 0 {
		sort.Slice(fastPathIRQs,
			func(i, j int) bool {
				return intelIrqToQueueIdx(fastPathIRQs[i], procFileLines) < intelIrqToQueueIdx(fastPathIRQs[j], procFileLines)
			})
		return fastPathIRQs, nil
	}
	return IRQs, nil
}

func intelIrqToQueueIdx(irq int, procFileLines map[int]string) int {
	intelFastPathIrqPattern := regexp.MustCompile(`-TxRx-(\d+)`)
	fdirPattern := regexp.MustCompile(`fdir-TxRx-\d+`)
	procLine := procFileLines[irq]

	intelFastPathMatch := intelFastPathIrqPattern.FindStringSubmatch(procLine)
	fdirPatternMatch := fdirPattern.FindStringSubmatch(procLine)

	if len(intelFastPathMatch) > 0 && len(fdirPatternMatch) == 0 {
		idx, _ := strconv.Atoi(intelFastPathMatch[1])
		return idx
	}
	return MaxInt
}

func (n *nic) GetMaxRxQueueCount() (int, error) {
	// Networking drivers serving HW with the known maximum RSS queue limitation (due to lack of RSS bits):

	// ixgbe:   PF NICs support up to 16 RSS queues.
	// ixgbevf: VF NICs support up to 4 RSS queues.
	// i40e:    PF NICs support up to 64 RSS queues.
	// i40evf:  VF NICs support up to 16 RSS queues.
	zap.L().Sugar().Debugf("Getting max RSS queues count for '%s'", n.name)

	driverName, err := n.ethtool.DriverName(n.name)
	if err != nil {
		return 0, err
	}
	zap.L().Sugar().Debugf("NIC '%s' uses '%s' driver", n.name, driverName)
	if maxQueues, present := driverMaxRssQueues[driverName]; present {
		return maxQueues, nil
	}

	return MaxInt, nil
}

func (n *nic) GetRxQueueCount() (int, error) {
	rpsCpus, err := n.GetRpsCPUFiles()
	if err != nil {
		return 0, utils.ChainedError(err, "Unable to get the RPS number")
	}
	rxQueuesCount := len(rpsCpus)
	zap.L().Sugar().Debugf("Getting number of Rx queues for '%s'", n.name)
	if rxQueuesCount == 0 {
		IRQs, err := n.GetIRQs()
		if err != nil {
			return 0, err
		}
		irqsCount := len(IRQs)
		rxQueuesCount = irqsCount
	}

	maxRxQueueCount, err := n.GetMaxRxQueueCount()
	if err != nil {
		return 0, err
	}
	if rxQueuesCount < maxRxQueueCount {
		return rxQueuesCount, nil
	}
	return maxRxQueueCount, nil
}

func (n *nic) GetRpsCPUFiles() ([]string, error) {
	// Prints all rps_cpus files names for the given HW interface.
	// There is a single rps_cpus file for each RPS queue and there is a single RPS
	// queue for each HW Rx queue. Each HW Rx queue should have an IRQ.
	// Therefore the number of these files is equal to the number of fast path Rx irqs for this interface.
	return afero.Glob(n.fs, fmt.Sprintf("/sys/class/net/%s/queues/*/rps_cpus", n.name))
}

func (n *nic) GetXpsCPUFiles() ([]string, error) {
	return afero.Glob(n.fs, fmt.Sprintf("/sys/class/net/%s/queues/*/xps_cpus", n.name))
}

func (n *nic) GetRpsLimitFiles() ([]string, error) {
	return afero.Glob(n.fs, fmt.Sprintf("/sys/class/net/%s/queues/*/rps_flow_cnt", n.name))
}

func (n *nic) GetNTupleStatus() (NTupleStatus, error) {
	features, err := n.ethtool.Features(n.name)
	if err != nil {
		return 0, err
	}
	if enabled, exists := features["ntuple"]; exists {
		if enabled {
			return NTupleEnabled, nil
		}
		return NTupleDisabled, nil
	}
	return NTupleNotSupported, nil
}
