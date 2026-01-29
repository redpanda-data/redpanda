// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

//go:build linux

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

type IrqInfo struct {
	Num      int
	ProcLine string
	// IndexFunc returns the queue index for a given IRQ
	// This will effectively always parse the queue index out of the name from the proc line
	indexFunc func(IrqInfo) int
}

func (irqInfo *IrqInfo) QueueIndex() int {
	return irqInfo.indexFunc(*irqInfo)
}

func IrqInfosToIDs(irqs []IrqInfo) []int {
	ids := make([]int, 0, len(irqs))
	for _, irq := range irqs {
		ids = append(ids, irq.Num)
	}
	return ids
}

type Nic interface {
	IsHwInterface() bool
	IsBondIface() bool
	Slaves() []Nic
	GetIRQs() ([]IrqInfo, error)
	GetMaxRxQueueCount() (int, error)
	GetRxQueueCount() (int, error)
	GetRpsCPUFiles() ([]string, error)
	GetXpsCPUFiles() ([]string, error)
	GetRpsLimitFiles() ([]string, error)
	GetNTupleStatus() (NTupleStatus, error)
	// Do we support lowering RX queues
	// Mostly returns false when driver is unknown
	SupportsRxTxQueueLowering() (bool, error)
	Name() string
}

type nic struct {
	fs            afero.Fs
	irqProcFile   irq.ProcFile
	irqDeviceInfo irq.DeviceInfo
	ethtool       ethtool.EthtoolWrapper
	name          string
	slaves        []Nic
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
		slaves:        getslaves(fs, irqProcFile, irqDeviceInfo, ethtool, name),
	}
}

func (n *nic) Name() string {
	return n.name
}

// We use the existence of lower_* files in /sys/class/net/<nic>/ to determine
// if the NIC is a bond or something bond-like like VLAN interfaces or hyper-v
// NICs.
func getLowerNames(fs afero.Fs, nicName string) []string {
	nicDir := fmt.Sprintf("/sys/class/net/%s", nicName)
	files := utils.ListFilesInPath(fs, nicDir)

	lowers := []string{}
	for _, file := range files {
		if after, ok := strings.CutPrefix(file, "lower_"); ok {
			lowers = append(lowers, after)
		}
	}
	return lowers
}

func (n *nic) IsBondIface() bool {
	return len(n.slaves) > 0
}

func getslaves(fs afero.Fs, irqProcFile irq.ProcFile,
	irqDeviceInfo irq.DeviceInfo,
	ethtool ethtool.EthtoolWrapper,
	name string,
) []Nic {
	slaves := []Nic{}

	zap.L().Sugar().Debugf("Reading slaves of '%s'", name)
	slaveNames := getLowerNames(fs, name)

	for _, name := range slaveNames {
		slaves = append(slaves, NewNic(fs, irqProcFile, irqDeviceInfo, ethtool, name))
	}

	return slaves
}

func (n *nic) Slaves() []Nic {
	return n.slaves
}

func (n *nic) IsHwInterface() bool {
	zap.L().Sugar().Debugf("Checking if '%s' is HW interface", n.name)
	exists, _ := afero.Exists(n.fs, fmt.Sprintf("/sys/class/net/%s/device", n.name))
	return exists
}

type driverSupport struct {
	name           string
	indexFuncMaker func(numIRQs int) func(IrqInfo) int
}

var supportedDrivers = []driverSupport{
	{"ena", func(_ int) func(IrqInfo) int { return intelIrqToQueueIdx }},
	{"virtio", func(_ int) func(IrqInfo) int { return virtioIrqToQueueIdx }},
	{"gve", func(numIRQs int) func(IrqInfo) int {
		return func(irq IrqInfo) int { return gvnicIrqToQueueIdx(irq, numIRQs) }
	}},
	{"mlx5_core", func(_ int) func(IrqInfo) int { return azureHyperVIrqToQueueIdx }},
	{"mana", func(_ int) func(IrqInfo) int { return azureManaIrqToQueueIdx }},
}

func getQueueIndexFunc(driverName string, numIRQs int) func(IrqInfo) int {
	for _, driver := range supportedDrivers {
		if strings.HasPrefix(driverName, driver.name) {
			return driver.indexFuncMaker(numIRQs)
		}
	}

	// Use intel func as the default as that's what we always did and what perf_tune does
	return intelIrqToQueueIdx
}

func (n *nic) SupportsRxTxQueueLowering() (bool, error) {
	driverName, err := n.ethtool.DriverName(n.name)
	if err != nil {
		return false, err
	}

	for _, driver := range supportedDrivers {
		if strings.HasPrefix(driverName, driver.name) {
			return true, nil
		}
	}

	return false, nil
}

func (n *nic) GetIRQs() ([]IrqInfo, error) {
	zap.L().Sugar().Debugf("Getting NIC '%s' IRQs", n.name)
	IRQNums, err := n.irqDeviceInfo.GetIRQs(fmt.Sprintf("/sys/class/net/%s/device", n.name),
		n.name)
	if err != nil {
		return nil, err
	}
	procFileLines, err := n.irqProcFile.GetIRQProcFileLinesMap()
	if err != nil {
		return nil, err
	}

	driverName, err := n.ethtool.DriverName(n.name)
	if err != nil {
		return nil, err
	}

	fastPathIRQsPattern := regexp.MustCompile(`-TxRx-|-fp-|virtio\d+-(input|output)|ntfy-block|gve-ntfy-blk|mlx5_comp\d+|mana_q|-Tx-Rx-|mlx\d+-\d+@`)
	var fastPathIRQNums []int
	for _, irq := range IRQNums {
		if fastPathIRQsPattern.MatchString(procFileLines[irq]) {
			fastPathIRQNums = append(fastPathIRQNums, irq)
		}
	}

	var fastPathIRQs []IrqInfo
	fastPathIndexFunc := getQueueIndexFunc(driverName, len(fastPathIRQNums))
	for _, irq := range fastPathIRQNums {
		fastPathIRQs = append(fastPathIRQs, IrqInfo{Num: irq, ProcLine: procFileLines[irq], indexFunc: fastPathIndexFunc})
	}

	if len(fastPathIRQs) > 0 {
		// Because not all IRQs might be active we want them to be sorted by queue index
		// see nics.go:GetHwInterfaceIRQsDistribution()
		sort.Slice(fastPathIRQs,
			func(i, j int) bool {
				return fastPathIRQs[i].QueueIndex() < fastPathIRQs[j].QueueIndex()
			})
		return fastPathIRQs, nil
	}

	var IRQs []IrqInfo
	indexFunc := getQueueIndexFunc(driverName, len(IRQNums))
	for _, irq := range IRQNums {
		IRQs = append(IRQs, IrqInfo{Num: irq, ProcLine: procFileLines[irq], indexFunc: indexFunc})
	}
	return IRQs, nil
}

func intelIrqToQueueIdx(irq IrqInfo) int {
	intelFastPathIrqPattern := regexp.MustCompile(`-TxRx-(\d+)`)
	fdirPattern := regexp.MustCompile(`fdir-TxRx-\d+`)

	intelFastPathMatch := intelFastPathIrqPattern.FindStringSubmatch(irq.ProcLine)
	fdirPatternMatch := fdirPattern.FindStringSubmatch(irq.ProcLine)

	if len(intelFastPathMatch) > 0 && len(fdirPatternMatch) == 0 {
		idx, _ := strconv.Atoi(intelFastPathMatch[1])
		return idx
	}
	return MaxInt
}

func virtioIrqToQueueIdx(irq IrqInfo) int {
	virtioFastPathIrqPattern := regexp.MustCompile(`virtio\d+-(input|output)\.(\d+)$`)

	virtioMatch := virtioFastPathIrqPattern.FindStringSubmatch(irq.ProcLine)
	if len(virtioMatch) == 3 {
		idx, _ := strconv.Atoi(virtioMatch[2])
		return idx
	}
	return MaxInt
}

func gvnicIrqToQueueIdx(irq IrqInfo, numIRQs int) int {
	// legacy pattern: eth%d-ntfy-block.30
	// New pattern: gve-ntfy-blk30@pci:0000:00:08.0
	newPattern := regexp.MustCompile(`gve-ntfy-blk(\d+)`)

	gvnicMatch := newPattern.FindStringSubmatch(irq.ProcLine)
	if len(gvnicMatch) == 0 {
		// try the legacy pattern
		legacyPattern := regexp.MustCompile(`ntfy-block\.(\d+)$`)
		gvnicMatch = legacyPattern.FindStringSubmatch(irq.ProcLine)
	}
	if len(gvnicMatch) == 2 {
		idx, _ := strconv.Atoi(gvnicMatch[1])
		// https://github.com/torvalds/linux/blob/v6.17/drivers/net/ethernet/google/gve/gve.h#L1082-L1094
		// TX is the lower half, RX the upper half of the IRQs
		return idx % (numIRQs / 2)
	}
	return MaxInt
}

func azureHyperVIrqToQueueIdx(irq IrqInfo) int {
	// Below will only pattern match on Azure but that's fine
	hyperVPattern := regexp.MustCompile(`mlx5_comp(\d+)@`)
	match := hyperVPattern.FindStringSubmatch(irq.ProcLine)
	if len(match) == 2 {
		idx, _ := strconv.Atoi(match[1])
		return idx
	}
	return MaxInt
}

func azureManaIrqToQueueIdx(irq IrqInfo) int {
	manaPattern := regexp.MustCompile(`mana_q(\d+)@`)
	match := manaPattern.FindStringSubmatch(irq.ProcLine)
	if len(match) == 2 {
		idx, _ := strconv.Atoi(match[1])
		return idx
	}
	return MaxInt
}

func (n *nic) GetMaxRxQueueCount() (int, error) {
	// Apparently some NICs advertise more Rx queues but only a subset of them is RSS queues
	// Limited background in https://github.com/scylladb/seastar/commit/d3c6a3685ba695bb2850172f91c8c889a0391e03

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
