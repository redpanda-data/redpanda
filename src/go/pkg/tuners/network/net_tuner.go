package network

import (
	"fmt"
	"net"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"vectorized/tuners"
	"vectorized/tuners/irq"
	"vectorized/utils"

	"github.com/lorenzosaino/go-sysctl"
	"github.com/safchain/ethtool"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
)

type NetTuner struct {
	irqDeviceInfo       irq.DeviceInfo
	cpuMasks            irq.CpuMasks
	irqBalanceService   irq.BalanceService
	irqProcFile         irq.ProcFile
	mode                irq.Mode
	ethTool             *ethtool.Ethtool
	baseCpuMask         string
	computationsCPUMask string
	irqCPUMask          string
	nic                 string
	slaves              []string
	deviceIRQs          map[string][]string
	nicIsBondIface      bool
	nicIsHWIface        bool
	rfsTableSize        uint
	driverMaxRssQueues  map[string]uint
	procFileLines       map[string]string
	fs                  afero.Fs
}

func NewNetTuner(
	mode irq.Mode,
	cpuMask string,
	nic string,
	fs afero.Fs,
	irqDeviceInfo irq.DeviceInfo,
	cpuMasks irq.CpuMasks,
	irqBalanceService irq.BalanceService,
	irqProcFile irq.ProcFile,
	ethTool *ethtool.Ethtool,
) tuners.Tunable {

	log.Debugf("Creating network performance tuner with mode '%s', cpu mask '%s' and NIC '%s'",
		mode, cpuMask, nic)

	return &NetTuner{
		mode:              mode,
		baseCpuMask:       cpuMask,
		irqDeviceInfo:     irqDeviceInfo,
		cpuMasks:          cpuMasks,
		irqBalanceService: irqBalanceService,
		irqProcFile:       irqProcFile,
		ethTool:           ethTool,
		rfsTableSize:      32768,
		nic:               nic,
		driverMaxRssQueues: map[string]uint{
			"ixgbe":   16,
			"ixgbevf": 4,
			"i40e":    64,
			"i40evf":  16},
		fs: fs}
}

func (tuner *NetTuner) Tune() tuners.TuneResult {
	var err error
	tuner.nicIsBondIface = tuner.checkNicIsBondIface(tuner.nic)
	tuner.nicIsHWIface = tuner.checkNicIsHWIface(tuner.nic)

	tuner.mode, err = tuner.getDefaultMode()
	tuner.baseCpuMask, err = tuner.cpuMasks.BaseCpuMask(tuner.baseCpuMask)
	tuner.procFileLines, err = tuner.irqProcFile.GetIRQProcFileLinesMap()
	if err != nil {
		return tuners.NewTuneError(err)
	}
	tuner.computationsCPUMask, err = tuner.cpuMasks.CpuMaskForComputations(tuner.mode, tuner.baseCpuMask)
	tuner.irqCPUMask, err = tuner.cpuMasks.CpuMaskForIRQs(tuner.mode, tuner.baseCpuMask)
	if err != nil {
		return tuners.NewTuneError(err)
	}
	tuner.deviceIRQs, err = tuner.getDevicesIRQs()
	if err != nil {
		return tuners.NewTuneError(err)
	}
	if !tuner.nicIsBondIface && !tuner.nicIsHWIface {
		return tuners.NewTuneError(fmt.Errorf("virtual device '%s' is not supported", tuner.nic))
	}
	tuner.slaves = tuner.getSlaves(tuner.nic)
	if err = tuner.irqBalanceService.BanIRQsAndRestart(tuner.getAllIRQs()); err != nil {
		return tuners.NewTuneError(err)
	}
	log.WithFields(log.Fields{
		"Mode":              tuner.mode,
		"NIC":               tuner.nic,
		"IRQs Mask":         tuner.irqCPUMask,
		"Computations Mask": tuner.computationsCPUMask,
	}).Info("Tuning NIC...")

	if tuner.nicIsHWIface {

		if err = tuner.setupHwInface(tuner.nic); err != nil {
			return tuners.NewTuneError(err)
		}
	} else {
		if err = tuner.setupBondingIface(); err != nil {
			return tuners.NewTuneError(err)
		}
	}
	log.Infof("Increasing socket listen() backlog")
	err = utils.WriteFileLines(tuner.fs,
		[]string{"4096"}, "/proc/sys/net/core/somaxconn")
	if err != nil {
		return tuners.NewTuneError(err)
	}
	log.Infof("Increase the maximum number of remembered connection requests")
	err = utils.WriteFileLines(tuner.fs,
		[]string{"4096"}, "/proc/sys/net/ipv4/tcp_max_syn_backlog")
	if err != nil {
		return tuners.NewTuneError(err)
	}
	return tuners.NewTuneResult(false)
}

func (tuner *NetTuner) CheckIfSupported() (supported bool, reason string) {
	if !tuner.cpuMasks.IsSupported() {
		return false, "Unable to calculate CPU masks " +
			"required for IRQs tuner. Please install 'hwloc'"
	}
	if tuner.nic == "" {
		return false, "'nic' paramter is required for Network Tuner"
	}
	ifaces, err := net.Interfaces()
	if err != nil {
		return false, err.Error()
	}
	for _, iface := range ifaces {
		if tuner.nic == iface.Name {
			return true, ""
		}
	}
	return false, fmt.Sprintf("Interface '%s' not found", tuner.nic)
}

func (tuner *NetTuner) getAllIRQs() []string {
	irqsSet := map[string]bool{}
	for _, irqs := range tuner.deviceIRQs {
		for _, irq := range irqs {
			irqsSet[irq] = true
		}
	}
	return utils.GetKeys(irqsSet)
}

func (tuner *NetTuner) getDefaultModeForHwInterface(
	nic string,
) (irq.Mode, error) {
	rxQueuesCount, err := tuner.getRxQueueCount(nic)
	if err != nil {
		return "", err
	}
	log.Debugf("Calculating default mode for '%s'", nic)
	numOfCores, err := tuner.cpuMasks.GetNumberOfCores(tuner.baseCpuMask)
	if err != nil {
		return "", err
	}
	numOfPUs, err := tuner.cpuMasks.GetNumberOfPUs(tuner.baseCpuMask)
	if err != nil {
		return "", err
	}
	log.Debugf("Considering '%d' cores and '%d' PUs", numOfCores, numOfPUs)

	if numOfPUs <= 4 || rxQueuesCount == uint(numOfPUs) {
		return irq.Mq, nil
	} else if numOfCores <= 4 {
		return irq.Sq, nil
	} else {
		return irq.SqSplit, nil
	}
}

func (tuner *NetTuner) getDefaultMode() (irq.Mode, error) {
	if tuner.mode != irq.Default {
		return tuner.mode, nil
	}
	if tuner.nicIsBondIface {
		defaultMode := irq.Mq
		for _, slave := range tuner.slaves {
			if tuner.checkNicIsHWIface(slave) {
				slaveDefaultMode, err := tuner.getDefaultModeForHwInterface(slave)
				if err != nil {
					return "", err
				}
				if slaveDefaultMode == irq.Sq {
					defaultMode = irq.Sq
				} else if slaveDefaultMode == irq.SqSplit && defaultMode == irq.Mq {
					defaultMode = irq.SqSplit
				}
			}
		}
		return defaultMode, nil
	}

	return tuner.getDefaultModeForHwInterface(tuner.nic)
}

func (tuner *NetTuner) checkNicIsBondIface(nic string) bool {
	log.Debugf("Checking if '%s' is bond interface", nic)
	lines, err := utils.ReadFileLines(tuner.fs, "/sys/class/net/bonding_masters")
	if err != nil {
		return false
	}
	for _, line := range lines {
		if strings.Contains(line, nic) {
			log.Debugf("'%s' is bond interface", nic)
			return true
		}
	}
	return false
}

func (tuner *NetTuner) getSlaves(nic string) []string {
	var slaves []string
	if tuner.nicIsBondIface {
		log.Debugf("Reading slaves of '%s'", nic)
		lines, _ := utils.ReadFileLines(tuner.fs,
			fmt.Sprintf("/sys/class/net/%s/bonding/slaves", nic))
		for _, line := range lines {
			slaves = append(slaves, strings.Split(line, " ")...)
		}
	}
	return slaves
}

func (tuner *NetTuner) checkNicIsHWIface(nic string) bool {
	log.Debugf("Checking if '%s' is HW interface", nic)
	return utils.FileExists(tuner.fs, fmt.Sprintf("/sys/class/net/%s/device", nic))
}

func (tuner *NetTuner) getDevicesIRQs() (map[string][]string, error) {
	deviceToIRQsMap := map[string][]string{}
	var err error
	if tuner.nicIsBondIface {
		for _, slave := range tuner.slaves {
			if tuner.checkNicIsHWIface(slave) {
				deviceToIRQsMap[slave], err = tuner.getNicIRQs(slave)
				if err != nil {
					return nil, err
				}
			}
		}
	} else {
		deviceToIRQsMap[tuner.nic], err = tuner.getNicIRQs(tuner.nic)
		if err != nil {
			return nil, err
		}
	}
	return deviceToIRQsMap, nil
}

func (tuner *NetTuner) getNicIRQs(device string) ([]string, error) {
	log.Debugf("Getting NIC '%s' irqs", device)
	irqs, err := tuner.irqDeviceInfo.GetIRQs(fmt.Sprintf("/sys/class/net/%s/device", device),
		device)
	if err != nil {
		return nil, err
	}
	fastPathIRQsPattern := regexp.MustCompile("-TxRx-|-fp-|-Tx-Rx-")
	var fastPathIRQs []string
	for _, irq := range irqs {

		if fastPathIRQsPattern.MatchString(tuner.procFileLines[irq]) {
			fastPathIRQs = append(fastPathIRQs, irq)
		}
	}
	if len(fastPathIRQs) > 0 {
		sort.Slice(fastPathIRQs,
			func(i, j int) bool {
				return tuner.intelIrqToQueueIdx(fastPathIRQs[i]) < tuner.intelIrqToQueueIdx(fastPathIRQs[j])
			})
		return fastPathIRQs, nil
	} else {
		return irqs, nil
	}
}

func (tuner *NetTuner) intelIrqToQueueIdx(irq string) uint {
	intelFastPathIrqPattern := regexp.MustCompile("-TxRx-(\\d+)")
	fdirPattern := regexp.MustCompile("fdir-TxRx-\\d+")
	procLine := tuner.procFileLines[irq]

	intelFastPathMatch := intelFastPathIrqPattern.FindStringSubmatch(procLine)
	fdirPatternMatch := fdirPattern.FindStringSubmatch(procLine)

	if len(intelFastPathMatch) > 0 && len(fdirPatternMatch) == 0 {
		idx, _ := strconv.Atoi(intelFastPathMatch[1])
		return uint(idx)
	} else {
		return ^uint(0)
	}
}

func (tuner *NetTuner) setupHwInface(nic string) error {
	maxNumOfRxQueues := tuner.maxRxQueueCount(nic)
	allIrqs := tuner.deviceIRQs[nic]
	log.Debugf("Max RSS queues for '%s' - %d", nic, maxNumOfRxQueues)
	if maxNumOfRxQueues < uint(len(allIrqs)) {
		var err error
		numOfRxQueues, err := tuner.getRxQueueCount(nic)
		if err != nil {
			return err
		}
		log.Debugf("Number of Rx queues for '%s' = '%d'", nic, numOfRxQueues)
		log.Infof("Distributing '%s' IRQs handling Rx queues", nic)
		err = tuner.cpuMasks.DistributeIRQs(allIrqs[0:numOfRxQueues], tuner.irqCPUMask)
		if err != nil {
			return err
		}
		log.Infof("Distributing rest of '%s' IRQs", nic)
		err = tuner.cpuMasks.DistributeIRQs(allIrqs[numOfRxQueues:], tuner.irqCPUMask)
		if err != nil {
			return err
		}
	} else {
		log.Infof("Distributing all '%s' IRQs", nic)
		err := tuner.cpuMasks.DistributeIRQs(allIrqs, tuner.irqCPUMask)
		if err != nil {
			return err
		}
	}
	err := tuner.setupRps(nic, tuner.computationsCPUMask)
	if err != nil {
		return err
	}
	err = tuner.setupXps(nic)
	if err != nil {
		return err
	}
	return nil
}

func (tuner *NetTuner) setupRps(nic string, cpuMask string) error {
	log.Debugf("Setting up RPS for '%s' with CPU mask '%s'", nic, cpuMask)
	rps, err := tuner.getRpsCPUs(nic)
	if err != nil {
		return err
	}
	for _, rpsCPU := range rps {
		if err = tuner.cpuMasks.SetMask(rpsCPU, cpuMask); err != nil {
			return err
		}
	}
	tuner.setupRfs(nic)
	return nil
}

func (tuner *NetTuner) setupXps(nic string) error {
	log.Debugf("Setting up XPS for '%s'", nic)
	xpsCpus, err := filepath.Glob(fmt.Sprintf("/sys/class/net/%s/queues/*/xps_cpus", nic))
	masks, err := tuner.cpuMasks.GetDistributionMasks(uint(len(xpsCpus)))
	for i, mask := range masks {
		if err = tuner.cpuMasks.SetMask(xpsCpus[i], mask); err != nil {
			return err
		}
	}
	return err
}

func (tuner *NetTuner) setupRfs(nic string) {
	log.Debugf("Setting up RFS for '%s'", nic)
	rpsLimits, err := filepath.Glob(fmt.Sprintf("/sys/class/net/%s/queues/*/rps_flow_cnt", nic))
	oneQueueLimit := int(tuner.rfsTableSize / uint(len(rpsLimits)))

	log.Infof("Setting net.core.rps_sock_flow_entries to %d", tuner.rfsTableSize)
	err = sysctl.Set("net.core.rps_sock_flow_entries", strconv.Itoa(int(tuner.rfsTableSize)))
	if err != nil {
		log.Infof("Unable to setup 'net.core.rps_sock_flow_entries' - %s", err)
	}
	for _, rfsLimit := range rpsLimits {
		log.Infof("Setting limtit '%d' in '%s'", oneQueueLimit, rfsLimit)
		_ = utils.WriteFileLines(tuner.fs, []string{strconv.Itoa(oneQueueLimit)}, rfsLimit)
	}

	err = tuner.enableNTuple(nic)
	if err != nil {
		log.Infof("Unable to enable ntuple filtering HW offload for '%s'- %s", nic, err)
	}
}

func (tuner *NetTuner) enableNTuple(nic string) error {
	log.Infof("Trying to enable ntuple filtering HW offload for '%s'", nic)
	features, err := tuner.ethTool.Features(nic)
	for featureName := range features {
		if strings.Contains(featureName, "ntuple") {
			log.Debugf("Found 'ntuple' feature '%s' in '%s'", featureName, nic)
			features[featureName] = true
			break
		}
	}
	err = tuner.ethTool.Change(nic, features)
	return err
}

func (tuner *NetTuner) maxRxQueueCount(nic string) uint {
	// Networking drivers serving HW with the known maximum RSS queue limitation (due to lack of RSS bits):

	// ixgbe:   PF NICs support up to 16 RSS queues.
	// ixgbevf: VF NICs support up to 4 RSS queues.
	// i40e:    PF NICs support up to 64 RSS queues.
	// i40evf:  VF NICs support up to 16 RSS queues.
	log.Debugf("Checking max RSS queues count for '%s'", nic)

	driverName, _ := tuner.ethTool.DriverName(nic)
	log.Debugf("NIC '%s' uses '%s' driver", nic, driverName)
	if maxQueues, present := tuner.driverMaxRssQueues[driverName]; present {
		return maxQueues
	} else {
		return ^uint(0)
	}
}

func (tuner *NetTuner) getRxQueueCount(nic string) (uint, error) {
	irqsCount := len(tuner.deviceIRQs[nic])
	rpsCpus, err := tuner.getRpsCPUs(nic)
	if err != nil {
		return 0, utils.ChainedError(err, "Unable to get the RPS number")
	}
	rxQueuesCount := uint(len(rpsCpus))
	log.Debugf("Getting number of Rx queues for '%s'", nic)
	if rxQueuesCount == 0 {
		rxQueuesCount = uint(irqsCount)
	}

	maxRxQueueCount := tuner.maxRxQueueCount(nic)
	if rxQueuesCount < maxRxQueueCount {
		return rxQueuesCount, nil
	}
	return maxRxQueueCount, nil
}

func (tuner *NetTuner) getRpsCPUs(nic string) ([]string, error) {
	// Prints all rps_cpus files names for the given HW interface.
	// There is a single rps_cpus file for each RPS queue and there is a single RPS
	// queue for each HW Rx queue. Each HW Rx queue should have an IRQ.
	// Therefore the number of these files is equal to the number of fast path Rx irqs for this interface.

	rps, err := filepath.Glob(fmt.Sprintf("/sys/class/net/%s/queues/*/rps_cpus", nic))
	if err != nil {
		return nil, err
	}
	return rps, nil
}

func (tuner *NetTuner) setupBondingIface() error {
	for _, slave := range tuner.slaves {
		if tuner.checkNicIsHWIface(slave) {
			if err := tuner.setupHwInface(slave); err != nil {
				return err
			}
		} else {
			log.Infof("Skipping setup of slave '%s' interface", slave)
		}
	}
	return nil
}
