package cpu

import (
	"fmt"
	"strconv"
	"vectorized/os"
	"vectorized/tuners"
	"vectorized/tuners/irq"
	"vectorized/utils"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
)

type tuner struct {
	tuners.Tunable
	cpuMasks      irq.CpuMasks
	grub          os.Grub
	rebootAllowed bool
	cores         uint
	pus           uint
	fs            afero.Fs
}

func NewCpuTuner(
	cpuMasks irq.CpuMasks, grub os.Grub, fs afero.Fs, rebootAllowed bool,
) tuners.Tunable {
	return &tuner{
		cpuMasks:      cpuMasks,
		grub:          grub,
		fs:            fs,
		rebootAllowed: rebootAllowed,
	}
}

func (tuner *tuner) Tune() tuners.TuneResult {
	var err error
	grubUpdated := false
	log.Info("Running CPU tuner...")
	allCpusMask, err := tuner.cpuMasks.GetAllCpusMask()
	if err != nil {
		return tuners.NewTuneError(err)
	}
	tuner.cores, err = tuner.cpuMasks.GetNumberOfCores(allCpusMask)
	tuner.pus, err = tuner.cpuMasks.GetNumberOfPUs(allCpusMask)
	log.Debugf("Running on system with '%d' cores and '%d' PUs",
		tuner.cores, tuner.pus)
	if err != nil {
		return tuners.NewTuneError(err)
	}

	if tuner.isHtEnabled() {
		err = tuner.disableHt()
		if err != nil {
			return tuners.NewTuneError(err)
		}
		if tuner.rebootAllowed {
			err = tuner.addNoHtBootOption()
			if err != nil {
				return tuners.NewTuneError(err)
			}
			grubUpdated = true
		}
	}
	if tuner.rebootAllowed {
		maxCState, err := tuner.getMaxCState()
		if err != nil {
			return tuners.NewTuneError(err)
		}
		if maxCState != 0 {
			err = tuner.disableCStates()
			grubUpdated = true
			if err != nil {
				return tuners.NewTuneError(err)
			}
		}
		pStatesEnabled, err := tuner.checkIfPStateIsEnabled()

		if pStatesEnabled {
			err = tuner.disablePStates()
			grubUpdated = true
			if err != nil {
				return tuners.NewTuneError(err)
			}
		}
	}
	if grubUpdated {
		err = tuner.grub.MakeConfig()
		if err != nil {
			return tuners.NewTuneError(err)
		}
		return tuners.NewTuneResult(true)
	}

	err = tuner.setupCPUGovernors()
	if err != nil {
		return tuners.NewTuneError(err)
	}

	return tuners.NewTuneResult(false)
}

func (tuner *tuner) CheckIfSupported() (supported bool, reason string) {
	hwLocSupported := tuner.cpuMasks.IsSupported()
	if !hwLocSupported {
		return false, "Unable to find 'hwloc' library"
	}
	return true, ""
}

func (tuner *tuner) isHtEnabled() bool {
	log.Debug("Checking Intel Hyper Threading status")
	return tuner.cores != tuner.pus
}

func (tuner *tuner) disableHt() error {
	log.Debug("Disabling Hyper Threading")
	for i := uint(0); i < tuner.cores; i = i + 1 {
		coreIds, err := tuner.cpuMasks.GetLogicalCoreIdsFromPhysCore(i)
		if err != nil {
			return err
		}
		if len(coreIds) != 2 {
			return fmt.Errorf("number of PU per core different than 2, " +
				"Unable to disable HT")
		}
		toDisable := coreIds[1]
		log.Debugf("Disabling virtual core '%d'", toDisable)
		err = utils.WriteFileLines(tuner.fs, []string{"0"},
			fmt.Sprintf("/sys/devices/system/cpu/cpu%d/online", toDisable))
		if err != nil {
			return err
		}
	}
	return nil
}

func (tuner *tuner) addNoHtBootOption() error {
	return tuner.grub.AddCommandLineOptions([]string{"noht"})
}

func (tuner *tuner) getMaxCState() (uint, error) {
	log.Debugf("Getting max allowed CState")
	lines, err := utils.ReadFileLines(tuner.fs,
		"/sys/module/intel_idle/parameters/max_cstate")
	if err != nil {
		return 0, err
	}
	if len(lines) == 1 {
		cState, err := strconv.Atoi(lines[0])
		if err != nil {
			return 0, err
		}
		return uint(cState), nil
	}
	return 0, fmt.Errorf("Unsuported length of 'max_cstate' file")
}

func (tuner *tuner) disableCStates() error {
	log.Info("Disabling CPU C-States ")
	return tuner.grub.AddCommandLineOptions(
		[]string{"intel_idle.max_cstate=0",
			"processor.max_cstate=1",
		})
}

func (tuner *tuner) checkIfPStateIsEnabled() (bool, error) {
	log.Debugf("Checking if Intel P-States are enabled")
	lines, err := utils.ReadFileLines(tuner.fs,
		"/sys/devices/system/cpu/cpu0/cpufreq/scaling_driver")
	if err != nil {
		return false, nil
	}

	if len(lines) == 0 {
		return false, fmt.Errorf("Unable to read P-State status")
	}
	return lines[0] == "intel_pstate", nil
}

func (tuner *tuner) disablePStates() error {
	log.Info("Disabling CPU P-States")
	/* According to the Intel's documentation disabling P-States
	   (only available in Xenon CPUs) sets the cores frequency to constant
	   max non Turbo value (max non turbo P-State).
	   In here we disable the intel_pstate module in odrer to fallback to
	   acpi_cpufreq module.
	*/

	return tuner.grub.AddCommandLineOptions([]string{"intel_pstate=disable"})
}

func (tuner *tuner) setupCPUGovernors() error {
	log.Debugf("Setting up ACPI based CPU governors")
	if utils.FileExists(tuner.fs, "/sys/devices/system/cpu/cpufreq/boost") {
		err := utils.WriteFileLines(tuner.fs, []string{"0"},
			"/sys/devices/system/cpu/cpufreq/boost")
		if err != nil {
			return err
		}
	} else {
		log.Infof("CPU frequency boost is not available in this system")
	}
	for i := uint(0); i < tuner.cores; i = i + 1 {
		policyPath := fmt.Sprintf(
			"/sys/devices/system/cpu/cpufreq/policy%d/scaling_governor", i)
		if utils.FileExists(tuner.fs, policyPath) {
			err := utils.WriteFileLines(tuner.fs, []string{"performance"},
				policyPath)
			if err != nil {
				return err
			}
		} else {
			log.Warnf("Unable to set CPU governor policy for CPU %d", i)
		}
	}
	return nil
}
