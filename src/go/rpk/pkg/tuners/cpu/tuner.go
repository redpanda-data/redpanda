package cpu

import (
	"fmt"
	"strconv"
	"vectorized/pkg/system"
	"vectorized/pkg/tuners"
	"vectorized/pkg/tuners/executors"
	"vectorized/pkg/tuners/executors/commands"
	"vectorized/pkg/tuners/irq"
	"vectorized/pkg/utils"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
)

type tuner struct {
	cpuMasks      irq.CpuMasks
	grub          system.Grub
	rebootAllowed bool
	cores         uint
	pus           uint
	fs            afero.Fs
	executor      executors.Executor
}

func NewCpuTuner(
	cpuMasks irq.CpuMasks,
	grub system.Grub,
	fs afero.Fs,
	rebootAllowed bool,
	executor executors.Executor,
) tuners.Tunable {
	return &tuner{
		cpuMasks:      cpuMasks,
		grub:          grub,
		fs:            fs,
		rebootAllowed: rebootAllowed,
		executor:      executor,
	}
}

func (tuner *tuner) Tune() tuners.TuneResult {
	grubUpdated := false
	log.Debug("Running CPU tuner...")
	allCpusMask, err := tuner.cpuMasks.GetAllCpusMask()
	if err != nil {
		return tuners.NewTuneError(err)
	}
	tuner.cores, err = tuner.cpuMasks.GetNumberOfCores(allCpusMask)
	if err != nil {
		return tuners.NewTuneError(err)
	}
	tuner.pus, err = tuner.cpuMasks.GetNumberOfPUs(allCpusMask)
	log.Debugf("Running on system with '%d' cores and '%d' PUs",
		tuner.cores, tuner.pus)
	if err != nil {
		return tuners.NewTuneError(err)
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
		if err != nil {
			return tuners.NewTuneError(err)
		}
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
	if exists, _ := afero.Exists(tuner.fs, "/sys/devices/system/cpu/cpufreq/boost"); exists {
		err := tuner.executor.Execute(
			commands.NewWriteFileCmd(tuner.fs,
				"/sys/devices/system/cpu/cpufreq/boost", "0"))
		if err != nil {
			return err
		}
	} else {
		log.Debugf("CPU frequency boost is not available in this system")
	}
	for i := uint(0); i < tuner.cores; i = i + 1 {
		policyPath := fmt.Sprintf(
			"/sys/devices/system/cpu/cpufreq/policy%d/scaling_governor", i)
		if exists, _ := afero.Exists(tuner.fs, policyPath); exists {
			err := tuner.executor.Execute(
				commands.NewWriteFileCmd(tuner.fs, policyPath, "performance"))
			if err != nil {
				return err
			}
		} else {
			log.Debugf("Unable to set CPU governor policy for CPU %d", i)
		}
	}
	return nil
}
