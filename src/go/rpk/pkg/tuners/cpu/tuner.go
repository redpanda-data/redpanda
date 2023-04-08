// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package cpu

import (
	"fmt"
	"strconv"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/system"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/executors"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/executors/commands"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/irq"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/utils"
	"github.com/spf13/afero"
	"go.uber.org/zap"
)

type tuner struct {
	cpuMasks      irq.CPUMasks
	grub          system.Grub
	rebootAllowed bool
	cores         uint
	pus           uint
	fs            afero.Fs
	executor      executors.Executor
}

func NewCPUTuner(
	cpuMasks irq.CPUMasks,
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
	zap.L().Sugar().Debug("Running CPU tuner...")
	allCpusMask, err := tuner.cpuMasks.GetAllCpusMask()
	if err != nil {
		return tuners.NewTuneError(err)
	}
	tuner.cores, err = tuner.cpuMasks.GetNumberOfCores(allCpusMask)
	if err != nil {
		return tuners.NewTuneError(err)
	}
	tuner.pus, err = tuner.cpuMasks.GetNumberOfPUs(allCpusMask)
	zap.L().Sugar().Debugf("Running on system with '%d' cores and '%d' PUs",
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
	zap.L().Sugar().Debugf("Getting max allowed CState")
	// Possible errors while reading max_cstate:
	// File doesn't exist or reading error.
	lines, err := utils.ReadFileLines(tuner.fs,
		"/sys/module/intel_idle/parameters/max_cstate")
	// We return maxCstate = 0 when any of the above errors occurred.
	if err != nil {
		return 0, nil //nolint:nilerr //We don't want to interrupt tune execution if any of the above errors oc	curred
	}
	if len(lines) == 1 {
		cState, err := strconv.Atoi(lines[0])
		if err != nil {
			return 0, err
		}
		return uint(cState), nil
	}
	// Only stop tune execution (i.e return error) if max_cstate file length is unsupported.
	return 0, fmt.Errorf("Unsupported length of 'max_cstate' file")
}

func (tuner *tuner) disableCStates() error {
	fmt.Println("Disabling CPU C-States ")
	return tuner.grub.AddCommandLineOptions(
		[]string{
			"intel_idle.max_cstate=0",
			"processor.max_cstate=1",
		})
}

func (tuner *tuner) checkIfPStateIsEnabled() (bool, error) {
	zap.L().Sugar().Debugf("Checking if Intel P-States are enabled")
	lines, err := utils.ReadFileLines(tuner.fs,
		"/sys/devices/system/cpu/cpu0/cpufreq/scaling_driver")
	if err != nil {
		return false, err
	}

	if len(lines) == 0 {
		return false, fmt.Errorf("Unable to read P-State status")
	}
	return lines[0] == "intel_pstate", nil
}

func (tuner *tuner) disablePStates() error {
	fmt.Println("Disabling CPU P-States")
	/* According to the Intel's documentation disabling P-States
	   (only available in Xenon CPUs) sets the cores frequency to constant
	   max non Turbo value (max non turbo P-State).
	   In here we disable the intel_pstate module in odrer to fallback to
	   acpi_cpufreq module.
	*/

	return tuner.grub.AddCommandLineOptions([]string{"intel_pstate=disable"})
}

func (tuner *tuner) setupCPUGovernors() error {
	zap.L().Sugar().Debugf("Setting up ACPI based CPU governors")
	if exists, _ := afero.Exists(tuner.fs, "/sys/devices/system/cpu/cpufreq/boost"); exists {
		err := tuner.executor.Execute(
			commands.NewWriteFileCmd(tuner.fs,
				"/sys/devices/system/cpu/cpufreq/boost", "0"))
		if err != nil {
			return err
		}
	} else {
		zap.L().Sugar().Debugf("CPU frequency boost is not available in this system")
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
			zap.L().Sugar().Debugf("Unable to set CPU governor policy for CPU %d", i)
		}
	}
	return nil
}
