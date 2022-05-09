// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package tuners

import (
	"time"

	"github.com/pkg/errors"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/os"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/system/systemd"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/executors"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/executors/commands"
	"github.com/spf13/afero"
)

const (
	fstrim            = "fstrim"
	unitPrefix        = "redpanda-"
	timerName         = fstrim + ".timer"
	serviceName       = fstrim + ".service"
	customTimerName   = unitPrefix + timerName
	customServiceName = unitPrefix + serviceName

	fstrimTimer = `[Unit]
Description=Discard unused blocks once a week
Documentation=man:fstrim

[Timer]
OnCalendar=weekly
AccuracySec=1h
Persistent=true

[Install]
WantedBy=timers.target
`

	timeout = 500 * time.Millisecond
)

type fstrimTuner struct {
	fs       afero.Fs
	executor executors.Executor
}

func NewFstrimTuner(fs afero.Fs, executor executors.Executor) Tunable {
	return &fstrimTuner{fs: fs, executor: executor}
}

func (*fstrimTuner) CheckIfSupported() (bool, string) {
	// Check that systemd is available
	c, err := systemd.NewDbusClient()
	if err != nil {
		return false, err.Error()
	}
	defer c.Shutdown()

	_, err = whichFstrim(os.NewProc(), timeout)
	if err != nil {
		return false, err.Error()
	}
	return true, ""
}

func (t *fstrimTuner) Tune() TuneResult {
	c, err := systemd.NewDbusClient()
	if err != nil {
		return NewTuneError(err)
	}
	defer c.Shutdown()
	return tuneFstrim(t.fs, t.executor, c, os.NewProc())
}

func tuneFstrim(
	fs afero.Fs, exe executors.Executor, c systemd.Client, proc os.Proc,
) TuneResult {
	// Check if the default timer (fstrim.timer) is available.
	defaultAvailable, err := startIfAvailable(exe, c, timerName)
	if err != nil {
		return NewTuneError(err)
	}
	if defaultAvailable {
		return NewTuneResult(false)
	}

	// If the default timer isn't installed, check if the rpk-supplied one is.
	customAvailable, err := startIfAvailable(exe, c, customTimerName)
	if err != nil {
		return NewTuneError(err)
	}
	if customAvailable {
		return NewTuneResult(false)
	}
	// Other wise, install an rpk-provided fstrim service.
	fstrimBinPath, err := whichFstrim(proc, timeout)
	if err != nil {
		return NewTuneError(err)
	}
	fstrimService := renderFstrimService(fstrimBinPath)
	err = installSystemdUnit(fs, exe, c, fstrimService, customServiceName)
	if err != nil {
		return NewTuneError(err)
	}
	// Install the rpk-provided fstrim timer.
	err = installSystemdUnit(fs, exe, c, fstrimTimer, customTimerName)
	if err != nil {
		// If the timer can't be created, try removing the service too.
		rmErr := fs.Remove(systemd.UnitPath(customServiceName))
		if rmErr != nil {
			err = errors.Wrap(err, rmErr.Error())
		}
		return NewTuneError(err)
	}
	// Start the timer.
	err = startSystemdUnit(exe, c, customTimerName)
	if err != nil {
		return NewTuneError(err)
	}
	return NewTuneResult(false)
}

func NewFstrimChecker() Checker {
	return NewEqualityChecker(
		FstrimChecker,
		"Fstrim systemd service and timer active",
		Warning,
		true,
		func() (interface{}, error) {
			c, err := systemd.NewDbusClient()
			if err != nil {
				return false, err
			}
			defer c.Shutdown()
			// Check if the distro fstrim timer is available & active.
			defLoadState, defActiveState, err := c.UnitState(timerName)
			if err != nil {
				return false, err
			}
			if systemd.IsLoaded(defLoadState) {
				return systemd.IsActive(defActiveState), nil
			}
			// Otherwise, check if the rpk-provided timer is available
			// & active (i.e. if the tuner was run previously and
			// installed it).
			_, customActiveState, err := c.UnitState(customTimerName)
			if err != nil {
				return false, err
			}
			return systemd.IsActive(customActiveState), nil
		},
	)
}

// Returns true if the unit is available, and an error if it couldn't be started.
func startIfAvailable(
	executor executors.Executor, c systemd.Client, name string,
) (bool, error) {
	loadState, activeState, err := c.UnitState(name)
	if err != nil {
		return false, err
	}
	// If it's not loaded, then we can't start it.
	if !systemd.IsLoaded(loadState) {
		return false, nil
	}
	// If it's active, there's nothing left to do.
	if systemd.IsActive(activeState) {
		return true, nil
	}
	// If the timer is available but not started, start it.
	err = startSystemdUnit(executor, c, name)
	return err == nil, err
}

func installSystemdUnit(
	fs afero.Fs, executor executors.Executor, c systemd.Client, body, name string,
) error {
	cmd, err := commands.NewInstallSystemdUnitCmd(c, fs, body, name)
	if err != nil {
		return err
	}
	return executor.Execute(cmd)
}

func startSystemdUnit(
	executor executors.Executor, c systemd.Client, name string,
) error {
	cmd, err := commands.NewStartSystemdUnitCmd(c, name)
	if err != nil {
		return err
	}
	return executor.Execute(cmd)
}

func whichFstrim(proc os.Proc, timeout time.Duration) (string, error) {
	cmd := os.NewCommands(proc)
	return cmd.Which(fstrim, timeout)
}

func renderFstrimService(fstrimBinPath string) string {
	return `[Unit]
Description=Discard unused blocks on filesystems from /etc/fstab
Documentation=man:fstrim(8)

[Service]
Type=oneshot
ExecStart=` + fstrimBinPath + ` --fstab --verbose --quiet
ProtectSystem=strict
ProtectHome=read-only
PrivateDevices=no
PrivateNetwork=yes
PrivateUsers=no
ProtectKernelTunables=yes
ProtectKernelModules=yes
ProtectControlGroups=yes
MemoryDenyWriteExecute=yes
SystemCallFilter=@default @file-system @basic-io @system-service
`
}
