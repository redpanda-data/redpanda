// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

//go:build linux
// +build linux

package redpanda

import (
	"errors"
	"strconv"
	"syscall"
	"time"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/os"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/utils"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

func NewStopCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var timeout time.Duration
	cmd := &cobra.Command{
		Use:   "stop",
		Short: "Stop redpanda",
		Long: `Stop a local redpanda process. 'rpk stop'
first sends SIGINT, and waits for the specified timeout. Then, if redpanda
hasn't stopped, it sends SIGTERM. Lastly, it sends SIGKILL if it's still
running.`,
		Args: cobra.NoArgs,
		Run: func(_ *cobra.Command, _ []string) {
			y, err := p.LoadVirtualRedpandaYaml(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)

			err = executeStop(fs, y, timeout)
			out.MaybeDieErr(err)
		},
	}
	cmd.Flags().DurationVar(&timeout, "timeout", 5*time.Second, "The maximum amount of time to wait for redpanda to stop after each signal is sent (e.g. 300ms, 1.5s, 2h45m)")
	return cmd
}

func executeStop(fs afero.Fs, y *config.RedpandaYaml, timeout time.Duration) error {
	pidFile := y.PIDFile()
	isLocked, err := os.CheckLocked(pidFile)
	if err != nil {
		zap.L().Sugar().Debugf("error checking if the PID file is locked: %v", err)
	}
	if !isLocked {
		// If the file isn't locked or doesn't exist, that means
		// redpanda isn't running, so there's nothing to do.
		zap.L().Sugar().Debugf(
			"'%s' isn't locked, which means redpanda isn't running. Nothing to do.",
			pidFile,
		)
		return nil
	}
	pidStr, err := utils.ReadEnsureSingleLine(fs, pidFile)
	if err != nil {
		return err
	}
	pid, err := strconv.Atoi(pidStr)
	if err != nil {
		return err
	}
	return signalAndWait(pid, timeout)
}

func signalAndWait(pid int, timeout time.Duration) error {
	var f func(int, []syscall.Signal) error
	f = func(pid int, signals []syscall.Signal) error {
		if len(signals) == 0 {
			return errors.New("process couldn't be terminated")
		}
		signal := signals[0]
		pending := signals[1:]
		zap.L().Sugar().Debugf(
			"Sending %s to redpanda (PID %d).\n",
			signal,
			pid,
		)
		err := syscall.Kill(pid, signal)
		if err != nil {
			return err
		}
		stopPolling := make(chan bool)
		stoppedRunning := make(chan bool)
		go poll(pid, stopPolling, stoppedRunning)

		timedOut := false
		select {
		case <-time.After(timeout):
			stopPolling <- true
			timedOut = true
		case <-stoppedRunning:
		}
		close(stopPolling)
		close(stoppedRunning)
		if timedOut {
			return f(pid, pending)
		}
		return nil
	}
	return f(pid, []syscall.Signal{syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL})
}

func poll(pid int, stop <-chan bool, stoppedRunning chan<- bool) {
	for {
		select {
		case <-stop:
			return
		default:
			isRunning, err := os.IsRunningPID(afero.NewOsFs(), pid)
			if err != nil {
				zap.L().Sugar().Error(err)
			} else if !isRunning {
				stoppedRunning <- true
				return
			}
		}
	}
}
