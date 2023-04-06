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
	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func NewStopCommand(fs afero.Fs) *cobra.Command {
	var (
		configFile string
		timeout    time.Duration
	)
	command := &cobra.Command{
		Use:   "stop",
		Short: "Stop redpanda",
		Long: `Stop a local redpanda process. 'rpk stop'
first sends SIGINT, and waits for the specified timeout. Then, if redpanda
hasn't stopped, it sends SIGTERM. Lastly, it sends SIGKILL if it's still
running.`,
		Run: func(cmd *cobra.Command, args []string) {
			p := config.ParamsFromCommand(cmd)
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			err = executeStop(fs, cfg, timeout)
			out.MaybeDieErr(err)
		},
	}
	command.Flags().StringVar(
		&configFile,
		"config",
		"",
		"Redpanda config file, if not set the file will be searched for"+
			" in $PWD or /etc/redpanda/redpanda.yaml.",
	)
	command.Flags().DurationVar(
		&timeout,
		"timeout",
		5*time.Second,
		"The maximum amount of time to wait for redpanda to stop,"+
			"after each signal is sent. The value passed is a"+
			"sequence of decimal numbers, each with optional"+
			" fraction and a unit suffix, such as '300ms', '1.5s'"+
			" or '2h45m'. Valid time units are 'ns', 'us' (or"+
			" 'µs'), 'ms', 's', 'm', 'h'",
	)
	return command
}

func executeStop(fs afero.Fs, cfg *config.Config, timeout time.Duration) error {
	pidFile := cfg.PIDFile()
	isLocked, err := os.CheckLocked(pidFile)
	if err != nil {
		log.Debugf("error checking if the PID file is locked: %v", err)
	}
	if !isLocked {
		// If the file isn't locked or doesn't exist, that means
		// redpanda isn't running, so there's nothing to do.
		log.Debugf(
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
		log.Debugf(
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
				log.Error(err)
			} else if !isRunning {
				stoppedRunning <- true
				return
			}
		}
	}
}
