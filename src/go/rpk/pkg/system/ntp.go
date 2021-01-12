// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package system

import (
	"errors"
	"fmt"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/os"
)

const reachMask int64 = 1

type NtpQuery interface {
	IsNtpSynced() (bool, error)
}

func NewNtpQuery(timeout time.Duration, fs afero.Fs) NtpQuery {
	return &ntpQuery{
		timeout:	timeout,
		fs:		fs,
		proc:		os.NewProc(),
	}
}

type ntpQuery struct {
	timeout	time.Duration
	fs	afero.Fs
	proc	os.Proc
}

func (q *ntpQuery) IsNtpSynced() (bool, error) {
	_, err := exec.LookPath("timedatectl")
	if err != nil {
		log.Debug(err)
	}
	synced, err := q.checkWithTimedateCtl()
	if err != nil {
		log.Debug(err)
	} else {
		return synced, nil
	}
	_, err = exec.LookPath("ntpstat")
	if err != nil {
		log.Debug(err)
	}
	synced, err = q.checkWithNtpstat()
	if err != nil {
		log.Debug(err)
	} else {
		return synced, nil
	}

	return false, errors.New("couldn't check NTP with timedatectl or ntpstat")
}

func (q *ntpQuery) checkWithTimedateCtl() (bool, error) {
	output, err := q.proc.RunWithSystemLdPath(q.timeout, "timedatectl", "status")
	if err != nil {
		return false, err
	}
	clockSyncedLinePattern := regexp.MustCompile("^.* synchronized: (.*)$")
	for _, outLine := range output {
		log.Debugf("Parsing timedatectl output '%s'", outLine)
		matches := clockSyncedLinePattern.FindAllStringSubmatch(outLine, -1)
		if matches != nil {
			return matches[0][1] == "yes", nil
		}
	}
	return false, errors.New("NTP sync information not found in timedatectl output")
}

func (q *ntpQuery) checkWithNtpstat() (bool, error) {
	log.Debugf("Checking NTP sync with ntpstat")
	_, err := q.proc.RunWithSystemLdPath(q.timeout, "ntpstat")
	// ntpstat exits with status other than 0 when NTP is not synced
	if err != nil {
		log.Debugf("ntpstat returned an error '%s'", err.Error())
		return false, err
	}
	return true, nil
}

func (q *ntpQuery) checkWithNtpq() (bool, error) {
	log.Debugf("Checking NTP sync with ntpq")
	output, err := q.proc.RunWithSystemLdPath(q.timeout, "ntpq", "-p")
	if err != nil {
		log.Debugf("ntpq returned an error: '%s'", err.Error())
		return false, err
	}
	return checkNtpqOutput(output)
}

func checkNtpqOutput(output []string) (bool, error) {
	// Example output from ntpq -p:
	//       remote           refid     st t when poll reach   delay   offset  jitter
	// ==============================================================================
	// *metadata.google 71.79.79.71      2 u  115  128  377    0.733  -16.588   2.339
	// [more hosts...]
	//
	// This function checks every host's "reach" value, (which is an octal
	// representing the last eight tries to sync the current node, with 1
	// representing success and 0 representing failure),
	// returning true if at least one of the last attempts to reach a peer
	// was successful.

	// The first 2 lines are headers, and the rest are info about
	// the other hosts, so there have to be 3 lines minimum
	if len(output) < 3 {
		return false, fmt.Errorf(
			"ntpq failed, output:\n'%v'", strings.Join(output, "\n"),
		)
	}
	synced := false
	for _, line := range output[2:] {
		columns := strings.Fields(line)
		if len(columns) != 10 {
			continue
		}
		reach := columns[6]
		val, err := strconv.ParseInt(reach, 8, 0)
		if err != nil {
			continue
		}
		if val&reachMask == 1 {
			synced = synced || true
		}

	}
	return synced, nil
}
