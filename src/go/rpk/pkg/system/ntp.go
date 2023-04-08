// Copyright 2020 Redpanda Data, Inc.
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
	"os/exec"
	"regexp"
	"time"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/os"
	"github.com/spf13/afero"
	"go.uber.org/zap"
)

type NtpQuery interface {
	IsNtpSynced() (bool, error)
}

func NewNtpQuery(timeout time.Duration, fs afero.Fs) NtpQuery {
	return &ntpQuery{
		timeout: timeout,
		fs:      fs,
		proc:    os.NewProc(),
	}
}

type ntpQuery struct {
	timeout time.Duration
	fs      afero.Fs
	proc    os.Proc
}

func (q *ntpQuery) IsNtpSynced() (bool, error) {
	_, err := exec.LookPath("timedatectl")
	if err != nil {
		zap.L().Sugar().Debug(err)
	}
	synced, err := q.checkWithTimedateCtl()
	if err == nil {
		return synced, nil
	}
	zap.L().Sugar().Debug(err)

	_, err = exec.LookPath("ntpstat")
	if err != nil {
		zap.L().Sugar().Debug(err)
	}
	synced, err = q.checkWithNtpstat()
	if err == nil {
		return synced, nil
	}
	zap.L().Sugar().Debug(err)

	return false, errors.New("couldn't check NTP with timedatectl or ntpstat")
}

func (q *ntpQuery) checkWithTimedateCtl() (bool, error) {
	output, err := q.proc.RunWithSystemLdPath(q.timeout, "timedatectl", "status")
	if err != nil {
		return false, err
	}
	clockSyncedLinePattern := regexp.MustCompile("^.* synchronized: (.*)$")
	for _, outLine := range output {
		zap.L().Sugar().Debugf("Parsing timedatectl output '%s'", outLine)
		matches := clockSyncedLinePattern.FindAllStringSubmatch(outLine, -1)
		if matches != nil {
			return matches[0][1] == "yes", nil
		}
	}
	return false, errors.New("NTP sync information not found in timedatectl output")
}

func (q *ntpQuery) checkWithNtpstat() (bool, error) {
	zap.L().Sugar().Debugf("Checking NTP sync with ntpstat")
	_, err := q.proc.RunWithSystemLdPath(q.timeout, "ntpstat")
	// ntpstat exits with status other than 0 when NTP is not synced
	if err != nil {
		zap.L().Sugar().Debugf("ntpstat returned an error '%s'", err.Error())
		return false, err
	}
	return true, nil
}
