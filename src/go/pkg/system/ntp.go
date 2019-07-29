package system

import (
	"errors"
	"os/exec"
	"regexp"
	"vectorized/pkg/os"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
)

type NtpQuery interface {
	IsNtpSynced() (bool, error)
}

func NewNtpQuery(fs afero.Fs) NtpQuery {
	return &ntpQuery{
		fs:   fs,
		proc: os.NewProc(),
	}
}

type ntpQuery struct {
	NtpQuery
	fs   afero.Fs
	proc os.Proc
}

func (q *ntpQuery) IsNtpSynced() (bool, error) {
	var inSync bool
	if _, err := exec.LookPath("timedatectl"); err == nil {
		inSync, err = q.checkWithTimedateCtl()
		if err == nil {
			return inSync, nil
		}
	}
	if _, err := exec.LookPath("ntpstat"); !inSync && err == nil {
		inSync, err = q.checkWithNtpstat()
		if err != nil {
			return false, err
		}
	}
	return inSync, nil
}

func (q *ntpQuery) checkWithTimedateCtl() (bool, error) {
	output, err := q.proc.RunWithSystemLdPath("timedatectl", "status")
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
	_, err := q.proc.RunWithSystemLdPath("ntpstat")
	// ntpstat exits with status other than 0 when NTP is not synced
	if err != nil {
		log.Debugf("ntpstat returned an error '%s'", err.Error())
		return false, nil
	}
	return true, nil
}
