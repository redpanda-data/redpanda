// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package irq

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/os"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/executors"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/executors/commands"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/utils"
	"github.com/spf13/afero"
	"go.uber.org/zap"
)

type balanceServiceInfo struct {
	optionsKey string
	configFile string
	systemd    bool
}

type BalanceService interface {
	BanIRQsAndRestart(bannedIRQs []int) error
	GetBannedIRQs() ([]int, error)
	IsRunning() bool
}

func NewBalanceService(
	fs afero.Fs, proc os.Proc, executor executors.Executor, timeout time.Duration,
) BalanceService {
	return &balanceService{
		fs:       fs,
		proc:     proc,
		executor: executor,
		timeout:  timeout,
	}
}

type balanceService struct {
	fs       afero.Fs
	proc     os.Proc
	executor executors.Executor
	timeout  time.Duration
}

func (balanceService *balanceService) BanIRQsAndRestart(
	bannedIRQs []int,
) error {
	fmt.Printf("Restarting & Configuring 'irqbalance' with banned IRQs '%v'\n", bannedIRQs)
	if len(bannedIRQs) == 0 {
		return nil
	}
	running := balanceService.IsRunning()
	balanceService.GetBannedIRQs()
	if !running {
		fmt.Println("'irqbalance' process is not running")
		return nil
	}
	serviceInfo, err := balanceService.getBalanceServiceInfo()
	if err != nil {
		return err
	}
	err = balanceService.executor.Execute(
		commands.NewBackupFileCmd(balanceService.fs, serviceInfo.configFile))
	if err != nil {
		return err
	}

	configLines, err := utils.ReadFileLines(balanceService.fs, serviceInfo.configFile)
	if err != nil {
		return err
	}

	var (
		optionLines []string
		newOptions  string
		newLines    []string
	)
	optionsKeyPattern := regexp.MustCompile(fmt.Sprintf("^\\s*%s", serviceInfo.optionsKey))
	for _, line := range configLines {
		if optionsKeyPattern.MatchString(line) {
			optionLines = append(optionLines, line)
		} else {
			newLines = append(newLines, line)
		}
	}

	switch len(optionLines) {
	case 0:
		newOptions = fmt.Sprintf("%s=\"", serviceInfo.optionsKey)
	case 1:
		newOptions = regexp.MustCompile("\"\\s*$").ReplaceAllString(
			strings.TrimRight(optionLines[0], " "), "")
	default:
		return fmt.Errorf("invalid format in '%s' - more than one line with '%s' key",
			serviceInfo.configFile, serviceInfo.optionsKey)
	}

	for _, irq := range bannedIRQs {
		bannedParamPattern := regexp.MustCompile(
			fmt.Sprintf("\\-\\-banirq\\=%d$|\\-\\-banirq\\=%d\\s", irq, irq))
		if !bannedParamPattern.MatchString(newOptions) {
			newOptions += fmt.Sprintf(" --banirq=%d", irq)
		}
	}
	newOptions += "\""

	newLines = append(newLines, newOptions)
	err = balanceService.executor.Execute(
		commands.NewWriteFileLinesCmd(
			balanceService.fs, serviceInfo.configFile, newLines))
	if err != nil {
		return err
	}
	if serviceInfo.systemd {
		zap.L().Sugar().Debug("Restarting 'irqbalance' via systemctl...")
		err = balanceService.executor.Execute(
			commands.NewLaunchCmd(
				balanceService.proc, balanceService.timeout, "systemctl", "try-restart", "irqbalance"))
	} else {
		zap.L().Sugar().Debug("Restarting 'irqbalance' directly (init.d)...")
		err = balanceService.executor.Execute(
			commands.NewLaunchCmd(
				balanceService.proc, balanceService.timeout, "/etc/init.d/irqbalance", "restart"))
	}
	if err != nil {
		return err
	}
	return nil
}

func (balanceService *balanceService) IsRunning() bool {
	return balanceService.proc.IsRunning(balanceService.timeout, "irqbalance")
}

func (balanceService *balanceService) GetBannedIRQs() ([]int, error) {
	zap.L().Sugar().Debugf("Getting banned IRQs")
	serviceInfo, err := balanceService.getBalanceServiceInfo()
	if err != nil {
		return nil, err
	}
	configLines, err := utils.ReadFileLines(balanceService.fs, serviceInfo.configFile)
	if err != nil {
		return nil, err
	}

	var optionLines []string
	optionsKeyPattern := regexp.MustCompile(
		fmt.Sprintf("^\\s*%s", serviceInfo.optionsKey))
	for _, line := range configLines {
		if optionsKeyPattern.MatchString(line) {
			optionLines = append(optionLines, line)
		}
	}
	var bannedIRQs []int
	if len(optionLines) == 0 {
		return bannedIRQs, nil
	}

	if len(optionLines) != 1 {
		return nil, fmt.Errorf("invalid format in '%s' - more than one line with '%s' key",
			serviceInfo.configFile, serviceInfo.optionsKey)
	}
	bannedIRQPattern := regexp.MustCompile(`\-\-banirq\=(\d+)`)

	bannedIRQsMatches := bannedIRQPattern.FindAllStringSubmatch(optionLines[0], -1)

	if len(bannedIRQsMatches) > 0 {
		for _, groupMatch := range bannedIRQsMatches {
			if len(groupMatch) != 2 {
				return nil, fmt.Errorf("Malformed option --banirq option")
			}
			IRQ, err := strconv.Atoi(groupMatch[1])
			if err != nil {
				return nil, err
			}
			bannedIRQs = append(bannedIRQs, IRQ)
		}
	}
	return bannedIRQs, nil
}

func (balanceService *balanceService) getBalanceServiceInfo() (
	*balanceServiceInfo,
	error,
) {
	fs := balanceService.fs
	optionsKey := "OPTIONS"
	configFile := "/etc/default/irqbalance"
	systemd := false
	if exists, _ := afero.Exists(fs, configFile); !exists {
		zap.L().Sugar().Debugf("File '%s' does not exist", configFile)
		if exists, _ := afero.Exists(fs, "/etc/sysconfig/irqbalance"); exists {
			configFile = "/etc/sysconfig/irqbalance"
			optionsKey = "IRQBALANCE_ARGS"
			systemd = true
		} else if exists, _ := afero.Exists(fs, "/etc/conf.d/irqbalance"); !exists {
			zap.L().Sugar().Error("Unknown system configuration - not restarting irqbalance!")
			return nil, errors.New("Unsupported irqbalance service configuration")
		} else {
			configFile = "/etc/conf.d/irqbalance"
			optionsKey = "IRQBALANCE_OPTS"
			lines, err := utils.ReadFileLines(fs, "/proc/1/comm")
			if err != nil {
				return nil, err
			}
			systemd = strings.Contains(lines[0], "systemd")
		}
	}
	return &balanceServiceInfo{
		configFile: configFile,
		optionsKey: optionsKey,
		systemd:    systemd,
	}, nil
}

func AreIRQsStaticallyAssigned(
	IRQs []int, balanceService BalanceService,
) (bool, error) {
	if !balanceService.IsRunning() {
		// As balance service is not running
		// there is no one tho change IRQs alignment
		return true, nil
	}
	bannedIRQs, err := balanceService.GetBannedIRQs()
	if err != nil {
		return false, err
	}
	bannedIRQsMap := make(map[int]bool)
	for _, IRQ := range bannedIRQs {
		bannedIRQsMap[IRQ] = true
	}
	for _, IRQ := range IRQs {
		if _, exists := bannedIRQsMap[IRQ]; !exists {
			return false, nil
		}
	}
	return true, nil
}
