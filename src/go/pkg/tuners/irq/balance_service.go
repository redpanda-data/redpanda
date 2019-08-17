package irq

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"vectorized/pkg/os"
	"vectorized/pkg/utils"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
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

func NewBalanceService(fs afero.Fs, proc os.Proc) BalanceService {
	return &balanceService{
		fs:   fs,
		proc: proc,
	}
}

type balanceService struct {
	BalanceService
	fs   afero.Fs
	proc os.Proc
}

func (balanceService *balanceService) BanIRQsAndRestart(
	bannedIRQs []int,
) error {
	log.Infof("Restarting & Configuring 'irqbalance' with banned IRQs '%v'", bannedIRQs)
	if len(bannedIRQs) == 0 {
		return nil
	}
	running := balanceService.IsRunning()
	balanceService.GetBannedIRQs()
	if !running {
		log.Info("'irqbalance' process is not running")
		return nil
	}
	serviceInfo, err := balanceService.getBalanceServiceInfo()
	if err != nil {
		return err
	}
	originalFile := fmt.Sprintf("%s.rpk.orig", serviceInfo.configFile)
	if !utils.FileExists(balanceService.fs, originalFile) {
		log.Infof("Saving the original irqbalance configuration is in '%s'", originalFile)
		err := utils.CopyFile(balanceService.fs, serviceInfo.configFile, originalFile)
		if err != nil {
			return err
		}
	} else {
		log.Debugf("File '%s' already exists - not overwriting.", originalFile)
	}

	configLines, err := utils.ReadFileLines(balanceService.fs, serviceInfo.configFile)
	if err != nil {
		return err
	}

	var optionLines []string
	newOptions := ""
	optionsKeyPattern := regexp.MustCompile(fmt.Sprintf("^\\s*%s", serviceInfo.optionsKey))
	var newLines []string
	for _, line := range configLines {
		if optionsKeyPattern.MatchString(line) {
			optionLines = append(optionLines, line)
		} else {
			newLines = append(newLines, line)
		}
	}
	if len(optionLines) == 0 {
		newOptions = fmt.Sprintf("%s=\"", serviceInfo.optionsKey)
	} else if len(optionLines) == 1 {
		newOptions = regexp.MustCompile("\"\\s*$").ReplaceAllString(
			strings.TrimRight(optionLines[0], " "), "")
	} else {
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
	err = utils.WriteFileLines(balanceService.fs, newLines, serviceInfo.configFile)
	if err != nil {
		return err
	}
	if serviceInfo.systemd {
		log.Debug("Restarting 'irqbalance' via systemctl...")
		_, err = balanceService.proc.RunWithSystemLdPath("systemctl", "try-restart", "irqbalance")
	} else {
		log.Debug("Restarting 'irqbalance' directly (init.d)...")
		_, err = balanceService.proc.RunWithSystemLdPath("/etc/init.d/irqbalance", "restart")
	}
	if err != nil {
		return err
	}
	return nil
}

func (balanceService *balanceService) IsRunning() bool {
	return balanceService.proc.IsRunning("irqbalance")
}

func (balanceService *balanceService) GetBannedIRQs() ([]int, error) {
	log.Debugf("Getting banned IRQs")
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
	bannedIRQPattern := regexp.MustCompile("\\-\\-banirq\\=(\\d+)")

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
	optionsKey := "OPTIONS"
	configFile := "/etc/default/irqbalance"
	systemd := false

	if !utils.FileExists(balanceService.fs, configFile) {
		log.Debugf("File '/etc/default/irqbalance' does not exists")
		if utils.FileExists(balanceService.fs, "/etc/sysconfig/irqbalance") {
			configFile = "/etc/sysconfig/irqbalance"
			optionsKey = "IRQBALANCE_ARGS"
			systemd = true
		} else if utils.FileExists(balanceService.fs, "/etc/conf.d/irqbalance") {
			configFile = "/etc/conf.d/irqbalance"
			optionsKey = "IRQBALANCE_OPTS"
			lines, err := utils.ReadFileLines(balanceService.fs, "/proc/1/comm")
			if err != nil {
				return nil, err
			}
			systemd = strings.Contains(lines[0], "systemd")
		} else {
			log.Error("Unknown system configuration - not restarting irqbalance!")
			return nil, errors.New("Unsupported irqbalance service configuration")
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
