package irq

import (
	"fmt"
	"regexp"
	"strings"
	"vectorized/os"
	"vectorized/utils"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
)

type BalanceService interface {
	BanIRQsAndRestart(bannedIRQs []string) error
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
	bannedIRQs []string,
) error {
	optionsKey := "OPTIONS"
	configFile := "/etc/default/irqbalance"
	systemd := false
	log.Infof("Restarting & Configuring 'irqbalance' with banned IRQs '%s'", bannedIRQs)
	if len(bannedIRQs) == 0 {
		return nil
	}

	running := balanceService.proc.IsRunning("irqbalance")

	if !running {
		log.Info("'irqbalance' process is not running")
		return nil
	}

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
				return err
			}
			systemd = strings.Contains(lines[0], "systemd")
		} else {
			log.Error("Unknown system configuration - not restarting irqbalance!")
			return fmt.Errorf("you have to prevent it from moving IRQs '%s' manually", bannedIRQs)
		}
	}

	originalFile := fmt.Sprintf("%s.rpk.orig", configFile)
	if !utils.FileExists(balanceService.fs, originalFile) {
		log.Infof("Saving the original irqbalance configuration is in '%s'", originalFile)
		err := utils.CopyFile(balanceService.fs, configFile, originalFile)
		if err != nil {
			return err
		}
	} else {
		log.Infof("File '%s' already exists - not overwriting.", originalFile)
	}

	configLines, err := utils.ReadFileLines(balanceService.fs, configFile)
	if err != nil {
		return err
	}

	var optionLines []string
	newOptions := ""
	optionsKeyPattern := regexp.MustCompile(fmt.Sprintf("^\\s*%s", optionsKey))
	var newLines []string
	for _, line := range configLines {
		if optionsKeyPattern.MatchString(line) {
			optionLines = append(optionLines, line)
		} else {
			newLines = append(newLines, line)
		}
	}
	if len(optionLines) == 0 {
		newOptions = fmt.Sprintf("%s=\"", optionsKey)
	} else if len(optionLines) == 1 {
		newOptions = regexp.MustCompile("\"\\s*$").ReplaceAllString(
			strings.TrimRight(optionLines[0], " "), "")
	} else {
		return fmt.Errorf("invalid format in '%s' - more than one line with '%s' key", configFile, optionsKey)
	}

	for _, irq := range bannedIRQs {
		bannedParamPattern := regexp.MustCompile(
			fmt.Sprintf("\\-\\-banirq\\=%s$|\\-\\-banirq\\=%s\\s", irq, irq))
		if !bannedParamPattern.MatchString(newOptions) {
			newOptions += fmt.Sprintf(" --banirq=%s", irq)
		}
	}
	newOptions += "\""

	newLines = append(newLines, newOptions)
	err = utils.WriteFileLines(balanceService.fs, newLines, configFile)
	if err != nil {
		return err
	}
	if systemd {
		log.Info("Restarting 'irqbalance' via systemctl...")
		_, err = balanceService.proc.RunWithSystemLdPath("systemctl", "try-restart", "irqbalance")
	} else {
		log.Info("Restarting 'irqbalance' directly (init.d)...")
		_, err = balanceService.proc.RunWithSystemLdPath("/etc/init.d/irqbalance", "restart")
	}
	if err != nil {
		return err
	}
	return nil
}
