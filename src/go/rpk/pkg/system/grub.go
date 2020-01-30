package system

import (
	"fmt"
	"regexp"
	"strings"
	"time"
	"vectorized/pkg/os"
	"vectorized/pkg/tuners/executors"
	"vectorized/pkg/tuners/executors/commands"
	"vectorized/pkg/utils"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
)

const grubCfg = "/etc/default/grub"

var cmdLineOptPattern = regexp.MustCompile("^GRUB_CMDLINE_LINUX=\"(.*)\"$")

type Grub interface {
	AddCommandLineOptions(options []string) error
	MakeConfig() error
	CheckVersion() error
}

func NewGrub(
	commands os.Commands,
	proc os.Proc,
	fs afero.Fs,
	executor executors.Executor,
	timeout time.Duration,
) Grub {
	return &grub{
		commands: commands,
		proc:     proc,
		fs:       fs,
		executor: executor,
		timeout:  timeout,
	}
}

type grub struct {
	commands os.Commands
	proc     os.Proc
	fs       afero.Fs
	executor executors.Executor
	timeout  time.Duration
}

func (g *grub) CheckVersion() error {
	log.Debug("Checking if GRUB is present")
	_, err := g.commands.Which("grub2-mkconfig", g.timeout)
	if err != nil {
		return fmt.Errorf("Only GRUB 2 is currently supported")
	}
	return nil
}

func (g *grub) AddCommandLineOptions(opt []string) error {
	log.Debugf("Adding '%s' to GRUB command line config", opt)
	needChg, err := g.cmdLineCfgNeedChange(opt)
	if err != nil {
		return err
	}
	if !needChg {
		log.Infof("GRUB options are up to date, doing nothing")
		return nil
	}
	lines, err := utils.ReadFileLines(g.fs, grubCfg)
	if err != nil {
		return err
	}
	backupFile, err := utils.BackupFile(g.fs, grubCfg)
	if err != nil {
		return err
	}
	log.Infof("Backup of GRUB config created '%s'", backupFile)

	optionsToSet := optionsToMap(opt)
	var linesToWrite []string
	for _, line := range lines {
		if currentOpts := matchAndSplitCmdOptions(line); currentOpts != nil {
			resultOptsMap := optionsToMap(currentOpts)
			log.Debugf("Current GRUB command line config '%s'",
				currentOpts)
			for keyToSet, valToSet := range optionsToSet {
				resultOptsMap[keyToSet] = valToSet
			}

			newOptLine := fmt.Sprintf("GRUB_CMDLINE_LINUX=\"%s\"",
				toGrubOptionsLine(resultOptsMap))
			linesToWrite = append(linesToWrite, newOptLine)
		} else {
			linesToWrite = append(linesToWrite, line)
		}
	}

	return g.executor.Execute(
		commands.NewWriteFileLinesCmd(g.fs, "/etc/default/grub", linesToWrite))
}

func (g *grub) cmdLineCfgNeedChange(requestedOpts []string) (bool, error) {
	lines, err := utils.ReadFileLines(g.fs, grubCfg)
	if err != nil {
		return false, err
	}
	for _, line := range lines {
		if currentOpts := matchAndSplitCmdOptions(line); currentOpts != nil {
			return optionsNeedChange(currentOpts, requestedOpts), nil
		}
	}
	return false, nil
}

func (g *grub) MakeConfig() error {
	log.Info("Updating GRUB configuration")
	updateCmd, err := g.commands.Which("update-grub", g.timeout)
	if err == nil {
		log.Debugf("Running on Ubuntu based system with '%s' available",
			updateCmd)
		err := g.executor.Execute(commands.NewLaunchCmd(g.proc, g.timeout, updateCmd))
		return err
	}
	for _, file := range []string{
		"/boot/grub2/grub.cfg",
		"/boot/efi/EFI/fedora/grub.cfg"} {
		if exists, _ := afero.Exists(g.fs, file); exists {
			log.Debugf("Found 'grub.cfg' in %s", file)
			err := g.executor.Execute(
				commands.NewLaunchCmd(g.proc, g.timeout, "grub2-mkconfig", "-o", file))
			return err
		}
	}
	return fmt.Errorf("Unable to find grub.cfg")
}

func matchAndSplitCmdOptions(optLine string) []string {
	matches := cmdLineOptPattern.FindAllStringSubmatch(optLine, -1)
	if matches != nil {
		return strings.Split(matches[0][1], " ")
	}
	return nil
}

func splitGrubOption(opt string) (string, string) {
	splitted := strings.Split(opt, "=")
	if len(splitted) == 1 {
		return splitted[0], ""
	}
	return splitted[0], splitted[1]
}

func optionsNeedChange(current []string, requested []string) bool {
	currentOpts := optionsToMap(current)
	requestedOpts := optionsToMap(requested)
	for reqKey, reqVal := range requestedOpts {
		if currentVal, present := currentOpts[reqKey]; !present ||
			currentVal != reqVal {
			return true
		}
	}
	return false
}

func optionsToMap(options []string) map[string]string {
	result := make(map[string]string)
	for _, opt := range options {
		key, val := splitGrubOption(opt)
		result[key] = val
	}
	return result
}

func toGrubOptionsLine(options map[string]string) string {
	var resultOpts []string
	for key, val := range options {
		resultOpts = append(resultOpts, joinGrubOption(key, val))
	}
	return strings.Join(resultOpts, " ")
}

func joinGrubOption(key string, val string) string {
	if val == "" {
		return key
	}
	return fmt.Sprintf("%s=%s", key, val)
}
