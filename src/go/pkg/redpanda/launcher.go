package redpanda

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"

	log "github.com/sirupsen/logrus"
	"golang.org/x/sys/unix"
)

type Launcher interface {
	Start() error
}

type RedpandaArgs struct {
	ConfigFilePath string
	SeastarFlags   map[string]string
}

func NewLauncher(installDir string, args *RedpandaArgs) Launcher {
	return &launcher{
		installDir: installDir,
		args:       args,
	}
}

type launcher struct {
	installDir string
	args       *RedpandaArgs
}

func (l *launcher) Start() error {
	binary, err := l.getBinary()
	if err != nil {
		return err
	}

	if l.args.ConfigFilePath == "" {
		return errors.New("Redpanda config file is required")
	}
	redpandaArgs := collectRedpandaArgs(l.args)
	log.Debugf("Starting '%s' with arguments '%v'", binary, redpandaArgs)

	var rpEnv []string
	ldLibraryPathPattern := regexp.MustCompile("^LD_LIBRARY_PATH=.*$")
	for _, ev := range os.Environ() {
		if !ldLibraryPathPattern.MatchString(ev) {
			rpEnv = append(rpEnv, ev)
		}
	}
	return unix.Exec(binary, redpandaArgs, rpEnv)
}

func (l *launcher) getBinary() (string, error) {
	path, err := exec.LookPath(filepath.Join(l.installDir, "bin", "redpanda"))
	if err != nil {
		return "", err
	}
	return path, nil
}

func collectRedpandaArgs(args *RedpandaArgs) []string {
	redpandaArgs := []string{
		"redpanda",
		"--redpanda-cfg",
		args.ConfigFilePath,
	}

	for flag, value := range args.SeastarFlags {
		redpandaArgs = append(redpandaArgs, "--"+flag, value)
	}
	return redpandaArgs
}
