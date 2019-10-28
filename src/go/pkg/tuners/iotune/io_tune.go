package iotune

import (
	"errors"
	"strconv"
	"time"
	"vectorized/pkg/os"

	log "github.com/sirupsen/logrus"
)

type OutputFormat string

const (
	Envfile OutputFormat = "envfile" // Legacy env file
	Seastar OutputFormat = "seastar" // YAML properties file
)

type IoTuneArgs struct {
	Dirs           []string
	Format         OutputFormat
	PropertiesFile string
	IoConfFile     string
	Duration       int
	FsCheck        bool
}

type IoTune interface {
	Run(IoTuneArgs) ([]string, error)
}

func NewIoTune(proc os.Proc, timeout time.Duration) IoTune {
	return &ioTune{
		proc:    proc,
		timeout: timeout,
	}
}

type ioTune struct {
	IoTune
	proc    os.Proc
	timeout time.Duration
}

func (ioTune *ioTune) Run(args IoTuneArgs) ([]string, error) {
	cmdArgs, err := ioTuneCommandLineArgs(args)
	if err != nil {
		return nil, err
	}
	log.Debugf("Running 'iotune' with '%#q'", cmdArgs)
	return ioTune.proc.RunWithSystemLdPath(ioTune.timeout, "iotune", cmdArgs...)
}

func ioTuneCommandLineArgs(args IoTuneArgs) ([]string, error) {
	if len(args.Dirs) == 0 {
		return nil, errors.New("At least one directory is required for iotune")
	}
	var cmdArgs []string
	cmdArgs = append(cmdArgs, "--evaluation-directory")
	for _, dir := range args.Dirs {
		cmdArgs = append(cmdArgs, dir)
	}
	if args.Format != "" {
		cmdArgs = append(cmdArgs, "--format")
		cmdArgs = append(cmdArgs, string(args.Format))
	}
	if args.PropertiesFile != "" {
		cmdArgs = append(cmdArgs, "--properties-file")
		cmdArgs = append(cmdArgs, args.PropertiesFile)
	}
	if args.IoConfFile != "" {
		cmdArgs = append(cmdArgs, "--options-file")
		cmdArgs = append(cmdArgs, args.IoConfFile)
	}
	if args.Duration > 0 {
		cmdArgs = append(cmdArgs, "--duration")
		cmdArgs = append(cmdArgs, strconv.Itoa(args.Duration))
	}
	if args.FsCheck {
		cmdArgs = append(cmdArgs, "--fs-check")
		cmdArgs = append(cmdArgs, "true")
	}
	return cmdArgs, nil
}
