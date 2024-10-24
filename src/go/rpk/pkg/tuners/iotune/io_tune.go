// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package iotune

import (
	"errors"
	"strconv"
	"time"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/os"
	"go.uber.org/zap"
)

type OutputFormat string

const (
	Envfile OutputFormat = "envfile" // Legacy env file
	Seastar OutputFormat = "seastar" // YAML properties file
	Bin     string       = "iotune-redpanda"
)

type IoTuneArgs struct {
	Dirs           []string
	Format         OutputFormat
	PropertiesFile string
	IoConfFile     string
	Duration       time.Duration
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
	zap.L().Sugar().Debugf("Running '%s' with '%#q'", Bin, cmdArgs)
	return ioTune.proc.RunWithSystemLdPath(ioTune.timeout, Bin, cmdArgs...)
}

func ioTuneCommandLineArgs(args IoTuneArgs) ([]string, error) {
	if len(args.Dirs) == 0 {
		return nil, errors.New("At least one directory is required for iotune")
	}
	var cmdArgs []string
	cmdArgs = append(cmdArgs, "--evaluation-directory")
	cmdArgs = append(cmdArgs, args.Dirs...)
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
	if args.Duration.Seconds() > 0 {
		cmdArgs = append(cmdArgs, "--duration")
		cmdArgs = append(cmdArgs, strconv.Itoa(int(args.Duration.Seconds())))
	}
	if args.FsCheck {
		cmdArgs = append(cmdArgs, "--fs-check")
		cmdArgs = append(cmdArgs, "true")
	}
	return cmdArgs, nil
}
