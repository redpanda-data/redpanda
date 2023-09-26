// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package cli

import (
	"bytes"
	"fmt"
	"io"

	"github.com/fatih/color"
	"github.com/sirupsen/logrus"
)

type noopFormatter struct{}

func (*noopFormatter) Format(e *logrus.Entry) ([]byte, error) {
	return []byte(e.Message), nil
}


func NewNoopFormatter() logrus.Formatter {
	return &noopFormatter{}
}

var logLevelColorMap = map[logrus.Level]*color.Color{
	logrus.WarnLevel:  color.New(color.FgYellow),
	logrus.ErrorLevel: color.New(color.FgRed),
}

type rpkLogFormatter struct {
	logrus.Formatter
}

func NewRpkLogFormatter() logrus.Formatter {
	return &rpkLogFormatter{}
}

func (f rpkLogFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	var b *bytes.Buffer
	if entry.Buffer != nil {
		b = entry.Buffer
	} else {
		b = &bytes.Buffer{}
	}

	printer := f.getPrinter(entry.Level)
	printer(b, entry.Message)
	return b.Bytes(), nil
}

func (rpkLogFormatter) getPrinter(
	lvl logrus.Level,
) func(io.Writer, ...interface{}) (int, error) {
	if color, exists := logLevelColorMap[lvl]; exists {
		return color.Fprintln
	}
	return fmt.Fprintln
}
