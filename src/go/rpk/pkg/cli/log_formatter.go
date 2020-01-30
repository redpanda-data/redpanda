package cli

import (
	"bytes"
	"fmt"
	"io"

	"github.com/fatih/color"
	"github.com/sirupsen/logrus"
)

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

func (f rpkLogFormatter) getPrinter(
	lvl logrus.Level,
) func(io.Writer, ...interface{}) (int, error) {
	if color, exists := logLevelColorMap[lvl]; exists {
		return color.Fprintln
	}
	return fmt.Fprintln
}
