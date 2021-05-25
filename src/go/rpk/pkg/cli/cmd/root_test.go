package cmd

import (
	"os"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func TestLogOutput(t *testing.T) {
	Execute()
	// Execute should have configured logrus's global logger instance to log to
	// STDOUT.
	require.Exactly(t, os.Stdout, logrus.StandardLogger().Out)
}
