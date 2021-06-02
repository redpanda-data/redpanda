package cmd

import (
	"errors"
	"fmt"
	"os"
	"strings"
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

func TestTryExecPlugin(t *testing.T) {
	fs := func() testPluginHandler {
		return testPluginHandler{
			"rpk-foo_bar-baz":     nil,
			"rpk-foo_bar":         nil,
			"rpk-fizz-buzz-bizzy": nil,
		}
	}
	hit := func(file, args string) testPluginHandler {
		h := fs()
		h["rpk-"+file] = map[string]int{args: 1}
		return h
	}

	for _, test := range []struct {
		name      string
		args      []string
		exp       testPluginHandler
		unhandled bool
	}{
		{
			name: "prefer longest command match",
			args: []string{"foo-bar", "baz"},
			exp:  hit("foo_bar-baz", ""),
		},

		{
			name: "match shorter command if not all pieces specified",
			args: []string{"foo-bar", "--baz"},
			exp:  hit("foo_bar", "--baz"),
		},

		{
			name: "match shorter command, flag with argument",
			args: []string{"foo-bar", "--baz", "buzz"},
			exp:  hit("foo_bar", "--baz\x00buzz"),
		},

		{
			name: "match shorter command, no flags",
			args: []string{"foo-bar"},
			exp:  hit("foo_bar", ""),
		},

		{
			name: "pieces joined with dash",
			args: []string{"fizz", "buzz", "bizzy"},
			exp:  hit("fizz-buzz-bizzy", ""),
		},

		{
			name:      "no plugin found",
			args:      []string{"unknown", "unknown", "unknown"},
			exp:       fs(),
			unhandled: true,
		},

		{
			name:      "exec error returns error",
			args:      []string{testPluginFileExecError},
			exp:       fs(),
			unhandled: true,
		},

		{
			name:      "no args is unhandled",
			args:      []string{},
			exp:       fs(),
			unhandled: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			fs := fs()
			foundPath, err := tryExecPlugin(fs, test.args)
			if err != nil {
				if len(test.args) > 0 && test.args[1] != "ERROR" {
					t.Errorf("unexpected tryExecPlugin error for arg0 %s: %v", test.args[0], err)
				}
				return
			}
			if !test.unhandled && len(foundPath) == 0 {
				t.Error("foundPath is unexpectedly empty")
			}
			require.Equal(t, fs, test.exp, "%s fs not as expected", test.name)
		})
	}
}

const (
	testPluginPathPrefix    = "/test/plugin/path/"
	testPluginFileExecError = "ERROR"
)

type testPluginHandler map[string]map[string]int // path => args used => # times calledk

func (t testPluginHandler) lookPath(file string) (string, bool) {
	_, ok := t[file]
	return testPluginPathPrefix + file, ok
}

func (t testPluginHandler) exec(path string, args []string) error {
	if !strings.HasPrefix(path, testPluginPathPrefix) {
		return fmt.Errorf("missing expected plugin path prefix %s", testPluginPathPrefix)
	}
	file := strings.TrimPrefix(path, testPluginPathPrefix)
	if file == testPluginFileExecError {
		return errors.New("error!")
	}
	argHits := t[file]
	if argHits == nil {
		argHits = make(map[string]int)
		t[file] = argHits
	}
	joined := strings.Join(args, "\x00")
	argHits[joined]++
	return nil
}
