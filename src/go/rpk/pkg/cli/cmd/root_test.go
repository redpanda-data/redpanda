package cmd

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
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
			".rpk-foo-bar_baz":     nil,
			".rpk-foo-bar":         nil,
			".rpk-fizz_buzz_bizzy": nil,
		}
	}
	hit := func(file, args string) testPluginHandler {
		h := fs()
		h[".rpk-"+file] = map[string]int{args: 1}
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
			exp:  hit("foo-bar_baz", ""),
		},

		{
			name: "match shorter command if not all pieces specified",
			args: []string{"foo-bar", "--baz"},
			exp:  hit("foo-bar", "--baz"),
		},

		{
			name: "match shorter command, flag with argument",
			args: []string{"foo-bar", "--baz", "buzz"},
			exp:  hit("foo-bar", "--baz\x00buzz"),
		},

		{
			name: "match shorter command, no flags",
			args: []string{"foo-bar"},
			exp:  hit("foo-bar", ""),
		},

		{
			name: "pieces joined with dash",
			args: []string{"fizz", "buzz", "bizzy"},
			exp:  hit("fizz_buzz_bizzy", ""),
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
		return errors.New("error")
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

func TestAddPluginWithExec(t *testing.T) {
	root := &cobra.Command{
		Use: "root",
	}

	subcommands := func(c *cobra.Command) []string {
		var subs []string
		for _, c := range c.Commands() {
			subs = append(subs, c.Use)
		}
		return subs
	}
	find := func(c *cobra.Command, search []string) *cobra.Command {
		child, _, _ := c.Find(search)
		return child
	}

	addPluginWithExec(root, []string{"foo", "bar"}, "") // we cannot test exec path, so we leave empty
	assert.Equal(t, []string{"foo"}, subcommands(root), "expected one subcommand of root to be [foo]")
	assert.Equal(t, []string{"bar"}, subcommands(find(root, []string{"foo"})), "expected one subcommand of root foo to be [bar]")

	addPluginWithExec(root, []string{"foo", "baz"}, "")
	assert.Equal(t, []string{"foo"}, subcommands(root), "expected one subcommand of root to still be [foo]")
	assert.Equal(t, []string{"bar", "baz"}, subcommands(find(root, []string{"foo"})), "expected two subcommands of root foo to be [bar, baz]")

	addPluginWithExec(root, []string{"fizz"}, "")
	assert.Equal(t, []string{"fizz", "foo"}, subcommands(root), "expected one subcommand of root to be [fizz, foo]")
	assert.Equal(t, []string{"bar", "baz"}, subcommands(find(root, []string{"foo"})), "expected two subcommands of root foo to still be [bar, baz]")
	assert.Equal(t, []string(nil), subcommands(find(root, []string{"fizz"})), "expected no subcommands under fizz")
}

func TestTrackHelp(t *testing.T) {
	var base useHelp

	trackHelp(&base, []string{"foo", "bar"}, pluginHelp{Short: "short foo bar"})
	exp := useHelp{
		inner: map[string]*useHelp{
			"foo": {
				inner: map[string]*useHelp{
					"bar": {help: pluginHelp{Short: "short foo bar"}},
				},
			},
		},
	}
	assert.Equal(t, exp, base, "expected trackHelp to create two nestings for %s", []string{"foo", "bar"})

	trackHelp(&base, []string{"foo", "baz"}, pluginHelp{Short: "short foo baz"})
	exp.inner["foo"].inner["baz"] = &useHelp{help: pluginHelp{Short: "short foo baz"}}
	assert.Equal(t, exp, base, "expected trackHelp to create additional second-level nesting for %s", []string{"foo", "baz"})

	trackHelp(&base, []string{"biz"}, pluginHelp{Short: "top level biz"})
	exp.inner["biz"] = &useHelp{help: pluginHelp{Short: "top level biz"}}
	assert.Equal(t, exp, base, "expected trackHelp to create additional top-level command for %s", []string{"biz"})

	trackHelp(&base, []string{"foo"}, pluginHelp{Short: "updated foo help"})
	exp.inner["foo"].help = pluginHelp{Short: "updated foo help"}
	assert.Equal(t, exp, base, "expected trackHelp to create perform in-place update to existing incomplete top level-foo")
}
