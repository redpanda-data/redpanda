// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package cobraext

import (
	"reflect"
	"testing"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

func TestStripFlagset(t *testing.T) {
	root := &cobra.Command{
		Use: "root",
	}
	root.Flags().BoolP("help", "h", false, "Help")
	pf := root.PersistentFlags()
	pf.String("config", "", "Config file")
	pf.StringArrayP("config-opt", "X", nil, "Override")
	pf.StringP("verbose", "v", "none", "Log level")
	pf.Lookup("verbose").NoOptDefVal = "info"

	subcmd := &cobra.Command{
		Use:                "subcmd",
		DisableFlagParsing: true,
	}

	root.AddCommand(subcmd)
	subcmd.Flags().StringP("foo", "f", "", "foo")

	for _, test := range []struct {
		args        []string
		expKept     []string
		expStripped []string
	}{
		{
			args:        []string{"--config", "foo", "--config-opt", "bar", "--config-opt=biz", "--version", "-v", "-v=debug", "subcmd", "-f", "foo", "finalarg", "finalarg2", "-r", "--finalUnknown"},
			expKept:     []string{"--version", "-f", "foo", "finalarg", "finalarg2", "-r", "--finalUnknown"},
			expStripped: []string{"--config", "foo", "--config-opt", "bar", "--config-opt=biz", "-v", "-v=debug"},
		},
	} {
		t.Run("", func(t *testing.T) {
			root.SetArgs(test.args)
			var kept, stripped []string
			subcmd.Run = func(cmd *cobra.Command, args []string) {
				kept, stripped = StripFlagset(args, cmd.InheritedFlags())
			}
			root.Execute()
			if !reflect.DeepEqual(kept, test.expKept) {
				t.Errorf("kept %v != exp %v", kept, test.expKept)
			}
			if !reflect.DeepEqual(stripped, test.expStripped) {
				t.Errorf("stripped %v != exp %v", stripped, test.expStripped)
			}
		})
	}
}

func TestStripFlags(t *testing.T) {
	fs := pflag.NewFlagSet("", pflag.ContinueOnError)
	fs.String("foo", "", "")
	fs.StringSlice("slice", nil, "")
	fs.IntP("int", "i", 0, "")
	fs.StringP("str", "s", "", "")
	fs.BoolP("help", "h", false, "")
	fs.StringP("version", "v", "", "")
	fs.Lookup("version").NoOptDefVal = "1"

	// Long flags: --foo, --slice, --int, --help, --version
	// Short flags: -i, -s, -h, -v

	for _, test := range []struct {
		name  string
		args  []string
		long  []string
		short []string

		expKept     []string
		expStripped []string
	}{
		{
			name:        "easy stripping",
			args:        []string{"cmd", "--foo", "foo", "--slice", "str1", "-hv=3"},
			long:        []string{"foo"},
			short:       []string{"h"},
			expKept:     []string{"cmd", "--slice", "str1", "-v=3"},
			expStripped: []string{"--foo", "foo", "-h"},
		},

		{
			// cmd       keep
			// --foo -i  strip
			// -i keep   keep
			// -i=keep   keep
			// subcmd    keep
			// -hvi 3    strip the h and the v, keep -i 3
			// -hiv 3    strip the h and the v; v has NoOptDevVal so we do nothing with 3; keep -i 3
			// -ivs 3    strip v and s; s requires a value and consumes 3; keep only -i
			// --int 4   strip
			// -v=4      strip
			// -s str    strip
			// --        keep
			// -hvs=3    keep
			name:        "args in the middle and comprehensive",
			args:        []string{"cmd", "--foo", "-i", "-i", "keep", "-i=keep", "subcmd", "-hvi", "3", "-hiv", "3", "-ivs", "3", "--int", "4", "-v=4", "-s", "str", "--", "-hvs=3"},
			long:        []string{"foo", "int"},
			short:       []string{"h", "v", "s"},
			expKept:     []string{"cmd", "-i", "keep", "-i=keep", "subcmd", "-i", "3", "-i", "3", "-i", "--", "-hvs=3"},
			expStripped: []string{"--foo", "-i", "-hv", "-hv", "-vs", "3", "--int", "4", "-v=4", "-s", "str"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			kept, stripped := StripFlags(test.args, fs, test.long, test.short)
			if !reflect.DeepEqual(kept, test.expKept) {
				t.Errorf("kept (%v) != expKept (%v)", kept, test.expKept)
			}
			if !reflect.DeepEqual(stripped, test.expStripped) {
				t.Errorf("stripped (%v) != expStripped (%v)", stripped, test.expStripped)
			}
		})
	}
}

func TestLongFlagValue(t *testing.T) {
	fs := pflag.NewFlagSet("", pflag.ContinueOnError)
	fs.BoolP("help", "h", false, "Help")
	fs.String("config", "", "Config file")
	fs.StringArrayP("config-opt", "X", nil, "Override")
	fs.StringP("verbose", "v", "none", "Log level")
	fs.Lookup("verbose").NoOptDefVal = "info"

	args := []string{"--config", "foo", "--config-opt", "bar", "--config-opt=biz", "-v", "-v=debug", "subcmd", "-f", "foo", "finalarg", "finalarg2", "--unknown", "handled", "--unknown2=handled2", "-h"}

	for _, test := range []struct {
		flag      string
		shorthand string
		exp       string
	}{
		{
			flag: "config",
			exp:  "foo",
		},
		{
			flag: "config-opt",
			exp:  "biz", // we take the last value
		},
		{
			flag: "noexist",
			exp:  "",
		},
		{
			flag: "unknown",
			exp:  "handled",
		},
		{
			flag: "unknown2",
			exp:  "handled2",
		},
		{
			flag:      "verbose",
			shorthand: "v",
			exp:       "debug",
		},
		{
			flag:      "help",
			shorthand: "h",
			exp:       "true",
		},
	} {
		got := LongFlagValue(args, fs, test.flag, test.shorthand)
		if got != test.exp {
			t.Errorf("got %v != exp %v", got, test.exp)
		}
	}
}
