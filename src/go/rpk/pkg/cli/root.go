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
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"

	"github.com/fatih/color"
	mTerm "github.com/moby/term"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/acl"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cloud"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cluster"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/connect"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/container"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/debug"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/generate"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/group"
	plugincmd "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/plugin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/profile"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/registry"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/security"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/topic"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/transform"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/version"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cobraext"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/plugin"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"golang.org/x/term"
)

func Execute() {
	fs := afero.NewOsFs()

	if !term.IsTerminal(int(os.Stdout.Fd())) {
		color.NoColor = true
	}

	p := new(config.Params)
	runXHelp := func() {
		for _, o := range p.FlagOverrides {
			switch {
			case o == "help":
				fmt.Print(config.ParamsHelp())
			case o == "list":
				fmt.Print(config.ParamsList())
			default:
				return
			}
			os.Exit(0)
		}
	}
	cobra.OnInitialize(func() {
		runXHelp()
		zap.ReplaceGlobals(p.Logger())
	})
	root := &cobra.Command{
		Use:     "rpk",
		Short:   "rpk is the Redpanda CLI & toolbox",
		Long:    "",
		Version: version.Pretty(),

		CompletionOptions: cobra.CompletionOptions{DisableDefaultCmd: true},
	}
	pf := root.PersistentFlags()

	searchLocal, _ := os.UserConfigDir()
	if searchLocal == "" {
		searchLocal = "~/.config"
	}
	searchLocal = filepath.Join(searchLocal, "rpk", "rpk.yaml")

	pf.StringVar(&p.ConfigFlag, "config", "", fmt.Sprintf("Redpanda or rpk config file; default search paths are %q, $PWD/redpanda.yaml, and /etc/redpanda/redpanda.yaml", searchLocal))
	pf.StringVar(&p.Profile, "profile", "", "rpk profile to use")
	pf.StringArrayVarP(&p.FlagOverrides, "config-opt", "X", nil, "Override rpk configuration settings; '-X help' for detail or '-X list' for terser detail")
	pf.BoolVarP(&p.DebugLogs, "verbose", "v", false, "Enable verbose logging")

	root.RegisterFlagCompletionFunc("config-opt", func(_ *cobra.Command, _ []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		var opts []string
		for _, line := range strings.Split(config.ParamsList(), "\n") {
			opt := strings.SplitN(line, "=", 2)
			if len(opt) != 2 {
				continue
			}
			if !strings.HasPrefix(opt[0], toComplete) {
				continue
			}
			opts = append(opts, opt[0]+"=")
		}
		return opts, cobra.ShellCompDirectiveNoSpace
	})
	root.RegisterFlagCompletionFunc("profile", profile.ValidProfiles(fs, p))

	root.AddCommand(
		acl.NewCommand(fs, p),
		cloud.NewCommand(fs, p, osExec),
		cluster.NewCommand(fs, p),
		container.NewCommand(fs, p),
		connect.NewCommand(fs, p, osExec),
		profile.NewCommand(fs, p),
		debug.NewCommand(fs, p),
		generate.NewCommand(fs, p),
		group.NewCommand(fs, p),
		plugincmd.NewCommand(fs),
		registry.NewCommand(fs, p),
		security.NewCommand(fs, p),
		topic.NewCommand(fs, p),
		transform.NewCommand(fs, p, osExec),
		version.NewCommand(fs, p),

		newStatusCommand(), // deprecated
	)

	addPlatformDependentCmds(fs, p, root)

	// Plugin autocompletion: Cobra creates autocompletion for shells via
	// all commands discoverable from the root command. Plugins that are
	// prefixed .rpk.ac- indicate they support the --help-autocomplete
	// flag. We exec with the flag and then create a bunch of fake cobra
	// commands within rpk. The plugin now looks like it is a part of rpk.
	//
	// We block plugins from overwriting actual rpk commands (rpk security acl),
	// unless the plugin is specifically rpk managed.
	//
	// Managed plugins are slightly weirder and are documented below.
	for _, pl := range plugin.ListPlugins(fs, plugin.UserPaths()) {
		if pl.Managed {
			mp, managedHook := plugin.LookupManaged(pl)
			if managedHook != nil {
				addPluginWithExec(root, mp.Name, mp.Arguments, mp.Path, managedHook, fs, p)
				continue
			}
		}
		addPluginWithExec(root, pl.Name, pl.Arguments, pl.Path, nil, nil, p)
	}

	// Cobra creates help flag as: help for <command> if you want to override
	// that message (capitalize the first letter) then this is the way.
	// See: spf13/cobra#480
	cobraext.Walk(root, func(c *cobra.Command) {
		c.Flags().BoolP("help", "h", false, "Help for "+c.Name())

		// If a command has no Run, then -X help or -X list by default
		// will just exit with the command's help text. We override
		// that to always exit on -X help and -X list, and fallback to
		// the old default of printing the command's help.
		if c.Run == nil {
			c.Run = func(cmd *cobra.Command, _ []string) {
				runXHelp()
				cmd.Help()
			}
		}
	})

	// Cobra does not return an 'unknown command' error unless cobra.NoArgs is
	// specified.
	// See: https://github.com/spf13/cobra/issues/706
	cobraext.Walk(root, func(c *cobra.Command) {
		if c.Args == nil && c.HasSubCommands() {
			c.Args = cobra.NoArgs
		}
	})

	cobra.AddTemplateFunc("wrappedLocalFlagUsages", wrappedLocalFlagUsages)
	cobra.AddTemplateFunc("wrappedGlobalFlagUsages", wrappedGlobalFlagUsages)
	root.SetUsageTemplate(usageTemplate)

	err := root.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func osExec(path string, args []string) error {
	args = append([]string{path}, args...)
	env := os.Environ()
	if runtime.GOOS == "windows" {
		return (&exec.Cmd{
			Path:   path,
			Args:   args,
			Env:    env,
			Stdin:  os.Stdin,
			Stdout: os.Stdout,
			Stderr: os.Stderr,
		}).Run()
	}
	return syscall.Exec(path, args, env)
}

func wrappedLocalFlagUsages(cmd *cobra.Command) string {
	width := 80
	if ws, err := mTerm.GetWinsize(0); err == nil {
		width = int(ws.Width)
	}
	return cmd.LocalFlags().FlagUsagesWrapped(width)
}

func wrappedGlobalFlagUsages(cmd *cobra.Command) string {
	width := 80
	if ws, err := mTerm.GetWinsize(0); err == nil {
		width = int(ws.Width)
	}
	return cmd.InheritedFlags().FlagUsagesWrapped(width)
}

// This is the same Cobra usage template but using the wrapped flag usages.
var usageTemplate = `Usage:{{if .Runnable}}
  {{.UseLine}}{{end}}{{if .HasAvailableSubCommands}}
  {{.CommandPath}} [command]{{end}}{{if gt (len .Aliases) 0}}

Aliases:
  {{.NameAndAliases}}{{end}}{{if .HasExample}}

Examples:
{{.Example}}{{end}}{{if .HasAvailableSubCommands}}

Available Commands:{{range .Commands}}{{if (or .IsAvailableCommand (eq .Name "help"))}}
  {{rpad .Name .NamePadding }} {{.Short}}{{end}}{{end}}{{end}}{{if .HasAvailableLocalFlags}}

Flags:
{{ wrappedLocalFlagUsages . | trimRightSpace}}{{end}}{{if .HasAvailableInheritedFlags}}

Global Flags:
{{ wrappedGlobalFlagUsages . | trimRightSpace}}{{end}}{{if .HasHelpSubCommands}}

Additional help topics:{{range .Commands}}{{if .IsAdditionalHelpTopicCommand}}
  {{rpad .CommandPath .CommandPathPadding}} {{.Short}}{{end}}{{end}}{{end}}{{if .HasAvailableSubCommands}}

Use "{{.CommandPath}} [command] --help" for more information about a command.{{end}}
`
