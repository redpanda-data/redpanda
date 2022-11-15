// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"syscall"

	"github.com/fatih/color"
	mTerm "github.com/moby/term"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cmd/acl"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cmd/cloud"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cmd/cluster"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cmd/common"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cmd/container"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cmd/debug"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cmd/generate"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cmd/group"
	plugincmd "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cmd/plugin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cmd/topic"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cmd/version"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cmd/wasm"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/plugin"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"golang.org/x/term"
)

func Execute() {
	verbose := false
	fs := afero.NewOsFs()

	if !term.IsTerminal(int(os.Stdout.Fd())) {
		color.NoColor = true
	}
	log.SetFormatter(cli.NewRpkLogFormatter())
	log.SetOutput(os.Stdout)

	cobra.OnInitialize(func() {
		// This is only executed when a subcommand (e.g. rpk check) is
		// specified.
		if verbose {
			log.SetLevel(log.DebugLevel)
		} else {
			log.SetLevel(log.InfoLevel)
		}
	})

	root := &cobra.Command{
		Use:   "rpk",
		Short: "rpk is the Redpanda CLI & toolbox",
		Long:  "",

		CompletionOptions: cobra.CompletionOptions{
			DisableDefaultCmd: true,
		},
	}
	root.PersistentFlags().BoolVarP(&verbose, config.FlagVerbose,
		"v", false, "Enable verbose logging (default: false)")

	root.AddCommand(
		acl.NewCommand(fs),
		cloud.NewCommand(fs, osExec),
		cluster.NewCommand(fs),
		container.NewCommand(),
		debug.NewCommand(fs),
		generate.NewCommand(fs),
		group.NewCommand(fs),
		plugincmd.NewCommand(fs),
		topic.NewCommand(fs),
		version.NewCommand(),
		wasm.NewCommand(fs),
	)

	addPlatformDependentCmds(fs, root)

	// Plugin autocompletion: Cobra creates autocompletion for shells via
	// all commands discoverable from the root command. Plugins that are
	// prefixed .rpk.ac- indicate they support the --help-autocomplete
	// flag. We exec with the flag and then create a bunch of fake cobra
	// commands within rpk. The plugin now looks like it is a part of rpk.
	//
	// We block plugins from overwriting actual rpk commands (rpk acl),
	// unless the plugin is specifically rpk managed.
	//
	// Managed plugins are slightly weirder and are documented below.
	for _, p := range plugin.ListPlugins(fs, plugin.UserPaths()) {
		if plugin.IsManaged(p.Name) {
			mp, managedHook := plugin.LookupManaged(p)
			if managedHook != nil {
				addPluginWithExec(root, mp.Name, mp.Arguments, mp.Path, managedHook, fs)
				continue
			}
		}
		addPluginWithExec(root, p.Name, p.Arguments, p.Path, nil, nil)
	}

	// Cobra creates help flag as: help for <command> if you want to override
	// that message (capitalize the first letter) then this is the way.
	// See: spf13/cobra#480
	walk(root, func(c *cobra.Command) {
		c.Flags().BoolP("help", "h", false, "Help for "+c.Name())
	})

	cobra.AddTemplateFunc("wrappedLocalFlagUsages", wrappedLocalFlagUsages)
	cobra.AddTemplateFunc("wrappedGlobalFlagUsages", wrappedGlobalFlagUsages)
	root.SetUsageTemplate(usageTemplate)

	err := root.Execute()
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "check":
			fallthrough
		case "tune":
			log.Info(common.FeedbackMsg)
		}
	}
	if err != nil {
		os.Exit(1)
	}
}

// See the two use cases for this.
const pluginShortSuffix = " external plugin"

// This recursive function handles everything related to installing a plugin
// into rpk.
//
// 1) We create a command space so that the plugin looks to be at the correct
// location: .rpk-foo_bar will be installed at "rpk foo bar".
//
// 2) If the plugin indicates autocomplete (.rpk.ac-) or is managed
// (.rpk.managed-), we install autocompletion.
//
// 3) If the plugin is managed, we use our managed hook as the run function
// rather than our standard exec-plugin run function.
//
// These steps are documented more below.
func addPluginWithExec(
	parentCmd *cobra.Command,
	name string,
	pieces []string,
	execPath string,
	managedHook plugin.ManagedHook,
	fs afero.Fs,
) {
	// Step 1: we install the plugin into our existing command space.
	//
	// For simple plugins with one piece, this is easy. For more complex,
	// or for managed plugins, this is a bit more confusing.
	//
	// How: we first "find" the path to the plugin and then install
	// anything new recursively.
	//
	// Simple: .rpk-foo, we do not recurse. We find "foo" does not exist,
	// and then we add the command under "rpk".
	//
	// Complex: .rpk-foo_bar, we see "foo" does not exist and install it
	// under "rpk", then we recurse and see "bar" does not exist and
	// install it under "rpk".
	//
	// Managed: managed plugins are an agreement of the plugin name to how
	// that name is managed within rpk. For example, the byoc plugin (named
	// .rpk.managed-byoc) gets prefixed with rpk with "cloud", so that the
	// plugin is installed at "rpk cloud byoc". This command also already
	// exists, so the main thing we add here is exec and autocompletion of
	// byoc subcommands.

	p0 := pieces[0]
	var childCmd *cobra.Command
	for _, cmd := range parentCmd.Commands() {
		if cmd.Name() == p0 {
			childCmd = cmd
			break
		}
	}

	if childCmd == nil {
		childCmd = &cobra.Command{
			Use:                p0,
			Short:              p0 + pluginShortSuffix,
			DisableFlagParsing: true,
			Run: func(_ *cobra.Command, args []string) {
				err := osExec(execPath, args)
				out.MaybeDie(err, "unable to execute plugin: %v", err)
			},
		}
		parentCmd.AddCommand(childCmd)
	}

	if len(pieces) > 1 { // recursive: we are not done yet adding our nested command
		args := pieces[1:]
		addPluginWithExec(childCmd, name, args, execPath, managedHook, fs)
		return
	}

	// If the command does not indicate autocomplete and is not managed, we
	// are done. Note that managed commands likely do not have any fake
	// commands installed yet: managed commands receive additional logic in
	// the autocompletion step.
	base := filepath.Base(execPath)
	if !plugin.IsAutoComplete(base) && !plugin.IsManaged(base) {
		return
	}

	// Step 2: we exec the plugin with --help-autocomplete and we install
	// autocompletion.
	out, err := (&exec.Cmd{
		Path: execPath,
		Args: []string{execPath, plugin.FlagAutoComplete},
		Env:  os.Environ(),
	}).Output()
	if err != nil {
		log.Debugf("unable to run %s: %v", plugin.FlagAutoComplete, err)
		return
	}
	var helps []pluginHelp
	if err = json.Unmarshal(out, &helps); err != nil {
		log.Debugf("unable to parse %s return: %v", plugin.FlagAutoComplete, err)
		return
	}
	if len(helps) == 0 {
		log.Debugf("plugin that supports %s did not return any help", plugin.FlagAutoComplete)
		return
	}

	addPluginHelp(childCmd, name, helps, execPath, managedHook, fs)
}

type pluginHelp struct {
	Path    string   `json:"path"`
	Short   string   `json:"short"`
	Long    string   `json:"long"`
	Example string   `json:"example"`
	Args    []string `json:"args"`
}

var (
	rePluginString = "^[A-Za-z0-9_-]+$"
	rePlugin       = regexp.MustCompile(rePluginString)
)

// If a plugin supports --help-autocomplete, we exec it and add its commands to
// rpk itself. This allows autocompletion to work across plugins.
//
// We expect similar paths to the binary path of a plugin itself:
//
//	cloud_foo_bar corresponds to "rpk cloud foo bar"
//	cloud_foo-bar corresponds to "rpk cloud foo-bar"
//	cloud         corresponds to "rpk cloud"
func addPluginHelp(
	cmd *cobra.Command,
	pluginName string,
	helps []pluginHelp,
	execPath string,
	managedHook plugin.ManagedHook,
	fs afero.Fs,
) {
	childPrefix := pluginName + "_"
	uniques := make(map[string]pluginHelp, len(helps))
	for _, h := range helps {
		// Plugin help is supposed to be prefixed with the plugin name,
		// but to support people renaming a plugin locally on disk, we
		// strip the plugin's name and prefix with the on-disk name.
		piece0 := strings.Split(h.Path, "_")[0]
		h.Path = pluginName + strings.TrimPrefix(h.Path, piece0)
		if _, exists := uniques[h.Path]; exists {
			log.Debugf("invalid plugin help returned duplicate path %s", h.Path)
			return
		}
		uniques[h.Path] = h

		if !strings.HasPrefix(h.Path, childPrefix) && h.Path != pluginName {
			log.Debugf("invalid plugin help has path %s missing required prefix %s", h.Path, childPrefix)
			return
		}
		if !rePlugin.MatchString(h.Path) {
			log.Debugf("invalid plugin help path %s is not %s", h.Path, rePluginString)
			return
		}
	}

	// With our unique paths and the help that corresponds to each path, we
	// need to translate those paths to the nested cobra.Command layout.
	var subcommands useHelp
	for path, help := range uniques {
		trackHelp(
			&subcommands,
			plugin.NameToArgs(path), // path here is the argument path (foo_bar_baz)
			help,
		)
	}

	// This translation must yield exactly one top level help, and that top
	// level must be our plugin name. This is a double-check of the logic
	// at the start of the function and the logic entering this function.
	if l := len(subcommands.inner); l != 1 {
		log.Debugf("invalid plugin help specified multiple (%d) top-level subcommands", l)
		return
	}
	us, exists := subcommands.inner[pluginName]
	if !exists {
		log.Debugf("invalid plugin help specified different top level subcommand than %s", pluginName)
		return
	}

	// For managed plugins, the plugin could be installed over an existing
	// command. We want to keep the rpk version. Plugin installed commands
	// all have the same prefix.
	if strings.HasSuffix(cmd.Short, pluginShortSuffix) {
		addPluginHelpToCmd(cmd, pluginName, us.help)
	}
	addPluginSubcommands(cmd, us, nil, execPath, managedHook, fs)
}

type useHelp struct {
	help  pluginHelp
	inner map[string]*useHelp
}

// trackHelp translates paths from a plugin's help return to the nested layout
// they will have for cobra commands.
func trackHelp(on *useHelp, pieces []string, help pluginHelp) {
	if len(pieces) == 0 {
		on.help = help
		return
	}
	if on.inner == nil {
		on.inner = make(map[string]*useHelp)
	}
	use := pieces[0]
	inner, exists := on.inner[use]
	if !exists {
		inner = new(useHelp)
		on.inner[use] = inner
	}
	trackHelp(inner, pieces[1:], help)
}

// As the final part of adding autocompletion for plugins, this function takes
// our pre-built useHelp which is shaped like our desired cobra.Command's and
// actually creates the fake command space.
func addPluginSubcommands(
	parentCmd *cobra.Command,
	parentHelp *useHelp,
	leadingPieces []string,
	execPath string,
	managedHook plugin.ManagedHook,
	fs afero.Fs,
) {
	for childUse, childHelp := range parentHelp.inner {
		childCmd := &cobra.Command{
			Short:              fmt.Sprintf("%s external plugin", childUse),
			DisableFlagParsing: true,
			Run: func(cmd *cobra.Command, args []string) {
				osExec(execPath, append(append(leadingPieces, cmd.Use), args...))
			},
		}
		addPluginHelpToCmd(childCmd, childUse, childHelp.help)

		// Step 3, from way above: if this is a managed command, we now
		// (finally) need to hook this fake command's run through the
		// managed hook.
		if managedHook != nil {
			childCmd = managedHook(childCmd, fs)
		}
		if childHelp.inner != nil {
			addPluginSubcommands(childCmd, childHelp, append(leadingPieces, childCmd.Use), execPath, managedHook, fs)
		}

		parentCmd.AddCommand(childCmd)
	}
}

// Adds all non-empty parts from --help-autocomplete to our command.
func addPluginHelpToCmd(cmd *cobra.Command, use string, h pluginHelp) {
	cmd.Use = use
	if h.Short != "" {
		cmd.Short = h.Short
	}
	if h.Long != "" {
		cmd.Long = h.Long
	}
	if h.Example != "" {
		cmd.Example = h.Example
	}
	if len(h.Args) > 0 {
		cmd.ValidArgs = h.Args
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

// walk calls f for c and all of its children.
func walk(c *cobra.Command, f func(*cobra.Command)) {
	f(c)
	for _, c := range c.Commands() {
		walk(c, f)
	}
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
