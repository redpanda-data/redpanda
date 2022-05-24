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
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cmd/common"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cmd/group"
	plugincmd "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cmd/plugin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/plugin"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"golang.org/x/term"
)

func Execute() {
	verbose := false
	fs := afero.NewOsFs()
	mgr := config.NewManager(fs)

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

	rootCmd := &cobra.Command{
		Use:   "rpk",
		Short: "rpk is the Redpanda CLI & toolbox.",
		Long:  "",
	}
	rootCmd.PersistentFlags().BoolVarP(&verbose, config.FlagVerbose,
		"v", false, "Enable verbose logging (default: false).")

	rootCmd.AddCommand(NewGenerateCommand(fs))
	rootCmd.AddCommand(NewVersionCommand())
	rootCmd.AddCommand(NewWasmCommand(fs))
	rootCmd.AddCommand(NewContainerCommand())
	rootCmd.AddCommand(NewTopicCommand(fs))
	rootCmd.AddCommand(NewClusterCommand(fs))
	rootCmd.AddCommand(NewACLCommand(fs))
	rootCmd.AddCommand(group.NewCommand(fs))

	rootCmd.AddCommand(plugincmd.NewCommand(fs))

	addPlatformDependentCmds(fs, mgr, rootCmd)

	// To support autocompletion even for plugins, we list all plugins now
	// and add tiny commands to our root command. Cobra works by creating
	// autocompletion scripts that contain the commands discoverable from
	// the root command, so by adding cobra.Command's for all plugins, we
	// allow autocompletion.
	//
	// We do not want any plugin to shadow or replace functionality of any
	// rpk command. We do not want `rpk-acl-foo` to exec a plugin, when
	// `rpk acl` exists and the single argument foo may be important. We
	// block rpk command shadowing by not keeping any plugin that shares an
	// argument search path with an rpk command.
	//
	// Further, unlike kubectl, we do not allow one plugin to be at the end
	// of another plugin (rpk foo bar cannot exist if rpk foo does). This
	// is ensured by the return from listPlugins, but we can also ensure
	// that here by only adding a plugin with exec if a command does not
	// exist yet.
	for _, plugin := range plugin.ListPlugins(fs, plugin.UserPaths()) {
		if _, _, err := rootCmd.Find(plugin.Arguments); err != nil {
			addPluginWithExec(rootCmd, plugin.Arguments, plugin.Path)
		}
	}

	// Lastly, if rpk is being exec'd with arguments that do not match a
	// command nor a discovered plugin, we do one more search for an
	// external plugin. Given the above code, this is likely to find
	// nothing, but this does not hurt.
	if _, _, err := rootCmd.Find(os.Args[1:]); err != nil {
		if foundPath, err := tryExecPlugin(new(osPluginHandler), os.Args[1:]); len(foundPath) > 0 {
			if err != nil {
				log.Fatalf("exec %s: %v\n", foundPath, err)
			}
			os.Exit(0)
		}
	}

	err := rootCmd.Execute()
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

type pluginHandler interface {
	lookPath(file string) (path string, ok bool)
	exec(path string, args []string) error
}

// tryExecPlugin looks for a plugin, based on the following rules:
//
//  - all "pieces" (non-flags) are joined with an underscore
//  - we prefer the longest command match
//  - we search upward by piece until we run out of pieces
//  - the command must be executable
//
// So,
//
//     rpk foo-bar baz boz fizz-buzz --flag
//
// is translated into searching and execing (with osPluginHandler), in order:
//
//     rpk-foo-bar_baz_boz_fizz-buzz (with args "--flag")
//     rpk-foo-bar_baz_boz           (with args "fizz-buzz --flag")
//     rpk-foo-bar_baz               (with args "boz fizz-buzz --flag")
//     rpk-foo-bar                   (with args "baz boz fizz-buzz --flag")
//
// If a plugin is run, this returns the run error and true, otherwise this
// returns false.
func tryExecPlugin(h pluginHandler, args []string) (string, error) {
	var pieces []string
	for _, arg := range args {
		if strings.HasPrefix(arg, "-") { // found a flag, quit
			break
		}
		pieces = append(pieces, arg)
	}

	if len(pieces) == 0 {
		return "", nil // no plugin specified (command is just "rpk")
	}

	foundPath := ""
	for len(pieces) > 0 {
		joined := strings.Join(pieces, "_")
		if path, ok := h.lookPath(plugin.NamePrefixAutoComplete + joined); ok {
			foundPath = path
			break
		}
		if path, ok := h.lookPath(plugin.NamePrefix + joined); ok {
			foundPath = path
			break
		}
		pieces = pieces[:len(pieces)-1] // did not find with this piece, strip and search higher
	}
	if len(foundPath) == 0 {
		return "", nil
	}

	return foundPath, h.exec(foundPath, args[len(pieces):])
}

// This recursive function recursively adds commands to parent commands,
// stopping when there is only one piece left.
//
// Pieces corresponds to the pieces of a plugin, and on the last piece, the
// cobra.Commabd will execute execPath.
func addPluginWithExec(
	parentCmd *cobra.Command, pieces []string, execPath string,
) {
	// pieces[0] must exist because this function is only called from the
	// result of listPlugins (which ensures there are pieces), or
	// recursively below when there is more than one piece.
	p0 := pieces[0]

	childCmd, _, err := parentCmd.Find(pieces)

	// If the command does not exist, then err will be non-nil. If the
	// command does not exist and the parent does not have subcommands,
	// then childCmd is equal to parentCmd. We also check nil to be sure.
	if err != nil || childCmd == nil || parentCmd == childCmd {
		childCmd = &cobra.Command{
			Use:                p0,
			Short:              fmt.Sprintf("%s external plugin", p0),
			DisableFlagParsing: true,
		}
		parentCmd.AddCommand(childCmd)
	}

	if len(pieces) > 1 { // recursive: we are not done yet adding our nested command
		args := pieces[1:]
		addPluginWithExec(childCmd, args, execPath)
		return
	}

	childCmd.Run = func(_ *cobra.Command, args []string) { // base: we are at the last piece and can add our exec
		new(osPluginHandler).exec(execPath, args)
	}

	// If the exec command has the rpk.ac- prefix, then the plugin
	// signifies that it supports --help-autocomplete, and we can exec it
	// quickly to get useful fields for the command.
	if !strings.HasPrefix(filepath.Base(execPath), plugin.NamePrefixAutoComplete) {
		return
	}

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

	addPluginHelp(childCmd, p0, helps, execPath)
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
// In this function, we validate the plugin's return, ensuring the paths it
// returns for commands follows the convention we expect.
//
// We expect similar paths to the binary path of a plugin itself:
//
//     cloud_foo-bar corresponds to "rpk cloud foo bar"
//     cloud_foo_bar corresponds to "rpk cloud foo-bar"
//     cloud         corresponds to "rpk cloud"
//
// For sanity, all returned paths must begin with the plugin name itself and a
// dash. The only path that can be without a dash is a help for the plugin name
// itself (e.g., "cloud").
func addPluginHelp(
	cmd *cobra.Command, pluginName string, helps []pluginHelp, execPath string,
) {
	childPrefix := pluginName + "_"
	uniques := make(map[string]pluginHelp, len(helps))
	for _, h := range helps {
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

	addPluginHelpToCmd(cmd, pluginName, us.help)
	addPluginSubcommands(cmd, us, nil, execPath)
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
// actually creates that shape.
//
// We always have to track the leading pieces for each subcommand, so that when
// we exec the plugin, we re-create the args that we passed to and consumed by
// rpk.
func addPluginSubcommands(
	parentCmd *cobra.Command,
	parentHelp *useHelp,
	leadingPieces []string,
	execPath string,
) {
	for childUse, childHelp := range parentHelp.inner {
		childCmd := &cobra.Command{
			Short:              fmt.Sprintf("%s external plugin", childUse),
			DisableFlagParsing: true,
			Run: func(cmd *cobra.Command, args []string) {
				new(osPluginHandler).exec(execPath, append(append(leadingPieces, cmd.Use), args...))
			},
		}
		addPluginHelpToCmd(childCmd, childUse, childHelp.help)
		if childHelp.inner != nil {
			addPluginSubcommands(childCmd, childHelp, append(leadingPieces, childCmd.Use), execPath)
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

type osPluginHandler struct{}

func (*osPluginHandler) lookPath(file string) (string, bool) {
	path, err := exec.LookPath(file)
	return path, err == nil
}

func (*osPluginHandler) exec(path string, args []string) error {
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
