// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package cli

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cobraext"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/plugin"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

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
	p *config.Params,
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
			Args:               cobra.MinimumNArgs(0),
			Run: func(cmd *cobra.Command, args []string) {
				keepForPlugin, _ := cobraext.StripFlagset(args, cmd.InheritedFlags()) // strip all rpk specific flags before execing the plugin
				err := osExec(execPath, keepForPlugin)
				out.MaybeDie(err, "unable to execute plugin: %v", err)
			},
		}
		parentCmd.AddCommand(childCmd)
	}

	if len(pieces) > 1 { // recursive: we are not done yet adding our nested command
		args := pieces[1:]
		addPluginWithExec(childCmd, name, args, execPath, managedHook, fs, p)
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
		zap.L().Sugar().Debugf("unable to run %s: %v", plugin.FlagAutoComplete, err)
		return
	}
	var helps []pluginHelp
	if err = json.Unmarshal(out, &helps); err != nil {
		zap.L().Sugar().Debugf("unable to parse %s return: %v", plugin.FlagAutoComplete, err)
		return
	}
	if len(helps) == 0 {
		zap.L().Sugar().Debugf("plugin that supports %s did not return any help", plugin.FlagAutoComplete)
		return
	}

	addPluginHelp(childCmd, name, helps, execPath, managedHook, fs, p)
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
	p *config.Params,
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
			zap.L().Sugar().Debugf("invalid plugin help returned duplicate path %s", h.Path)
			return
		}
		uniques[h.Path] = h

		if !strings.HasPrefix(h.Path, childPrefix) && h.Path != pluginName {
			zap.L().Sugar().Debugf("invalid plugin help has path %s missing required prefix %s", h.Path, childPrefix)
			return
		}
		if !rePlugin.MatchString(h.Path) {
			zap.L().Sugar().Debugf("invalid plugin help path %s is not %s", h.Path, rePluginString)
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
		zap.L().Sugar().Debugf("invalid plugin help specified multiple (%d) top-level subcommands", l)
		return
	}
	us, exists := subcommands.inner[pluginName]
	if !exists {
		zap.L().Sugar().Debugf("invalid plugin help specified different top level subcommand than %s", pluginName)
		return
	}

	// For managed plugins, the plugin could be installed over an existing
	// command. We want to keep the rpk version. Plugin installed commands
	// all have the same prefix.
	if strings.HasSuffix(cmd.Short, pluginShortSuffix) {
		addPluginHelpToCmd(cmd, pluginName, us.help)
	}
	addPluginSubcommands(cmd, us, nil, execPath, managedHook, fs, p)
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
	p *config.Params,
) {
	for childUse, childHelp := range parentHelp.inner {
		childCmd := &cobra.Command{
			Short:              fmt.Sprintf("%s external plugin", childUse),
			DisableFlagParsing: true,
			Args:               cobra.MinimumNArgs(0),
			Run: func(cmd *cobra.Command, args []string) {
				keepForPlugin, _ := cobraext.StripFlagset(args, cmd.InheritedFlags()) // strip all rpk specific flags before execing the plugin
				osExec(execPath, append(append(leadingPieces, cmd.Use), keepForPlugin...))
			},
		}
		addPluginHelpToCmd(childCmd, childUse, childHelp.help)

		// Step 3, from way above: if this is a managed command, we now
		// (finally) need to hook this fake command's run through the
		// managed hook.
		if managedHook != nil {
			childCmd = managedHook(childCmd, fs, p)
		}
		if childHelp.inner != nil {
			addPluginSubcommands(childCmd, childHelp, append(leadingPieces, childCmd.Use), execPath, managedHook, fs, p)
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
