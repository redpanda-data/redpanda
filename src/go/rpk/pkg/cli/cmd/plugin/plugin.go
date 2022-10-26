// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// Package plugin contains the plugin command.
package plugin

import (
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/plugin"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

const urlBase = "https://vectorized-public.s3.us-west-2.amazonaws.com/rpk-plugins"

func NewCommand(fs afero.Fs) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "plugin",
		Short: "List, download, update, and remove rpk plugins",
		Long: `List, download, update, and remove rpk plugins.
	
Plugins augment rpk with new commands.

For a plugin to be used, it must be in $HOME/.local/bin or somewhere 
discoverable by rpk in your $PATH. All plugins follow a defined naming scheme:

  .rpk-<name>
  .rpk.ac-<name>

All plugins are prefixed with either .rpk- or .rpk.ac-. When rpk starts up, it
searches all directories in your $PATH for any executable binary that begins
with either of those prefixes. For any binary it finds, rpk adds a command for
that name to the rpk command space itself.

No plugin name can shadow an existing rpk command, and only one plugin can
exist under a given name at once. Plugins are added to the rpk command space on
a first-seen basis. If you have two plugins rpk-foo, and the second is
discovered later on in the $PATH directories, then only the first will be used.
The second will be ignored.

Plugins that have an .rpk.ac- prefix indicate that they support the
--help-autocomplete flag. If rpk sees this, rpk will exec the plugin with that
flag when rpk starts up, and the plugin will return all commands it supports as
well as short and long help test for each command. Rpk uses this return to
build a shadow command space within rpk itself so that it looks as if the
plugin exists within rpk. This is particularly useful if you enable
autocompletion.

The expected return for plugins from --help-autocomplete is an array of the
following:

  type pluginHelp struct {
          Path    string   ` + "`json:\"path,omitempty\"`" + `
          Short   string   ` + "`json:\"short,omitempty\"`" + `
          Long    string   ` + "`json:\"long,omitempty\"`" + `
          Example string   ` + "`json:\"example,omitempty\"`" + `
          Args    []string ` + "`json:\"args,omitempty\"`" + `
  }

where "path" is an underscore delimited argument path to a command. For
example, "foo_bar_baz" corresponds to the command "rpk foo bar baz".
`,
		Args: cobra.ExactArgs(0),
	}
	cmd.AddCommand(
		newListCommand(fs),
		newInstallCommand(fs),
		newUninstallCommand(fs),
	)
	return cmd
}

func getManifest() (*plugin.Manifest, error) {
	return plugin.DownloadManifest(urlBase + "/manifest.yaml")
}
