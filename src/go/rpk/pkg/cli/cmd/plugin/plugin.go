// Package plugin contains the plugin command.
package plugin

import (
	"errors"
	"fmt"
	"os"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/plugin"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

const urlBase = "https://vectorized-public.s3.us-west-2.amazonaws.com/rpk-plugins"

func NewCommand(fs afero.Fs) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "plugin",
		Short: "List, download, update, and remove rpk plugins.",
		Long: `List, download, update, and remove rpk plugins.
	
Plugins augment rpk with new commands.

For a plugin to be used, it must be somewhere discoverable by rpk in your
$PATH. All plugins follow a defined naming scheme:

  rpk-<name>
  rpk.ac-<name>

All plugins are prefixed with either rpk- or rpk.ac-. When rpk starts up, it
searches all directories in your $PATH for any executable binary that begins
with either of those prefixes. For any binary it finds, rpk adds a command for
that name to the rpk command space itself.

No plugin name can shadow an existing rpk command, and only one plugin can
exist under a given name at once. Plugins are added to the rpk command space on
a first-seen basis. If you have two plugins rpk-foo, and the second is
discovered later on in the $PATH directories, then only the first will be used.
The second will be ignored.

Plugins that have an rpk.ac- prefix indicate that they support the
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

func newListCommand(fs afero.Fs) *cobra.Command {
	var local bool

	cmd := &cobra.Command{
		Use:   "list",
		Short: "List all available plugins.",
		Long: `List all available plugins.

By default, this command fetches the remote manifest and prints plugins
available for download. Any plugin that is already downloaded is prefixed with
an asterisk. If a locally installed plugin has a different sha256sum as the one
specified in the manifest, or if the sha256sum could not be calculated for the
local plugin, an additional message is printed.

You can specify --local to print all locally installed plugins, as well as
whether you have "shadowed" plugins (the same plugin specified multiple times).
`,

		Args: cobra.ExactArgs(0),
		Run: func(*cobra.Command, []string) {
			installed := plugin.ListPlugins(fs, plugin.UserPaths())

			if local {
				installed.Sort()

				tw := out.NewTable("NAME", "PATH", "SHADOWS")
				defer tw.Flush()

				for _, p := range installed {
					var shadowed string
					if len(p.ShadowedPaths) > 0 {
						shadowed = p.ShadowedPaths[0]
					}
					tw.Print(p.Name(), p.Path, shadowed)

					if len(p.ShadowedPaths) < 1 {
						continue
					}
					for _, shadowed = range p.ShadowedPaths[1:] {
						tw.Print("", "", shadowed)
					}
				}

				return
			}

			m, err := getManifest()
			out.MaybeDieErr(err)

			tw := out.NewTable("NAME", "DESCRIPTION", "MESSAGE")
			defer tw.Flush()
			for _, entry := range m.Plugins {
				name := entry.Name
				_, entrySha, _ := entry.PathShaForUser()

				var message string

				p, exists := installed.Find(name)
				if exists {
					name = "*" + name

					sha, err := plugin.Sha256Path(fs, p.Path)
					if err != nil {
						message = fmt.Sprintf("unable to calculate local binary sha256: %v", err)
					} else if sha != entrySha {
						message = "local binary sha256 differs from manifest sha256"
					}
				}

				tw.Print(name, entry.Description, message)
			}
		},
	}

	cmd.Flags().BoolVarP(&local, "local", "l", false, "list locally installed plugins and shadowed plugins")

	return cmd
}

func newInstallCommand(fs afero.Fs) *cobra.Command {
	var dir string
	var update bool
	cmd := &cobra.Command{
		Use:     "install [PLUGIN]",
		Aliases: []string{"download"},
		Short:   "Install an rpk plugin.",
		Long: `Install an rpk plugin.

An rpk plugin must be saved in a directory that is in your $PATH. By default,
this command installs plugins to the first directory in your $PATH. This can
be overridden by specifying the --bin-dir flag.
`,
		Args: cobra.ExactArgs(1),
		Run: func(_ *cobra.Command, args []string) {
			name := args[0]

			fmt.Printf("Searching plugin manifest for %q...\n", name)
			m, err := getManifest()
			out.MaybeDieErr(err)

			p, err := m.FindEntry(name)
			out.MaybeDieErr(err)

			_, remoteSha, err := p.PathShaForUser()
			out.MaybeDieErr(err)

			var userAlreadyHas bool
			installed := plugin.ListPlugins(fs, plugin.UserPaths())
			for _, p := range installed {
				if name == p.Name() {
					sha, err := plugin.Sha256Path(fs, p.Path)
					out.MaybeDieErr(err)

					if sha == remoteSha {
						out.Exit("Plugin %q is already installed and up to date!", name)
					}

					msg := fmt.Sprintf(`Plugin %q is already installed, but is different from the remote plugin!

 Local sha256: %s
Remote sha256: %s

`, name, sha, remoteSha)
					if !update {
						out.Exit(msg + "--update was not requested, exiting!")
					}
					fmt.Println(msg + "Downloading and validating updated plugin...")
					userAlreadyHas = true
				}
			}
			if !userAlreadyHas {
				fmt.Println("Found! Downloading and validating plugin...")
			}

			body, err := p.DownloadForUser(urlBase)
			out.MaybeDieErr(err)

			fmt.Println("Downloaded! Writing plugin to disk...")
			dst, err := plugin.WriteBinary(fs, p.Name, dir, body, p.HelpAutoComplete)
			out.MaybeDieErr(err)

			fmt.Printf("Success! Plugin %q has been saved to %q and is now ready to use!\n", p.Name, dst)

			if p.HelpAutoComplete {
				fmt.Printf(`
This plugin supports autocompletion through rpk.

If you enable rpk autocompletion, start a new terminal to tab complete your new
command %q!
`, p.Name)
			} else {
				fmt.Printf(`

If you enable rpk autocompletion, start a new terminal to tab complete your new
command %q!
`, p.Name)
			}
		},
	}

	var err error
	dir, err = determineBinDir()
	out.MaybeDieErr(err)

	cmd.Flags().StringVar(&dir, "dir", dir, "destination directory to save the installed plugin (defaults to the first dir in $PATH)")
	cmd.Flags().BoolVarP(&update, "update", "u", false, "update a locally installed plugin if it differs from the current remote version")

	return cmd
}

func newUninstallCommand(fs afero.Fs) *cobra.Command {
	var includeShadowed bool
	cmd := &cobra.Command{
		Use:     "uninstall [NAME]",
		Aliases: []string{"rm"},
		Short:   "Uninstall / remove an existing local plugin.",
		Long: `Uninstall / remove an existing local plugin.

This command lists locally installed plugins and removes the first plugin that
matches the requested removal. If --include-shadowed is specified, this command
also removes all shadowed plugins of the same name.
`,

		Args: cobra.ExactArgs(1),

		Run: func(_ *cobra.Command, args []string) {
			name := args[0]
			installed := plugin.ListPlugins(fs, plugin.UserPaths())

			p, ok := installed.Find(name)
			if !ok {
				out.Exit("Plugin %q does not appear to be installed, exiting!", name)
			}

			err := os.Remove(p.Path)
			out.MaybeDie(err, "unable to remove %q: %v", p.Path, err)
			fmt.Printf("Removed %q.\n", p.Path)

			if !includeShadowed {
				return
			}
			for _, shadowed := range p.ShadowedPaths {
				err = os.Remove(shadowed)
				out.MaybeDie(err, "unable to remove shadowed at %q: %v", shadowed, err)
				fmt.Printf("Removed shadowed at %q.\n", shadowed)
			}
		},
	}
	cmd.Flags().BoolVar(&includeShadowed, "include-shadowed", false, "also remove shadowed plugins that have the same name")
	return cmd
}

func getManifest() (*plugin.Manifest, error) {
	return plugin.DownloadManifest(urlBase + "/manifest.yaml")
}

func determineBinDir() (string, error) {
	paths := plugin.UserPaths()
	if len(paths) == 0 {
		return "", errors.New("unable to determine where to save plugin: PATH list is empty")
	}
	return paths[0], nil
}
