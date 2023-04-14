// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package plugin

import (
	"context"
	"fmt"
	"os"
	"runtime"

	rpkos "github.com/redpanda-data/redpanda/src/go/rpk/pkg/os"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/plugin"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

func newInstallCommand(fs afero.Fs) *cobra.Command {
	var (
		dir     string
		update  bool
		version string
	)
	cmd := &cobra.Command{
		Use:     "install [PLUGIN]",
		Aliases: []string{"download"},
		Short:   "Install an rpk plugin",
		Long: `Install an rpk plugin.

An rpk plugin must be saved in $HOME/.local/bin or in a directory that is in 
your $PATH. By default, this command installs plugins to $HOME/.local/bin. This 
can be overridden by specifying the --dir flag.

If --dir is not present, rpk will create $HOME/.local/bin if it does not exist.
`,
		Args: cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			// If we don't explicitly set --dir flag, then dir is the default
			// path, so we check if the path exists.
			if !cmd.Flags().Changed("dir") {
				err := checkAndCreateDefaultPath(fs)
				out.MaybeDieErr(err)
			}

			installed := plugin.ListPlugins(fs, plugin.UserPaths())

			name := args[0]
			var (
				autoComplete bool
				body         []byte
				err          error

				remoteSha string
			)
			if len(version) > 0 {
				// We do not use sha's for this temporary
				// direct download path.
				body, err = tryDirectDownload(cmd.Context(), name, version)
				if err != nil {
					zap.L().Sugar().Debugf("unable to download: %v", err)
				}
			}
			if body == nil {
				fmt.Printf("Searching plugin manifest for %q...\n", name)
				m, err := getManifest()
				out.MaybeDieErr(err)

				p, err := m.FindEntry(name)
				out.MaybeDieErr(err)

				_, remoteSha, err = p.PathShaForUser()
				out.MaybeDieErr(err)

				var userAlreadyHas bool
				for _, p := range installed {
					if name == p.FullName() {
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

				body, err = p.DownloadForUser(urlBase)
				out.MaybeDieErr(err)
				autoComplete = p.HelpAutoComplete
			}

			fmt.Println("Downloaded! Writing plugin to disk...")
			dst, err := plugin.WriteBinary(fs, name, dir, body, autoComplete, false)
			out.MaybeDieErr(err)

			// If we add shas to filenames, then writing our binary
			// likely will not replace the old plugin. So, if the
			// old plugin exists, the path is *not* equal to the
			// new path, but this is technically the same "plugin
			// path", we remove the old plugin manually.
			if old, exists := installed.Find(name); exists && old.Path != dst && plugin.IsSamePluginPath(old.Path, dst) {
				if err := os.Remove(old.Path); err != nil {
					fmt.Printf("Unable to remove old plugin at %q: %v\n", old.Path, err)
				}
			}

			fmt.Printf("Success! Plugin %q has been saved to %q and is now ready to use!\n", name, dst)

			if autoComplete {
				fmt.Printf(`
This plugin supports autocompletion through rpk.

If you enable rpk autocompletion, start a new terminal to tab complete your new
command %q!
`, name)
			} else {
				fmt.Printf(`

If you enable rpk autocompletion, start a new terminal to tab complete your new
command %q!
`, name)
			}
		},
	}

	dir, _ = plugin.DefaultBinPath()

	cmd.Flags().StringVar(&dir, "dir", dir, "Destination directory to save the installed plugin (defaults to $HOME/.local/bin)")
	cmd.Flags().BoolVarP(&update, "update", "u", false, "Update a locally installed plugin if it differs from the current remote version")
	cmd.Flags().StringVar(&version, "version", "", "Version of the plugin you wish to download")
	cmd.Flags().MarkHidden("version")

	return cmd
}

// checkAndCreateDefaultPath will verify if the plugin.DefaultBinPath exists, if
// not, it will create the directory.
func checkAndCreateDefaultPath(fs afero.Fs) error {
	path, err := plugin.DefaultBinPath()
	if err != nil {
		return err
	}

	// If we fail to check if the directory exists, then at worst, we try to
	// recreate the directory (and then we may fail there and actually return
	// the error).
	if exists, _ := afero.DirExists(fs, path); !exists {
		if rpkos.IsRunningSudo() {
			return fmt.Errorf("detected rpk is running with sudo; please execute this command without sudo to avoid saving the plugin as a root owned binary in %s", path)
		}
		err = os.MkdirAll(path, 0o755)
		if err != nil {
			return fmt.Errorf("unable to create the plugin bin directory: %v", err)
		}
	}
	return nil
}

func tryDirectDownload(ctx context.Context, name, version string) ([]byte, error) {
	u := fmt.Sprintf("https://dl.redpanda.com/public/rpk-plugins/raw/names/%[1]s-%[2]s-%[3]s/versions/%[4]s/%[1]s.tgz", name, runtime.GOOS, runtime.GOARCH, version)
	fmt.Printf("Searching for plugin %q in %s...\n", name, u)
	return plugin.Download(ctx, u, true, "")
}
