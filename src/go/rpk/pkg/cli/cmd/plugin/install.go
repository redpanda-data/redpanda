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
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime"
	"time"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/plugin"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
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

			name := args[0]
			var (
				autoComplete bool
				body         []byte
				err          error
			)
			if len(version) > 0 {
				body, err = tryDirectDownload(name, version)
				if err != nil {
					log.Debugf("unable to download: %v", err)
				}
			}
			if body == nil {
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

				body, err = p.DownloadForUser(urlBase)
				out.MaybeDieErr(err)
				autoComplete = p.HelpAutoComplete
			}

			fmt.Println("Downloaded! Writing plugin to disk...")
			dst, err := plugin.WriteBinary(fs, name, dir, body, autoComplete)
			out.MaybeDieErr(err)

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

	var err error
	dir, err = plugin.DefaultBinPath()
	out.MaybeDieErr(err)

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
		if os.Getuid() == 0 && os.Getenv("SUDO_UID") != "0" {
			return fmt.Errorf("detected rpk is running with sudo; please execute this command without sudo to avoid saving the plugin as a root owned binary in %s", path)
		}
		err = os.MkdirAll(path, 0o755)
		if err != nil {
			return fmt.Errorf("unable to create the plugin bin directory: %v", err)
		}
	}
	return nil
}

func tryDirectDownload(name, version string) ([]byte, error) {
	u := fmt.Sprintf("https://dl.redpanda.com/public/rpk-plugins/raw/names/%[1]s-%[2]s-%[3]s/versions/%[4]s/%[1]s.tgz", name, runtime.GOOS, runtime.GOARCH, version)
	fmt.Printf("Searching for plugin %q in %s...\n", name, u)

	client := &http.Client{Timeout: 100 * time.Second}
	req, err := http.NewRequestWithContext(
		context.Background(),
		http.MethodGet,
		u,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("unable to create request %s: %v", u, err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("unable to issue request to %s: %v", u, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("unsuccessful plugin response from %s, status: %s", u, http.StatusText(resp.StatusCode))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("unable to read response from %s: %v", u, err)
	}

	gzr, err := gzip.NewReader(bytes.NewBuffer(body))
	if err != nil {
		return nil, fmt.Errorf("unable to create gzip reader: %w", err)
	}
	if body, err = io.ReadAll(gzr); err != nil {
		return nil, fmt.Errorf("unable to gzip decompress plugin: %w", err)
	}
	if err = gzr.Close(); err != nil {
		return nil, fmt.Errorf("unable to close gzip reader: %w", err)
	}

	return body, nil
}
