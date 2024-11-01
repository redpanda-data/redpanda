// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package remotebundle

import (
	"archive/zip"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/redpanda-data/common-go/rpadmin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/debug/common"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
)

type downloadResponse struct {
	Broker     string
	Downloaded bool
	Error      string
}

func newDownloadCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var (
		noConfirm bool
		jobID     string
		outFile   string
	)
	cmd := &cobra.Command{
		Use:   "download",
		Short: "Download a remote debug bundle",
		Long: `Download a remote debug bundle.

This command downloads the debug collection process in a remote cluster that
you configured in flags, environment variables, or your rpk profile.

Use the flag '--job-id' to only download the debug bundle with
the given job ID.

Use the flag '--no-confirm' to avoid the confirmation prompt.
`,
		Args: cobra.NoArgs,
		Run: func(cmd *cobra.Command, _ []string) {
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			config.CheckExitCloudAdmin(p)

			status, _, _, cache := executeBundleStatus(cmd.Context(), fs, p)
			if jobID != "" {
				status = filterStatusByJobID(status, jobID)
			}
			ready := filterReadyBrokers(status)
			if len(ready) == 0 {
				out.Die("There are no bundles ready for download; to check status run 'rpk debug remote-bundle status'")
			}
			if len(ready) != len(status) {
				fmt.Printf("WARNING: There are (%d) bundles not ready to download; to check the status run 'rpk debug remote-bundle status'\n", len(status)-len(ready))
			}

			if !noConfirm {
				printTextBundleStatus(ready, false)
				confirmed, err := out.Confirm("Confirm debug bundle download from these brokers?")
				out.MaybeDie(err, "unable to confirm download: %v;  if you want to select a single broker, use -X admin.hosts=<brokers,to,cancel>", err)
				if !confirmed {
					out.Exit("operation canceled; if you want to select a single broker, use -X admin.hosts=<brokers,to,cancel>")
				}
			}
			downloadPath, err := fileLocation(fs, outFile)
			out.MaybeDie(err, "unable to determine download path: %v", err)

			anyResponseErr, response, err := downloadBundle(cmd.Context(), fs, downloadPath, status, cache)
			out.MaybeDie(err, "unable to download bundle: %v", err)

			headers := []string{"broker", "downloaded"}
			if anyResponseErr {
				headers = append(headers, "error")
				defer os.Exit(1)
			}
			tw := out.NewTable(headers...)
			for _, row := range response {
				tw.PrintStructFields(row)
			}
			tw.Flush()

			if anyResponseErr {
				fmt.Printf("\nDownloaded remote debug bundle to %v, but there were errors while downloading the broker files.\n", downloadPath)
				os.Exit(1)
			}
			fmt.Printf("\nSuccessfully downloaded remote debug bundle to %v\n", downloadPath)
		},
	}
	cmd.Flags().StringVarP(&outFile, "output", "o", "", "The file path where the debug file will be written (default ./<timestamp>-remote-bundle.zip)")
	cmd.Flags().StringVar(&jobID, "job-id", "", "ID of the job to download the debug bundle from")
	cmd.Flags().BoolVar(&noConfirm, "no-confirm", false, "Disable confirmation prompt")
	return cmd
}

func downloadBundle(ctx context.Context, fs afero.Fs, downloadPath string, status []statusResponse, cache map[string]*rpadmin.AdminAPI) (bool, []downloadResponse, error) {
	var (
		rwMu     sync.RWMutex // read from cache
		statusMu sync.Mutex   // write to status
		zipMu    sync.Mutex   // write to file
		response []downloadResponse
		anyErr   bool
	)
	f, err := fs.OpenFile(downloadPath, os.O_CREATE|os.O_WRONLY, os.FileMode(0o755)) // This is the final zip file, a zip of zips.
	if err != nil {
		return false, nil, fmt.Errorf("unable to create zip file: %v", err)
	}
	w := zip.NewWriter(f)
	defer w.Close()

	downloadRoot := strings.TrimSuffix(filepath.Base(downloadPath), ".zip")
	updateStatus := func(resp downloadResponse, err error) {
		statusMu.Lock()
		defer statusMu.Unlock()
		anyErr = anyErr || err != nil
		response = append(response, resp)
	}
	writeToZip := func(broker, jobID string, httpResp *http.Response) error {
		if httpResp != nil {
			zipMu.Lock()
			defer zipMu.Unlock()
			wr, err := w.CreateHeader(&zip.FileHeader{
				// Each individual zip file will be stored under a directory
				// with the broker address.
				Name:     filepath.Join(downloadRoot, common.SanitizeName(broker), jobID+".zip"),
				Method:   zip.Deflate,
				Modified: time.Now(),
			})
			if err != nil {
				return err
			}
			_, err = io.Copy(wr, httpResp.Body)
			if err != nil {
				return err
			}
		}
		return nil
	}

	grp, gCtx := errgroup.WithContext(ctx)
	for _, s := range status {
		grp.Go(func() error {
			rwMu.RLock()
			cl := cache[s.Broker]
			rwMu.RUnlock()
			resp := downloadResponse{Broker: s.Broker}
			httpResp, err := cl.DownloadDebugBundleFile(gCtx, s.filename)
			if err != nil {
				resp.Error = fmt.Sprintf("unable to cancel debug bundle: %s", tryMessageFromErr(err))
				updateStatus(resp, err)
				return nil // No need to return error, we print to this to the table.
			}
			defer httpResp.Body.Close()
			resp.Downloaded = true
			updateStatus(resp, nil)
			err = writeToZip(resp.Broker, s.JobID, httpResp)
			if err != nil {
				return fmt.Errorf("error writing debug bundle of %v: %v", s.Broker, err)
			}
			return nil
		})
	}
	if err := grp.Wait(); err != nil {
		return false, nil, err
	}
	return anyErr, response, nil
}

func filterReadyBrokers(status []statusResponse) []statusResponse {
	var filtered []statusResponse
	for _, s := range status {
		if strings.EqualFold(s.Status, "success") {
			filtered = append(filtered, s)
		}
	}
	return filtered
}

func fileLocation(fs afero.Fs, path string) (string, error) {
	// If it's empty, use "./<timestamp>-remote-bundle.zip"
	if path == "" {
		path = fmt.Sprintf("%d-remote-bundle.zip", time.Now().Unix())
	} else if isDir, _ := afero.IsDir(fs, path); isDir {
		return "", fmt.Errorf("output file path is a directory, please specify the name of the file")
	}

	var finalPath string
	// Check for file extension, if extension is empty, defaults to .zip
	switch ext := filepath.Ext(path); ext {
	case ".zip":
		finalPath = path
	case "":
		finalPath = path + ".zip"
	default:
		return "", fmt.Errorf("extension %q not supported", ext)
	}
	return finalPath, nil
}
