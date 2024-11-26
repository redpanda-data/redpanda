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
	"fmt"
	"os"
	"sync"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

type cancelResponse struct {
	Broker   string
	Canceled bool
	Error    string
}

func newCancelCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var noConfirm bool
	var jobID string
	cmd := &cobra.Command{
		Use:   "cancel",
		Short: "Cancel a remote bundle execution",
		Long: `Cancel a remote bundle execution.

This command cancels the debug collection process in a remote cluster that
you configured in flags, environment variables, or your rpk profile.

Use the flag '--job-id' to only cancel the debug bundle with
the given job ID.

Use the flag '--no-confirm' to avoid the confirmation prompt.
`,
		Args: cobra.NoArgs,
		Run: func(cmd *cobra.Command, _ []string) {
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			config.CheckExitCloudAdmin(p)

			status, anyErr, _, cache := executeBundleStatus(cmd.Context(), fs, p)
			if jobID != "" {
				status = filterStatusByJobID(status, jobID)
			}
			if !noConfirm {
				printTextBundleStatus(status, anyErr)
				confirmed, err := out.Confirm("Confirm debug bundle cancel from these brokers?")
				out.MaybeDie(err, "unable to confirm cancel: %v;  if you want to select a single broker, use -X admin.hosts=<brokers,to,cancel>", err)
				if !confirmed {
					out.Exit("operation canceled; if you want to select a single broker, use -X admin.hosts=<brokers,to,cancel>")
				}
			}
			var (
				wg           sync.WaitGroup
				rwMu         sync.RWMutex // read from cache.
				mu           sync.Mutex   // write to status.
				response     []cancelResponse
				anyCancelErr bool
			)
			updateStatus := func(resp cancelResponse, err error) {
				mu.Lock()
				defer mu.Unlock()
				anyCancelErr = anyCancelErr || err != nil
				response = append(response, resp)
			}
			for _, s := range status {
				wg.Add(1)
				go func(addr, jobID string) {
					defer wg.Done()
					rwMu.RLock()
					cl := cache[addr]
					rwMu.RUnlock()
					resp := cancelResponse{Broker: addr}
					err := cl.CancelDebugBundleProcess(cmd.Context(), jobID)
					if err != nil {
						resp.Error = fmt.Sprintf("unable to cancel debug bundle: %s", tryMessageFromErr(err))
						updateStatus(resp, err)
						return
					}
					resp.Canceled = true
					updateStatus(resp, nil)
				}(s.Broker, s.JobID)
			}
			wg.Wait()
			headers := []string{"broker", "canceled"}
			if anyCancelErr {
				headers = append(headers, "error")
				defer os.Exit(1)
			}

			tw := out.NewTable(headers...)
			defer tw.Flush()
			for _, row := range response {
				tw.PrintStructFields(row)
			}
		},
	}
	cmd.Flags().StringVar(&jobID, "job-id", "", "ID of the job to cancel the debug bundle")
	cmd.Flags().BoolVar(&noConfirm, "no-confirm", false, "Disable confirmation prompt")
	return cmd
}

func filterStatusByJobID(status []statusResponse, jobID string) []statusResponse {
	var filtered []statusResponse
	for _, s := range status {
		if s.JobID == jobID {
			filtered = append(filtered, s)
		}
	}
	return filtered
}
