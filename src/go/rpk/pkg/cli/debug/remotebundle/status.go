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
	"context"
	"fmt"
	"os"
	"slices"
	"sync"

	"github.com/redpanda-data/common-go/rpadmin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

type statusResponse struct {
	Broker string `json:"broker" yaml:"broker"`
	Status string `json:"status,omitempty" yaml:"status,omitempty"`
	JobID  string `json:"job_id,omitempty" yaml:"job_id,omitempty"`
	Error  string `json:"error,omitempty" yaml:"error,omitempty"`

	filename string
}

func newStatusCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Get the status of the current debug bundle process",
		Long: `Get the status of the current debug bundle process.

This command prints the status of the debug bundle process in a remote cluster 
that you configured in flags, environment variables, or your rpk profile.

When the process completes, you can download it with 
  'rpk debug remote-bundle download'.
`,
		Args: cobra.NoArgs,
		Run: func(cmd *cobra.Command, _ []string) {
			f := p.Formatter
			if h, ok := f.Help([]statusResponse{}); ok {
				out.Exit(h)
			}
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			config.CheckExitCloudAdmin(p)

			slices.Sort(p.AdminAPI.Addresses)
			status, anyErr, anyOK, _ := executeBundleStatus(cmd.Context(), fs, p)

			if isText, _, formatted, err := f.Format(status); !isText {
				out.MaybeDie(err, "unable to print status in the required format %q: %v", f.Kind, err)
				fmt.Println(formatted)
				return
			}
			printTextBundleStatus(status, anyErr)

			if anyOK {
				fmt.Printf(`
After the process is completed, you may retrieve the debug bundle using:
  rpk debug remote-bundle download
`)
			}
			if anyErr {
				os.Exit(1)
			}
		},
	}
	p.InstallFormatFlag(cmd)
	return cmd
}

func executeBundleStatus(ctx context.Context, fs afero.Fs, p *config.RpkProfile) (status []statusResponse, anyErr, anyOK bool, clientCache map[string]*rpadmin.AdminAPI) {
	var (
		wg       sync.WaitGroup
		sMu, mMu sync.Mutex
	)
	clientCache = make(map[string]*rpadmin.AdminAPI)
	updateStatus := func(resp statusResponse, isErr bool) {
		sMu.Lock()
		defer sMu.Unlock()
		anyErr = anyErr || isErr
		anyOK = anyOK || !isErr
		status = append(status, resp)
	}
	addClientToCache := func(addr string, client *rpadmin.AdminAPI) {
		mMu.Lock()
		defer mMu.Unlock()
		clientCache[addr] = client
	}
	for _, addr := range p.AdminAPI.Addresses {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()

			resp := statusResponse{
				Broker: addr,
			}
			client, err := adminapi.NewHostClient(fs, p, addr)
			if err != nil {
				resp.Error = fmt.Sprintf("unable to connect: %s", tryMessageFromErr(err))
				updateStatus(resp, true)
				return
			}
			addClientToCache(addr, client)
			bs, err := client.GetDebugBundleStatus(ctx)
			if err != nil {
				resp.Error = fmt.Sprintf("unable to get bundle status: %s", tryMessageFromErr(err))
				updateStatus(resp, true)
				return
			}

			resp.Status = bs.Status
			resp.JobID = bs.JobID
			resp.filename = bs.Filename
			updateStatus(resp, false)
		}(addr)
	}
	wg.Wait()
	return
}

func printTextBundleStatus(status []statusResponse, anyErr bool) {
	headers := []string{"broker", "status", "job-ID"}
	if anyErr {
		headers = append(headers, "error")
	}
	tw := out.NewTable(headers...)
	defer tw.Flush()
	for _, row := range status {
		// Can't use tw.PrintStructField as we don't want to print filename.
		if anyErr {
			tw.Print(row.Broker, row.Status, row.JobID, row.Error)
		} else {
			tw.Print(row.Broker, row.Status, row.JobID)
		}
	}
}
