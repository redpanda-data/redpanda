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
	"strings"
	"sync"
	"time"

	"github.com/docker/go-units"
	"github.com/google/uuid"
	"github.com/redpanda-data/common-go/rpadmin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/debug/debugbundle"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/labels"
)

type startResponse struct {
	Broker string
	JobID  string
	Error  string
}

func newStartCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var (
		noConfirm   bool
		jobID       string
		opts        remoteBundleOptions
		wait        bool
		waitTimeout time.Duration
	)
	cmd := &cobra.Command{
		Use:   "start",
		Short: "Start a remote debug bundle collection in your cluster",
		Long: `Start a remote debug bundle collection in your cluster.

This command starts the debug collection process in a remote cluster that
you configured in flags, environment variables, or your rpk profile.

After starting the debug collection process, you can query the status with
'rpk debug remote-bundle status'. When it completes, you can download it with
'rpk debug remote-bundle download'.

Use the flag '--no-confirm' to avoid the confirmation prompt.
`,
		Args: cobra.NoArgs,
		PreRun: func(_ *cobra.Command, _ []string) {
			clsl, err := units.FromHumanSize(opts.ControllerLogsSizeLimit)
			out.MaybeDie(err, "unable to parse --controller-logs-size-limit: %v", err)
			opts.controllerLogsSizeLimitBytes = int32(clsl)

			lsl, err := units.FromHumanSize(opts.LogsSizeLimit)
			out.MaybeDie(err, "unable to parse --logs-size-limit: %v", err)
			opts.logsSizeLimitBytes = int32(lsl)

			if len(opts.labelSelectorMap) > 0 {
				labelsMap, err := labels.ConvertSelectorToLabelsMap(strings.Join(opts.LabelSelector, ","))
				out.MaybeDie(err, "unable to parse label-selector flag: %v", err)
				opts.labelSelectorMap = labelsMap
			}
		},
		Run: func(cmd *cobra.Command, _ []string) {
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			config.CheckExitCloudAdmin(p)

			// Remote debug bundle forwards SASL credentials to the broker so
			// rpk running on the broker can authenticate to Kafka. The broker
			// API currently only accepts a SCRAM-shaped payload; until that
			// gains OAUTHBEARER support (tracked in redpanda#30222), reject
			// up front rather than silently sending the request with no auth
			// (which then fails confusingly on secured clusters).
			if p.KafkaAPI.SASL != nil && strings.EqualFold(p.KafkaAPI.SASL.Mechanism, adminapi.OAuthBearer) {
				out.Die("OAUTHBEARER is not yet supported for remote debug bundle collection; use SCRAM-SHA-256 or SCRAM-SHA-512 credentials to run this command")
			}

			if !noConfirm {
				printBrokers(p.AdminAPI.Addresses)
				confirmed, err := out.Confirm("Confirm debug bundle collection from these brokers?")
				out.MaybeDie(err, "unable to confirm collection: %v;  if you want to select a single broker, use -X admin.hosts=<brokers,to,collect>", err)
				if !confirmed {
					out.Exit("operation canceled; if you want to select a single broker, use -X admin.hosts=<brokers,to,collect>")
				}
			}
			if jobID == "" {
				id, err := uuid.NewRandom()
				out.MaybeDie(err, "unable to generate a UUID for the Job ID: %v", err)
				jobID = id.String()
			}
			var (
				response       []startResponse
				wg             sync.WaitGroup
				mu             sync.Mutex
				anyErr, anyOK  bool
				alreadyRunning bool
			)
			updateStatus := func(resp startResponse, err error) {
				mu.Lock()
				defer mu.Unlock()
				anyErr = anyErr || err != nil
				anyOK = anyOK || err == nil
				alreadyRunning = alreadyRunning || isAlreadyStartedErr(err)
				response = append(response, resp)
			}
			for _, addr := range p.AdminAPI.Addresses {
				wg.Add(1)
				go func(addr string) {
					defer wg.Done()

					resp := startResponse{Broker: addr}
					client, err := adminapi.NewHostClient(fs, p, addr)
					if err != nil {
						resp.Error = fmt.Sprintf("unable to connect: %s", tryMessageFromErr(err))
						updateStatus(resp, err)
						return
					}
					bundleResp, err := client.CreateDebugBundle(cmd.Context(), jobID, opts.toRpadminOptions(p)...)
					if err != nil {
						isAlreadyStartedErr(err)
						resp.Error = fmt.Sprintf("unable to start debug bundle: %s", tryMessageFromErr(err))
						updateStatus(resp, err)
						return
					}
					resp.JobID = bundleResp.JobID
					updateStatus(resp, nil)
				}(addr)
			}
			wg.Wait()

			headers := []string{"broker", "job-ID"}
			if anyErr {
				headers = append(headers, "error")
				defer os.Exit(1)
			}

			tw := out.NewTable(headers...)
			for _, row := range response {
				tw.PrintStructFields(row)
			}
			tw.Flush()

			if alreadyRunning {
				fmt.Printf(`
A debug bundle collection is already in process. To cancel it, run:
  rpk debug remote-bundle cancel
`)
			}
			if wait {
				fmt.Printf(`
The debug bundle collection process has started with Job-ID %v.
Waiting for collection to complete...
`, jobID)
				ctx, cancel := context.WithTimeout(cmd.Context(), waitTimeout)
				defer cancel()

				statusCR := "\n"
				if !noConfirm {
					// Only use CRs if we've been launched interactively, so we don't break logging redirection for scripts
					statusCR = "\r"
				}

				var done bool
				for !done {
					select {
					case <-ctx.Done():
						// Context canceled or timed out
						out.Die("%s", ctx.Err())
					case <-time.After(10 * time.Second):
						status, _, _, _ := executeBundleStatus(ctx, fs, p)
						ready, errorred := filterCompletedBrokers(status)
						// Print in all cases, even if we're about to follow with a completion, to ensure the last print of the CR'd line is correct
						fmt.Printf("%sJob-ID %v status: %v/%v ready (%v errored)...", statusCR, jobID, len(ready), len(status), len(errorred))
						if len(ready)+len(errorred) == len(status) {
							if len(ready) == 0 {
								out.Die(`the debug bundle collection process with Job-ID %v has completed, but no bundles were successfully created; to check the status, run 'rpk debug remote-bundle status'`, jobID)
							}
							fmt.Printf(`
The debug bundle collection process with Job-ID %v has completed. To download the bundles, run:
  rpk debug remote-bundle download
`, jobID)
							done = true
						}
					}
				}
			} else if anyOK {
				fmt.Printf(`
The debug bundle collection process has started with Job-ID %v. To check the 
status, run:
  rpk debug remote-bundle status
`, jobID)
			}
		},
	}
	f := cmd.Flags()
	f.StringVar(&jobID, "job-id", "", "Custom UUID to assign to the job that generates the debug bundle")
	f.BoolVar(&noConfirm, "no-confirm", false, "Disable confirmation prompt")
	f.BoolVar(&wait, "wait", false, "Wait for completion of remote bundle before returning")
	f.DurationVar(&waitTimeout, "wait-timeout", 300*time.Second, "How long to wait locally for remote-bundle completion if --wait is specified. Collection on cluster will continue regardless.")

	// Debug bundle options:
	opts.InstallFlags(f)

	return cmd
}

type remoteBundleOptions struct {
	debugbundle.DebugBundleSharedOptions
	controllerLogsSizeLimitBytes int32
	logsSizeLimitBytes           int32
	labelSelectorMap             map[string]string
}

func (o *remoteBundleOptions) toRpadminOptions(p *config.RpkProfile) []rpadmin.DebugBundleOption {
	var opts []rpadmin.DebugBundleOption
	if o.controllerLogsSizeLimitBytes > 0 {
		opts = append(opts, rpadmin.WithControllerLogsSizeLimitBytes(o.controllerLogsSizeLimitBytes))
	}
	if cpuWait := int32(o.CPUProfilerWait.Seconds()); cpuWait > 0 {
		opts = append(opts, rpadmin.WithCPUProfilerWaitSeconds(cpuWait))
	}
	if o.LogsSince != "" {
		opts = append(opts, rpadmin.WithLogsSince(o.LogsSince))
	}
	if o.LogsUntil != "" {
		opts = append(opts, rpadmin.WithLogsUntil(o.LogsUntil))
	}
	if o.logsSizeLimitBytes > 0 {
		opts = append(opts, rpadmin.WithLogsSizeLimitBytes(o.logsSizeLimitBytes))
	}
	if mis := int32(o.MetricsInterval.Seconds()); mis > 0 {
		opts = append(opts, rpadmin.WithMetricsIntervalSeconds(mis))
	}
	if o.MetricsSampleCount > 0 {
		opts = append(opts, rpadmin.WithMetricsSamples(int32(o.MetricsSampleCount)))
	}
	if len(o.PartitionFlag) > 0 {
		opts = append(opts, rpadmin.WithPartitions(o.PartitionFlag))
	}
	if o.Namespace != "" {
		opts = append(opts, rpadmin.WithNamespace(o.Namespace))
	}
	if len(o.LabelSelector) > 0 {
		dbls := make([]rpadmin.DebugBundleLabelSelector, 0, len(o.LabelSelector))
		for k, v := range o.labelSelectorMap {
			dbls = append(dbls, rpadmin.DebugBundleLabelSelector{Key: k, Value: v})
		}
		opts = append(opts, rpadmin.WithLabelSelector(dbls))
	}
	if p.HasSASLCredentials() {
		s := p.KafkaAPI.SASL
		opts = append(opts, rpadmin.WithSCRAMAuthentication(s.User, s.Password, s.Mechanism))
	}
	if tls := p.KafkaAPI.TLS; tls != nil {
		opts = append(opts, rpadmin.WithTLS(true, tls.InsecureSkipVerify))
	}
	return opts
}
