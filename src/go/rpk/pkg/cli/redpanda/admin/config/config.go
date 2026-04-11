// Package config contains commands to talk to Redpanda's admin config
// endpoints.
//
// This package is named config to avoid import overlap with the rpk
// config package.
package config

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"

	"github.com/redpanda-data/common-go/rpadmin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/redpanda"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// NewCommand returns the config admin command.
func NewCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config",
		Short: "View or modify Redpanda configuration through the admin listener",
		Args:  cobra.ExactArgs(0),
	}
	cmd.AddCommand(
		newPrintCommand(fs, p),
		newLogLevelCommand(fs, p),
	)
	return cmd
}

func newPrintCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var host string
	cmd := &cobra.Command{
		Use:     "print",
		Aliases: []string{"dump", "list", "ls", "display"},
		Short:   "Display the current Redpanda configuration",
		Args:    cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, _ []string) {
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			config.CheckExitCloudAdmin(p)

			cl, err := adminapi.NewHostClient(fs, p, host)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			conf, err := cl.Config(cmd.Context(), true)
			out.MaybeDie(err, "unable to request configuration: %v", err)

			marshaled, err := json.MarshalIndent(conf, "", "  ")
			out.MaybeDie(err, "unable to json encode configuration: %v", err)

			fmt.Println(string(marshaled))
		},
	}

	cmd.Flags().StringVar(&host, "host", "", "either a hostname or an index into rpk.admin_api.addresses config section to select the hosts to issue the request to")
	cobra.MarkFlagRequired(cmd.Flags(), "host")

	return cmd
}

// 'rpk redpanda admin config log-level set'.
func newLogLevelCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "log-level",
		Short: "Manage a broker's log level",
		Args:  cobra.ExactArgs(0),
	}
	cmd.AddCommand(
		newLogLevelSetCommand(fs, p),
	)
	return cmd
}

func newLogLevelSetCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var (
		host          string
		level         string
		expirySeconds int
		helpLoggers   bool
	)
	cmd := &cobra.Command{
		Use:   "set [LOGGERS...]",
		Short: "Set broker logger's log level",
		Long: `Set broker logger's log level.

This command temporarily changes a broker logger's log level. Each Redpanda
broker has many loggers, and each can be individually changed. Any change
to a logger persists for a limited amount of time, so as to ensure you do
not accidentally enable debug logging permanently.

It is optional to specify a logger; if you do not, this command will prompt
from the set of available loggers.

The special logger "all" enables all loggers. Alternatively, you can specify
many loggers at once.

This command accepts loggers that it does not know of to ensure you can
independently update your redpanda installations from rpk. The success or
failure of enabling each logger is individually printed.

You can use --help-loggers to display available loggers:

  rpk redpanda admin config log-level set --help-loggers [--host <HOST>]
`,
		ValidArgsFunction: func(_ *cobra.Command, _ []string, _ string) ([]string, cobra.ShellCompDirective) {
			return defaultLoggers, cobra.ShellCompDirectiveNoFileComp
		},
		Run: func(cmd *cobra.Command, loggers []string) {
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			config.CheckExitCloudAdmin(p)

			if helpLoggers {
				printLoggers(cmd.Context(), fs, p, host)
				return
			}

			if host == "" {
				out.Die("required flag \"host\" not set")
			}

			cl, err := adminapi.NewHostClient(fs, p, host)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			availableLoggers := discoverLoggers(cmd.Context(), cl, fs)

			switch len(loggers) {
			case 0:
				choices := append([]string{"all"}, availableLoggers...)
				pick, err := out.Pick(choices, "Which logger would you like to set (all selects everything)?")
				out.MaybeDie(err, "unable to pick logger: %v", err)
				if pick == "all" {
					loggers = availableLoggers
				} else {
					loggers = []string{pick}
				}

			case 1:
				if loggers[0] == "all" {
					loggers = availableLoggers
				}
			}

			type result struct {
				logger string
				err    error
			}
			g, ctx := errgroup.WithContext(cmd.Context())
			g.SetLimit(20)

			var (
				mu      sync.Mutex
				results []result
			)
			for _, logger := range loggers {
				g.Go(func() error {
					err := cl.SetLogLevel(ctx, logger, level, expirySeconds)
					mu.Lock()
					results = append(results, result{logger, err})
					mu.Unlock()
					return nil
				})
			}
			g.Wait()

			var (
				failures  []result
				successes []string
			)
			for _, r := range results {
				if r.err != nil {
					failures = append(failures, r)
				} else {
					successes = append(successes, r.logger)
				}
			}
			if len(successes) > 0 {
				sort.Strings(successes)
				tw := out.NewTable("SUCCESS")
				for _, success := range successes {
					tw.Print(success)
				}
				tw.Flush()
			}
			if len(failures) > 0 {
				sort.Slice(failures, func(i, j int) bool { return failures[i].logger < failures[j].logger })
				if len(successes) > 0 {
					fmt.Println()
				}
				tw := out.NewTable("FAILURE", "ERROR")
				for _, f := range failures {
					tw.Print(f.logger, f.err)
				}
				tw.Flush()
				os.Exit(1)
			}
		},
	}

	cmd.Flags().BoolVar(&helpLoggers, "help-loggers", false, "Display the list of available loggers")
	cmd.Flags().StringVarP(&level, "level", "l", "debug", "Log level to set (error, warn, info, debug, trace)")
	cmd.Flags().IntVarP(&expirySeconds, "expiry-seconds", "e", 300, "Seconds to persist this log level override before redpanda reverts to its previous settings (if 0, persist until shutdown)")
	cmd.Flags().StringVar(&host, "host", "", "Either a hostname or an index into rpk.admin_api.addresses config section to select the hosts to issue the request to")

	return cmd
}

// parseHelpLoggersOutput parses the output of 'redpanda --help-loggers'.
// It looks for the "Available loggers:" header, then collects all subsequent
// indented lines as logger names.
func parseHelpLoggersOutput(output string) ([]string, error) {
	const header = "Available loggers:"
	lines := strings.Split(output, "\n")
	headerIdx := -1
	for i, line := range lines {
		if strings.TrimSpace(line) == header {
			headerIdx = i
			break
		}
	}
	if headerIdx == -1 {
		return nil, fmt.Errorf("header %q not found in output", header)
	}
	// Collect indented lines after the header
	var loggers []string
	for _, line := range lines[headerIdx+1:] {
		if line == "" {
			continue
		}
		// Stop at first non-indented line
		if len(line) > 0 && line[0] != ' ' && line[0] != '\t' {
			break
		}
		if trimmed := strings.TrimSpace(line); trimmed != "" {
			loggers = append(loggers, trimmed)
		}
	}
	if len(loggers) == 0 {
		return nil, fmt.Errorf("no loggers found in output")
	}
	sort.Strings(loggers)
	return loggers, nil
}

// loggersFromBinary attempts to discover available loggers by running the
// local redpanda binary with --help-loggers. This only works on Linux.
func loggersFromBinary(ctx context.Context, fs afero.Fs) ([]string, error) {
	if runtime.GOOS != "linux" {
		return nil, fmt.Errorf("binary discovery only supported on linux")
	}
	installDir, err := redpanda.FindInstallDir(fs)
	if err != nil {
		return nil, fmt.Errorf("unable to find redpanda install directory: %v", err)
	}
	bin := filepath.Join(installDir, "bin", "redpanda")
	output, err := exec.CommandContext(ctx, bin, "--help-loggers").Output()
	if err != nil {
		return nil, fmt.Errorf("unable to run %s --help-loggers: %v", bin, err)
	}
	return parseHelpLoggersOutput(string(output))
}

// printLoggers discovers and prints the available loggers. If --host is not
// set, it uses the first admin address from the profile so we can query the
// API without requiring --host.
func printLoggers(ctx context.Context, fs afero.Fs, p *config.RpkProfile, host string) {
	adminHost := host
	if adminHost == "" {
		if addrs := p.AdminAPI.Addresses; len(addrs) > 0 {
			adminHost = addrs[0]
		}
	}

	available := defaultLoggers
	if adminHost != "" {
		cl, err := adminapi.NewHostClient(fs, p, adminHost)
		if err == nil {
			available = discoverLoggers(ctx, cl, fs)
		}
	}
	tw := out.NewTable("LOGGER")
	defer tw.Flush()
	for _, l := range available {
		tw.Print(l)
	}
}

// discoverLoggers returns the list of available loggers using a fallback chain:
// local binary -> Admin API -> hardcoded default list.
func discoverLoggers(ctx context.Context, cl *rpadmin.AdminAPI, fs afero.Fs) []string {
	// Attempt 1: Local binary (Linux only).
	loggers, err := loggersFromBinary(ctx, fs)
	if err == nil && len(loggers) > 0 {
		return loggers
	}
	zap.L().Sugar().Debugf("Unable to discover loggers from local binary: %v; trying Admin API", err)

	// Attempt 2: Admin API.
	levels, err := cl.GetLogLevels(ctx)
	if err == nil && len(levels) > 0 {
		names := make([]string, 0, len(levels))
		for _, l := range levels {
			if l.Name != "" {
				names = append(names, l.Name)
			}
		}
		sort.Strings(names)
		return names
	}
	zap.L().Sugar().Debugf("Unable to discover loggers from Admin API: %v; using default list", err)

	// Attempt 3: Hardcoded fallback.
	return defaultLoggers
}

// defaultLoggers is the fallback list of loggers used when dynamic discovery
// fails. To regenerate this list, run 'redpanda --help-loggers'.
var defaultLoggers = []string{
	"abs",
	"admin/proxy/client",
	"admin/proxy/service",
	"admin_api_server",
	"admin_api_server/broker_service",
	"admin_api_server/cluster_service",
	"admin_api_server/internal_breakglass_service",
	"admin_api_server/internal_debug_service",
	"admin_api_server/security_service",
	"archival",
	"archival-ctrl",
	"assert",
	"auditing",
	"client_config",
	"client_pool",
	"cloud_io",
	"cloud_roles",
	"cloud_storage",
	"cloud_topics",
	"cloud_topics_compaction",
	"cluster",
	"compaction_ctrl",
	"compression",
	"config",
	"connectrpc",
	"controller_rate_limiter_log",
	"cpu_profiler",
	"crash-reporter",
	"crash_tracker",
	"data-migrate",
	"datalake",
	"debug-bundle-service",
	"dl_backlog_controller",
	"dns_resolver",
	"exception",
	"fault_injector",
	"features",
	"finject",
	"http",
	"httpd",
	"iceberg",
	"io",
	"json",
	"kafka",
	"kafka-cg",
	"kafka/authz",
	"kafka/client",
	"kafka/data/rpc",
	"kafka_data",
	"kafka_quotas",
	"kvstore",
	"level_zero_gc_service",
	"lsm",
	"main",
	"metrics-reporter",
	"net_tls",
	"offset_translator",
	"ossl-library-context-service",
	"pandaproxy",
	"pandaproxy/requests",
	"r/heartbeat",
	"raft",
	"reconciler",
	"request_auth",
	"resource_mgmt",
	"resources",
	"rpc",
	"s3",
	"schemaregistry",
	"schemaregistry/requests",
	"scollectd",
	"seastar",
	"seastar-tls",
	"seastar_memory",
	"security",
	"serde",
	"shadow_link",
	"shadow_link_internal_service",
	"shadow_link_service",
	"storage",
	"storage-gc",
	"storage-resources",
	"syschecks",
	"transform",
	"transform/logging",
	"transform/rpc",
	"transform/stm",
	"tx",
	"tx-migration",
	"wasm",
}
