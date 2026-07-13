// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package bundle

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"strings"
	"time"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/debug/debugbundle"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

const bundleHelpText = `Collect a read-only diagnostic bundle from a Redpanda SQL cluster.

The bundle is gathered over the Redpanda SQL admin API. It seeds from the first
--admin-hosts entry (defaulting to localhost:9090),
discovers the rest of the cluster via GetClusterNodes, and fans out per-node
collection (config, logs, host probes, resource usage, a Prometheus /metrics time
series) plus cluster-wide artifacts (topology, catalog head). Results are written
as a ZIP under an sql/ subtree with a manifest.json and a per-RPC errors.txt
roll-up; one failing RPC or unreachable node does not fail the bundle.

Run it from inside a Redpanda SQL pod (it defaults to localhost:9090 and discovers
the rest), or against a remote cluster by passing seed --admin-hosts.`

type tlsFlags struct {
	enabled    bool
	skipVerify bool
	ca         string
	cert       string
	key        string
}

type authFlags struct {
	user     string
	password string
	token    string
}

type bundleFlags struct {
	adminHosts  []string
	output      string
	uploadURL   string
	tls         tlsFlags
	auth        authFlags
	sqlText     string
	vmstat      bool
	cpuSeconds  uint
	logSince    time.Duration
	logSizeLim  uint64
	metricsPort uint16
	timeout     time.Duration
}

func NewCommand(fs afero.Fs, _ *config.Params) *cobra.Command {
	var cfg bundleFlags
	cmd := &cobra.Command{
		Use:   "bundle",
		Short: "Collect a diagnostic bundle from a Redpanda SQL cluster",
		Long:  bundleHelpText,
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, _ []string) {
			opts, err := cfg.options(fs)
			out.MaybeDieErr(err)

			// The local artifact is always written first; upload is a separate
			// step, so a failed upload still leaves the bundle on disk.
			f, err := fs.Create(cfg.output)
			out.MaybeDie(err, "unable to create %q: %v", cfg.output, err)

			runErr := writeBundle(cmd.Context(), f, opts)
			closeErr := f.Close()
			out.MaybeDie(runErr, "unable to collect debug bundle: %v", runErr)
			out.MaybeDie(closeErr, "unable to finalize %q: %v", cfg.output, closeErr)
			fmt.Printf("Wrote debug bundle to %q\n", cfg.output)

			if cfg.uploadURL != "" {
				err = debugbundle.UploadBundle(cmd.Context(), cfg.output, cfg.uploadURL)
				out.MaybeDie(err, "unable to upload bundle: %v", err)
				fmt.Println("Successfully uploaded the bundle")
			}
		},
	}
	cfg.install(cmd.Flags())

	cmd.MarkFlagsRequiredTogether("tls-cert", "tls-key")
	cmd.MarkFlagsRequiredTogether("user", "password")
	cmd.MarkFlagsMutuallyExclusive("user", "token")
	cmd.MarkFlagsMutuallyExclusive("password", "token")
	return cmd
}

// install registers every bundle flag against the flag set.
func (c *bundleFlags) install(f *pflag.FlagSet) {
	f.StringSliceVar(&c.adminHosts, "admin-hosts", []string{"localhost:9090"}, "Comma-separated seed admin endpoints host:port; the rest of the cluster is discovered via GetClusterNodes")
	f.StringVarP(&c.output, "output", "o", "redpanda-sql-debug-bundle.zip", "Output ZIP path")
	f.StringVar(&c.uploadURL, "upload-url", "", "If provided, where to upload the bundle in addition to creating a copy on disk")
	f.BoolVar(&c.tls.enabled, "tls", false, "Use HTTPS for admin endpoints")
	f.BoolVar(&c.tls.skipVerify, "tls-insecure-skip-verify", false, "Skip TLS certificate verification")
	f.StringVar(&c.tls.ca, "tls-ca", "", "PEM CA bundle for server verification")
	f.StringVar(&c.tls.cert, "tls-cert", "", "Client certificate for mTLS")
	f.StringVar(&c.tls.key, "tls-key", "", "Client key for mTLS")
	f.StringVar(&c.auth.user, "user", "", "HTTP Basic username")
	f.StringVar(&c.auth.password, "password", "", "HTTP Basic password")
	f.StringVar(&c.auth.token, "token", "", "Bearer token (mutually exclusive with --user/--password)")
	f.StringVar(&c.sqlText, "include-sql-text", "masked", "SQL text in query artifacts: masked|raw")
	f.BoolVar(&c.vmstat, "include-vmstat", false, "Include vmstat in host probes (~1s slower)")
	f.UintVar(&c.cpuSeconds, "cpu-profile-seconds", 0, "Collect a CPU profile of this duration per node (0 = skip)")
	f.DurationVar(&c.logSince, "log-since", 0, "Collect log lines newer than this (0 = server default window)")
	f.Uint64Var(&c.logSizeLim, "log-size-limit", 0, "Max log bytes per node (0 = server default)")
	f.Uint16Var(&c.metricsPort, "metrics-port", 8080, "Per-node Prometheus metrics port (scraped twice ~1s apart)")
	f.DurationVar(&c.timeout, "timeout", 60*time.Second, "Per-RPC timeout")
}

// options validates the flags and assembles the collector Options.
func (c *bundleFlags) options(fs afero.Fs) (Options, error) {
	sqlMode, err := sqlTextMode(c.sqlText)
	if err != nil {
		return Options{}, err
	}

	var tlsCfg *tls.Config
	if c.tls.enabled {
		tlsCfg, err = c.tls.config(fs)
		if err != nil {
			return Options{}, fmt.Errorf("unable to build TLS config: %w", err)
		}
	}

	var logSinceMs int64
	if c.logSince > 0 {
		logSinceMs = time.Now().Add(-c.logSince).UnixMilli()
	}

	return Options{
		Seeds:             c.adminHosts,
		UseTLS:            c.tls.enabled,
		TLSConfig:         tlsCfg,
		Auth:              Auth{Bearer: c.auth.token, User: c.auth.user, Pass: c.auth.password},
		Timeout:           c.timeout,
		SQLTextMode:       sqlMode,
		IncludeVmstat:     c.vmstat,
		CPUProfileSeconds: uint32(c.cpuSeconds),
		LogSinceUnixMs:    logSinceMs,
		LogSizeLimitBytes: c.logSizeLim,
		MetricsPort:       c.metricsPort,
		ToolVersion:       "rpk",
	}, nil
}

func sqlTextMode(s string) (string, error) {
	switch strings.ToLower(s) {
	case "masked":
		return "SQL_TEXT_MODE_MASKED", nil
	case "raw":
		return "SQL_TEXT_MODE_RAW", nil
	default:
		return "", fmt.Errorf("--include-sql-text must be masked|raw, got %q", s)
	}
}

// config builds a *tls.Config from the flags. --tls-cert and --tls-key are
// enforced together by the command's MarkFlagsRequiredTogether.
func (t tlsFlags) config(fs afero.Fs) (*tls.Config, error) {
	cfg := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: t.skipVerify,
	}
	if t.ca != "" {
		pem, err := afero.ReadFile(fs, t.ca)
		if err != nil {
			return nil, fmt.Errorf("unable to read --tls-ca: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("--tls-ca: no certificates found in %s", t.ca)
		}
		cfg.RootCAs = pool
	}
	if t.cert != "" {
		certPEM, err := afero.ReadFile(fs, t.cert)
		if err != nil {
			return nil, fmt.Errorf("unable to read --tls-cert: %w", err)
		}
		keyPEM, err := afero.ReadFile(fs, t.key)
		if err != nil {
			return nil, fmt.Errorf("unable to read --tls-key: %w", err)
		}
		cert, err := tls.X509KeyPair(certPEM, keyPEM)
		if err != nil {
			return nil, fmt.Errorf("unable to load client cert/key: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}
