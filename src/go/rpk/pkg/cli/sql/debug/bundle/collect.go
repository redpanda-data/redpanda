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
	"archive/zip"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/debug/debugbundle"
)

// bundleRoot is the top-level subtree for all Redpanda SQL artifacts, matching the
// design's `sql/` layout so the output can be spliced under an rpk bundle.
const bundleRoot = "sql"

// maxResponseBytes caps the metrics scrape response to guard against a hostile
// or wedged server. Prometheus payloads are large but bounded.
const maxResponseBytes = 1 << 30 // 1 GiB

// Options configures a collection run.
type Options struct {
	Seeds             []string
	UseTLS            bool
	TLSConfig         *tls.Config
	Auth              Auth
	Timeout           time.Duration
	SQLTextMode       string // sqlTextModeMasked | sqlTextModeRaw
	IncludeVmstat     bool
	CPUProfileSeconds uint32
	LogSinceUnixMs    int64
	LogSizeLimitBytes uint64
	MetricsPort       uint16
	ToolVersion       string
}

// collectionResult is one per-RPC, per-node outcome recorded in the manifest.
type collectionResult struct {
	Node      string `json:"node"`
	RPC       string `json:"rpc"`
	Status    string `json:"status"` // "ok" | "error"
	ElapsedMs int64  `json:"elapsed_ms"`
	Error     string `json:"error,omitempty"`
}

type manifest struct {
	BundleCreatedAt     string             `json:"bundle_created_at"`
	ToolVersion         string             `json:"tool_version"`
	RedpandaSQLVersions map[string]string  `json:"redpanda_sql_versions"`
	NodesAttempted      int                `json:"nodes_attempted"`
	NodesSucceeded      int                `json:"nodes_succeeded"`
	CollectionResults   []collectionResult `json:"collection_results"`
	RedactionModes      map[string]string  `json:"redaction_modes"`
}

// Run collects a debug bundle and streams it as a ZIP into out.
func writeBundle(ctx context.Context, out io.Writer, opts Options) error {
	b := &bundle{
		zw:   zip.NewWriter(out),
		opts: opts,
		hc: &http.Client{
			Timeout:   opts.Timeout,
			Transport: &http.Transport{TLSClientConfig: opts.TLSConfig},
		},
		versions: map[string]string{},
	}
	b.collect(ctx)
	b.writeManifest()
	b.writeErrors()
	return b.zw.Close()
}

type bundle struct {
	zw       *zip.Writer
	opts     Options
	hc       *http.Client
	results  []collectionResult
	errs     []string
	versions map[string]string // endpoint -> version
}

func (b *bundle) scheme() string {
	if b.opts.UseTLS {
		return "https"
	}
	return "http"
}

func (b *bundle) client(endpoint string) *Client {
	return NewClient(b.scheme()+"://"+endpoint, b.hc, b.opts.Auth)
}

func (b *bundle) collect(ctx context.Context) {
	endpoints, clusterCl := b.discover(ctx)
	b.clusterCalls(ctx, clusterCl)
	for _, ep := range endpoints {
		b.nodeCalls(ctx, ep)
	}
}

// discover resolves the node list from the first seed that answers
// GetClusterNodes, writing cluster_nodes.json as a side effect. It falls back to
// treating the seeds themselves as the node list so a single wedged node (or one
// that lacks GetClusterNodes) still yields a bundle. Returns the endpoints to fan
// out to and the client to use for cluster-wide calls.
func (b *bundle) discover(ctx context.Context) ([]string, *Client) {
	for _, seed := range b.opts.Seeds {
		cl := b.client(seed)
		start := time.Now()
		raw, err := cl.CallRaw(ctx, "GetClusterNodes", emptyRequest)
		b.record(seed, "GetClusterNodes", start, err)
		if err != nil {
			continue
		}
		b.addJSON(bundleRoot+"/cluster/cluster_nodes.json", raw)

		var resp getClusterNodesResponse
		if json.Unmarshal(raw, &resp) == nil && len(resp.Nodes) > 0 {
			endpoints := make([]string, 0, len(resp.Nodes))
			for _, n := range resp.Nodes {
				if n.AdminEndpoint != "" {
					endpoints = append(endpoints, n.AdminEndpoint)
				}
			}
			if len(endpoints) > 0 {
				return endpoints, cl
			}
		}
		return b.opts.Seeds, cl
	}
	// No seed answered discovery; fan out to the seeds directly.
	return b.opts.Seeds, b.client(b.opts.Seeds[0])
}

// clusterCalls collects the cluster-wide artifacts once, from a single node.
func (b *bundle) clusterCalls(ctx context.Context, cl *Client) {
	const node = "cluster"
	dir := bundleRoot + "/cluster/"

	b.grabJSON(ctx, cl, node, "GetOxlaHomeListing", emptyRequest, dir+"redpanda_sql_home_listing.json")
	b.grabJSON(ctx, cl, node, "GetRecentQueries",
		getRecentQueriesRequest{IncludeSQLText: b.opts.SQLTextMode}, dir+"recent_queries.json")

	if head := (getCatalogHeadResponse{}); b.call(ctx, cl, node, "GetCatalogHead", emptyRequest, &head) {
		b.add(dir+"catalog_head.pb", head.CatalogHead)
		if head.CatalogHeadJSON != "" {
			b.addJSON(dir+"catalog_head.json", json.RawMessage(head.CatalogHeadJSON))
		}
	}
}

// nodeCalls collects the per-node artifacts for one admin endpoint.
func (b *bundle) nodeCalls(ctx context.Context, endpoint string) {
	cl := b.client(endpoint)
	dir := fmt.Sprintf("%s/nodes/%s/", bundleRoot, debugbundle.SanitizeName(endpoint))

	if raw, ok := b.grabJSON(ctx, cl, endpoint, "GetVersion", emptyRequest, dir+"version.json"); ok {
		var v getVersionResponse
		if json.Unmarshal(raw, &v) == nil {
			b.versions[endpoint] = v.Version
		}
	}
	// config.yaml is per-node (env overrides, host_name, ports differ), so collect
	// it on every node rather than once cluster-wide.
	if cfg := (getConfigResponse{}); b.call(ctx, cl, endpoint, "GetConfig", emptyRequest, &cfg) {
		b.add(dir+"config.yaml", []byte(cfg.YAML))
	}
	b.grabJSON(ctx, cl, endpoint, "GetActiveQueries",
		getActiveQueriesRequest{IncludeSQLText: b.opts.SQLTextMode}, dir+"active_queries.json")

	b.resourceUsage(ctx, cl, endpoint, dir)

	if lt := (getLogTailResponse{}); b.call(ctx, cl, endpoint, "GetLogTail",
		getLogTailRequest{SinceUnixMs: b.opts.LogSinceUnixMs, SizeLimitBytes: b.opts.LogSizeLimitBytes}, &lt) {
		b.add(dir+"tail.log", lt.Content)
	}
	b.startupLog(ctx, cl, endpoint, dir)
	b.crashReports(ctx, cl, endpoint, dir)
	b.hostProbes(ctx, cl, endpoint, dir)

	if b.opts.CPUProfileSeconds > 0 {
		if cp := (getCPUProfileResponse{}); b.call(ctx, cl, endpoint, "GetCpuProfile",
			getCPUProfileRequest{DurationSeconds: b.opts.CPUProfileSeconds}, &cp) {
			b.add(dir+"cpu_profile.pprof.gz", cp.PprofGzip)
		}
	}
	b.scrapeMetrics(ctx, endpoint, dir)
}

// scrapeMetrics pulls the node's Prometheus /metrics endpoint twice ~1s apart so
// the bundle carries a short time series for rate/delta analysis. Metrics are
// served by a separate plain-HTTP server (config `metrics.port`), not the admin
// API, so this uses http:// on that port and neither TLS nor auth.
func (b *bundle) scrapeMetrics(ctx context.Context, node, dir string) {
	host := node
	if i := strings.LastIndex(node, ":"); i >= 0 {
		host = node[:i]
	}
	url := fmt.Sprintf("http://%s:%d/metrics", host, b.opts.MetricsPort)

	scrape := func(suffix string) {
		start := time.Now()
		err := b.httpGetToFile(ctx, url, dir+"metrics_"+suffix+".txt")
		b.record(node, "GET /metrics", start, err)
	}
	scrape("t0")
	time.Sleep(time.Second)
	scrape("t1")
}

// httpGetToFile GETs url and writes the body to name in the bundle. Non-200 is an
// error and nothing is written.
func (b *bundle) httpGetToFile(ctx context.Context, url, name string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := b.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s: HTTP %d", url, resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return fmt.Errorf("GET %s: read body: %w", url, err)
	}
	b.add(name, body)
	return nil
}

func (b *bundle) resourceUsage(ctx context.Context, cl *Client, node, dir string) {
	sample := func() (json.RawMessage, bool) {
		start := time.Now()
		raw, err := cl.CallRaw(ctx, "GetResourceUsage", emptyRequest)
		b.record(node, "GetResourceUsage", start, err)
		return raw, err == nil
	}
	first, ok := sample()
	if !ok {
		return
	}
	time.Sleep(time.Second)
	second, ok := sample()
	if !ok {
		b.addJSON(dir+"resource-usage.json", first)
		return
	}

	report := map[string]any{"samples": []json.RawMessage{first, second}}
	if pct, ok := cpuPercentage(first, second); ok {
		report["cpuPercentage"] = pct
	}
	out, _ := json.MarshalIndent(report, "", "  ")
	b.add(dir+"resource-usage.json", out)
}

func (b *bundle) startupLog(ctx context.Context, cl *Client, node, dir string) {
	var resp getStartupLogResponse
	if !b.call(ctx, cl, node, "GetStartupLog", emptyRequest, &resp) {
		return
	}
	var sb strings.Builder
	for _, f := range resp.Files { // newest-first
		fmt.Fprintf(&sb, "===== %s =====\n", f.Filename)
		sb.Write(f.Content)
		sb.WriteByte('\n')
	}
	b.add(dir+"startup.log", []byte(sb.String()))
}

func (b *bundle) crashReports(ctx context.Context, cl *Client, node, dir string) {
	var resp getCrashReportsResponse
	if !b.call(ctx, cl, node, "GetCrashReports", emptyRequest, &resp) {
		return
	}
	for i, r := range resp.Reports {
		name := r.Filename
		if name == "" {
			name = fmt.Sprintf("%s_%s.txt", nonempty(r.TimestampUnixMs, strconv.Itoa(i)), r.PID)
		}
		b.add(dir+"crash_reports/"+debugbundle.SanitizeName(name), []byte(r.Trace))
	}
}

func (b *bundle) hostProbes(ctx context.Context, cl *Client, node, dir string) {
	var resp getHostProbesResponse
	if !b.call(ctx, cl, node, "GetHostProbes", getHostProbesRequest{IncludeVmstat: b.opts.IncludeVmstat}, &resp) {
		return
	}
	files := map[string]string{
		"proc/cpuinfo":           resp.ProcCPUInfo,
		"proc/meminfo":           resp.ProcMemInfo,
		"proc/diskstats":         resp.ProcDiskstats,
		"proc/loadavg":           resp.ProcLoadavg,
		"proc/version":           resp.ProcVersion,
		"proc/mounts":            resp.ProcMounts,
		"proc/net/sockstat":      resp.ProcNetSockstat,
		"linux-utils/uname.txt":  resp.Uname,
		"linux-utils/df.txt":     resp.DF,
		"linux-utils/free.txt":   resp.Free,
		"linux-utils/vmstat.txt": resp.Vmstat,
		"linux-utils/top.txt":    resp.Top,
		"linux-utils/uptime.txt": resp.Uptime,
		"linux-utils/sysctl.txt": resp.Sysctl,
		"linux-utils/resolv.txt": resp.ResolvConf,
	}
	for rel, content := range files {
		if content != "" {
			b.add(dir+rel, []byte(content))
		}
	}
	if resp.ContainerImageTag != "" || resp.ContainerImageDigst != "" {
		b.add(dir+"linux-utils/container.txt",
			[]byte(fmt.Sprintf("tag: %s\ndigest: %s\n", resp.ContainerImageTag, resp.ContainerImageDigst)))
	}
}

// --- helpers ----------------------------------------------------------------

// grabJSON calls a method and writes its response verbatim (pretty-printed) to
// filename. Returns the raw response and whether the call succeeded.
func (b *bundle) grabJSON(ctx context.Context, cl *Client, node, method string, req any, filename string) (json.RawMessage, bool) {
	start := time.Now()
	raw, err := cl.CallRaw(ctx, method, req)
	b.record(node, method, start, err)
	if err != nil {
		return nil, false
	}
	b.addJSON(filename, raw)
	return raw, true
}

// call invokes a method, records the outcome, and decodes into out. Returns
// whether the call succeeded.
func (b *bundle) call(ctx context.Context, cl *Client, node, method string, req, out any) bool {
	start := time.Now()
	err := cl.Call(ctx, method, req, out)
	b.record(node, method, start, err)
	return err == nil
}

func (b *bundle) record(node, rpc string, start time.Time, err error) {
	res := collectionResult{Node: node, RPC: rpc, Status: "ok", ElapsedMs: time.Since(start).Milliseconds()}
	if err != nil {
		res.Status = "error"
		res.Error = err.Error()
		b.errs = append(b.errs, fmt.Sprintf("[%s] %s: %v", node, rpc, err))
	}
	b.results = append(b.results, res)
}

func (b *bundle) add(name string, data []byte) {
	f, err := b.zw.Create(name)
	if err != nil {
		b.errs = append(b.errs, fmt.Sprintf("zip create %s: %v", name, err))
		return
	}
	if _, err := f.Write(data); err != nil {
		b.errs = append(b.errs, fmt.Sprintf("zip write %s: %v", name, err))
	}
}

// addJSON re-indents a raw JSON response for readability before writing.
func (b *bundle) addJSON(name string, raw json.RawMessage) {
	pretty, err := jsonIndent(raw)
	if err != nil {
		b.add(name, raw)
		return
	}
	b.add(name, pretty)
}

func (b *bundle) writeManifest() {
	m := manifest{
		BundleCreatedAt:     time.Now().UTC().Format(time.RFC3339),
		ToolVersion:         b.opts.ToolVersion,
		RedpandaSQLVersions: b.versions,
		NodesAttempted:      len(b.versions),
		NodesSucceeded:      len(b.versions),
		CollectionResults:   b.results,
		RedactionModes:      map[string]string{"sql_text": b.opts.SQLTextMode},
	}
	out, _ := json.MarshalIndent(m, "", "  ")
	b.add(bundleRoot+"/manifest.json", out)
}

func (b *bundle) writeErrors() {
	if len(b.errs) == 0 {
		return
	}
	b.add(bundleRoot+"/errors.txt", []byte(strings.Join(b.errs, "\n")+"\n"))
}

func jsonIndent(raw json.RawMessage) ([]byte, error) {
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return nil, err
	}
	return json.MarshalIndent(v, "", "  ")
}

// cpuPercentage derives CPU utilization from two resource-usage samples:
// busy CPU-seconds over the wall-clock interval between them.
func cpuPercentage(firstRaw, secondRaw json.RawMessage) (float64, bool) {
	var a, c resourceUsageSample
	if json.Unmarshal(firstRaw, &a) != nil || json.Unmarshal(secondRaw, &c) != nil {
		return 0, false
	}
	ticksPerSec := atoi(c.ClockTicksPerSec)
	wallMs := atoi(c.SampledAtUnixMs) - atoi(a.SampledAtUnixMs)
	if ticksPerSec <= 0 || wallMs <= 0 {
		return 0, false
	}
	busyTicks := (atoi(c.CPUUserTicks) + atoi(c.CPUKernelTicks)) - (atoi(a.CPUUserTicks) + atoi(a.CPUKernelTicks))
	busySec := float64(busyTicks) / float64(ticksPerSec)
	wallSec := float64(wallMs) / 1000.0
	return busySec / wallSec * 100.0, true
}

func atoi(s string) int64 {
	n, _ := strconv.ParseInt(s, 10, 64)
	return n
}

func nonempty(s, fallback string) string {
	if s != "" {
		return s
	}
	return fallback
}
