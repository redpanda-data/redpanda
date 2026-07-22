// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package bundle

// Only the fields the collector acts on are modeled here; every other response is
// passed through to its output file verbatim. Wire encoding follows the proto3
// JSON mapping: field names are lowerCamelCase, 64-bit integers are quoted
// strings, `bytes` are base64 (std) strings, enums are their symbolic names.

// --- requests ---------------------------------------------------------------

type getRecentQueriesRequest struct {
	IncludeSQLText string `json:"includeSqlText,omitempty"`
}

type getActiveQueriesRequest struct {
	IncludeSQLText string `json:"includeSqlText,omitempty"`
}

type getLogTailRequest struct {
	SinceUnixMs    int64  `json:"sinceUnixMs,omitempty"`
	SizeLimitBytes uint64 `json:"sizeLimitBytes,omitempty"`
}

type getHostProbesRequest struct {
	IncludeVmstat bool `json:"includeVmstat,omitempty"`
}

type getCPUProfileRequest struct {
	DurationSeconds uint32 `json:"durationSeconds,omitempty"`
}

// --- responses --------------------------------------------------------------

type getVersionResponse struct {
	Version     string `json:"version"`
	CommitSHA   string `json:"commitSha"`
	ImageTag    string `json:"imageTag"`
	ImageDigest string `json:"imageDigest"`
	Hostname    string `json:"hostname"`
	NodeID      string `json:"nodeId"`
}

type getConfigResponse struct {
	YAML string `json:"yaml"`
}

type clusterNode struct {
	NodeID           string `json:"nodeId"`
	AdminEndpoint    string `json:"adminEndpoint"`
	Role             string `json:"role"`
	MembershipStatus string `json:"membershipStatus"`
}

type getClusterNodesResponse struct {
	Nodes []clusterNode `json:"nodes"`
}

type getCatalogHeadResponse struct {
	CatalogHead     []byte `json:"catalogHead"`     // base64 in, raw bytes after decode
	CatalogHeadJSON string `json:"catalogHeadJson"` // proto decoded to JSON server-side; empty if unparseable
}

// resourceUsageSample keeps the raw counters as strings (proto3 JSON int64/uint64
// encoding) and parses them only where arithmetic is needed.
type resourceUsageSample struct {
	CPUUserTicks     string `json:"cpuUserTicks"`
	CPUKernelTicks   string `json:"cpuKernelTicks"`
	ClockTicksPerSec string `json:"clockTicksPerSec"`
	SampledAtUnixMs  string `json:"sampledAtUnixMs"`
	FreeMemoryBytes  string `json:"freeMemoryBytes"`
	TotalMemoryBytes string `json:"totalMemoryBytes"`
	FreeDiskBytes    string `json:"freeDiskBytes"`
	TotalDiskBytes   string `json:"totalDiskBytes"`
}

type getLogTailResponse struct {
	Content   []byte `json:"content"` // base64 in, raw bytes after decode
	Truncated bool   `json:"truncated"`
}

type crashReport struct {
	TimestampUnixMs string `json:"timestampUnixMs"`
	NodeID          string `json:"nodeId"`
	PID             string `json:"pid"`
	Filename        string `json:"filename"`
	Trace           string `json:"trace"`
}

type getCrashReportsResponse struct {
	Reports []crashReport `json:"reports"`
}

type startupLogFile struct {
	Filename    string `json:"filename"`
	MtimeUnixMs string `json:"mtimeUnixMs"`
	Content     []byte `json:"content"` // base64 in, raw bytes after decode
}

type getStartupLogResponse struct {
	Files []startupLogFile `json:"files"` // newest-first
}

type getHostProbesResponse struct {
	ProcCPUInfo         string `json:"procCpuinfo"`
	ProcMemInfo         string `json:"procMeminfo"`
	ProcDiskstats       string `json:"procDiskstats"`
	ProcLoadavg         string `json:"procLoadavg"`
	ProcVersion         string `json:"procVersion"`
	ProcMounts          string `json:"procMounts"`
	ProcNetSockstat     string `json:"procNetSockstat"`
	Uname               string `json:"uname"`
	DF                  string `json:"df"`
	Free                string `json:"free"`
	Vmstat              string `json:"vmstat"`
	Top                 string `json:"top"`
	Uptime              string `json:"uptime"`
	Sysctl              string `json:"sysctl"`
	ContainerImageTag   string `json:"containerImageTag"`
	ContainerImageDigst string `json:"containerImageDigest"`
	ResolvConf          string `json:"resolvConf"`
}

type getCPUProfileResponse struct {
	PprofGzip []byte `json:"pprofGzip"` // base64 in, raw gzip bytes after decode
}
