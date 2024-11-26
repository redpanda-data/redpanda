package common

import (
	"strings"
	"time"

	"github.com/spf13/pflag"
)

// DebugBundleSharedOptions are the options of a debug bundle that can be
// shared between a normal and a remote bundle.
type DebugBundleSharedOptions struct {
	CPUProfilerWait         time.Duration
	LogsSince               string
	LogsUntil               string
	MetricsInterval         time.Duration
	MetricsSampleCount      int
	PartitionFlag           []string
	Namespace               string
	LabelSelector           []string
	LogsSizeLimit           string
	ControllerLogsSizeLimit string
}

// InstallFlags installs the debug bundle flags that fills the debug bundle
// options.
func (o *DebugBundleSharedOptions) InstallFlags(f *pflag.FlagSet) {
	f.StringVar(&o.ControllerLogsSizeLimit, "controller-logs-size-limit", "132MB", "The size limit of the controller logs that can be stored in the bundle. For example: 3MB, 1GiB")
	f.DurationVar(&o.CPUProfilerWait, "cpu-profiler-wait", 30*time.Second, "How long to collect samples for the CPU profiler. For example: 30s, 1.5m. Must be higher than 15s")
	f.StringVar(&o.LogsSizeLimit, "logs-size-limit", "100MiB", "Read the logs until the given size is reached. For example: 3MB, 1GiB")
	f.StringVar(&o.LogsSince, "logs-since", "yesterday", "Include logs dated from specified date onward; (journalctl date format: YYYY-MM-DD, 'yesterday', or 'today'). See the journalctl documentation for more options")
	f.StringVar(&o.LogsUntil, "logs-until", "", "Include logs older than the specified date; (journalctl date format: YYYY-MM-DD, 'yesterday', or 'today'). See the journalctl documentation for more options")
	f.DurationVar(&o.MetricsInterval, "metrics-interval", 10*time.Second, "Interval between metrics snapshots. For example: 30s, 1.5m")
	f.IntVar(&o.MetricsSampleCount, "metrics-samples", 2, "Number of metrics samples to take (at the interval of --metrics-interval). Must be >= 2")
	f.StringArrayVarP(&o.PartitionFlag, "partition", "p", nil, "Comma-separated partition IDs. When provided, rpk saves extra Admin API requests for those partitions. See the help for extended usage")
	f.StringVarP(&o.Namespace, "namespace", "n", "redpanda", "The namespace to use to collect the resources from (K8s only)")
	f.StringArrayVarP(&o.LabelSelector, "label-selector", "l", []string{"app.kubernetes.io/name=redpanda"}, "Comma-separated label selectors to filter your resources. For example: <label>=<value>,<label>=<value> (K8s only)")
}

// SanitizeName replace any of the following characters with "-": "<", ">", ":",
// `"`, "/", "|", "?", "*". This is to avoid having forbidden names in Windows
// environments.
func SanitizeName(name string) string {
	forbidden := []string{"<", ">", ":", `"`, "/", `\`, "|", "?", "*"}
	r := name
	for _, s := range forbidden {
		r = strings.Replace(r, s, "-", -1)
	}
	return r
}
