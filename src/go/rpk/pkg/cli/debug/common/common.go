package common

import (
	"context"
	"encoding/xml"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/httpapi"
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
	KafkaConnectionLimit    int
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
	f.StringVarP(&o.Namespace, "namespace", "n", "", "The namespace to use to collect the resources from (K8s only)")
	f.StringArrayVarP(&o.LabelSelector, "label-selector", "l", []string{"app.kubernetes.io/name=redpanda"}, "Comma-separated label selectors to filter your resources. For example: <label>=<value>,<label>=<value> (K8s only)")
	f.IntVar(&o.KafkaConnectionLimit, "kafka-connections-limit", 256, "The maximum number of kafka connections to store in the bundle.")
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

// S3EndpointError is the error that we get when calling an S3 url.
type S3EndpointError struct {
	XMLName xml.Name `xml:"Error"`
	Code    string   `xml:"Code"`
	Message string   `xml:"Message"`

	HTTPCode int
}

func (e *S3EndpointError) Error() string {
	return fmt.Sprintf("unexpected error code %v - %v : %v", e.HTTPCode, e.Code, e.Message)
}

// uploadBundle will send the file located in 'filepath' by issuing a PUT
// request to the 'uploadURL'.
func UploadBundle(ctx context.Context, filepath, uploadURL string) error {
	uploadFile, err := os.Open(filepath)
	if err != nil {
		return fmt.Errorf("unable to open the file %q: %v", filepath, err)
	}
	defer uploadFile.Close()

	cl := httpapi.NewClient(
		httpapi.Err4xx(func(code int) error { return &S3EndpointError{HTTPCode: code} }),
		httpapi.Headers(
			"Content-Type", "application/zip",
		))

	return cl.Put(ctx, uploadURL, nil, uploadFile, nil)
}
