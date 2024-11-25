// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package bundle

import (
	"context"
	"encoding/xml"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/docker/go-units"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/httpapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/kafka"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kgo"
	"k8s.io/apimachinery/pkg/labels"
)

type bundleParams struct {
	fs                      afero.Fs
	p                       *config.RpkProfile
	y                       *config.RedpandaYaml
	yActual                 *config.RedpandaYaml
	cl                      *kgo.Client
	logsSince               string
	logsUntil               string
	path                    string
	namespace               string
	logsLimitBytes          int
	controllerLogLimitBytes int
	timeout                 time.Duration
	metricsInterval         time.Duration
	partitions              []topicPartitionFilter
	labelSelector           map[string]string
}

type topicPartitionFilter struct {
	namespace    string
	topic        string
	partitionsID []int
}

func NewCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	const outputFlag = "output"
	var (
		outFile   string
		uploadURL string

		logsSince     string
		logsUntil     string
		logsSizeLimit string

		controllerLogsSizeLimit string
		namespace               string
		labelSelector           []string
		partitionFlag           []string

		timeout         time.Duration
		metricsInterval time.Duration
	)
	cmd := &cobra.Command{
		Use:   "bundle",
		Short: "Collect environment data and create a bundle file for the Redpanda Data support team to inspect",
		Long:  bundleHelpText,
		Run: func(cmd *cobra.Command, _ []string) {
			path, err := determineFilepath(fs, outFile, cmd.Flags().Changed(outputFlag))
			out.MaybeDie(err, "unable to determine filepath %q: %v", outFile, err)

			cfg, err := p.Load(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)

			// We do not config.CheckExitCloudAdmin here, because
			// capturing a debug *can* have access sometimes (k8s).
			var (
				p           = cfg.VirtualProfile()
				y           = cfg.VirtualRedpandaYaml()
				yActual, ok = cfg.ActualRedpandaYaml()
			)
			if !ok {
				yActual = y
			}

			partitions, err := parsePartitionFlag(partitionFlag)
			out.MaybeDie(err, "unable to parse partition flag %v: %v", partitionFlag, err)

			cl, err := kafka.NewFranzClient(fs, p)
			out.MaybeDie(err, "unable to initialize kafka client: %v", err)
			defer cl.Close()

			logsLimit, err := units.FromHumanSize(logsSizeLimit)
			out.MaybeDie(err, "unable to parse --logs-size-limit: %v", err)

			controllerLogsLimit, err := units.FromHumanSize(controllerLogsSizeLimit)
			out.MaybeDie(err, "unable to parse --controller-logs-size-limit: %v", err)
			bp := bundleParams{
				fs:                      fs,
				p:                       p,
				y:                       y,
				yActual:                 yActual,
				cl:                      cl,
				logsSince:               logsSince,
				logsUntil:               logsUntil,
				path:                    path,
				namespace:               namespace,
				logsLimitBytes:          int(logsLimit),
				controllerLogLimitBytes: int(controllerLogsLimit),
				timeout:                 timeout,
				metricsInterval:         metricsInterval,
				partitions:              partitions,
			}
			if len(labelSelector) > 0 {
				labelsMap, err := labels.ConvertSelectorToLabelsMap(strings.Join(labelSelector, ","))
				out.MaybeDie(err, "unable to parse label-selector flag: %v", err)
				bp.labelSelector = labelsMap
			}
			// To execute the appropriate bundle we look for
			// kubernetes_service_* env variables to identify if we are in a
			// k8s environment.
			host, port := os.Getenv("KUBERNETES_SERVICE_HOST"), os.Getenv("KUBERNETES_SERVICE_PORT")
			if len(host) == 0 || len(port) == 0 {
				err = executeBundle(cmd.Context(), bp)
			} else {
				err = executeK8SBundle(cmd.Context(), bp)
			}
			out.MaybeDie(err, "unable to create bundle: %v", err)
			if uploadURL != "" {
				err = uploadBundle(cmd.Context(), path, uploadURL)
				out.MaybeDie(err, "unable to upload bundle: %v", err)
				fmt.Println("Successfully uploaded the bundle")
			}
		},
	}

	p.InstallKafkaFlags(cmd)
	p.InstallAdminFlags(cmd)

	f := cmd.Flags()
	f.StringVarP(&outFile, outputFlag, "o", "", "The file path where the debug file will be written (default ./<timestamp>-bundle.zip)")
	f.DurationVar(&timeout, "timeout", 12*time.Second, "How long to wait for child commands to execute (e.g. 30s, 1.5m)")
	f.DurationVar(&metricsInterval, "metrics-interval", 10*time.Second, "Interval between metrics snapshots (e.g. 30s, 1.5m)")
	f.StringVar(&logsSince, "logs-since", "", "Include log entries on or newer than the specified date (journalctl date format, e.g. YYYY-MM-DD")
	f.StringVar(&logsUntil, "logs-until", "", "Include log entries on or older than the specified date (journalctl date format, e.g. YYYY-MM-DD")
	f.StringVar(&logsSizeLimit, "logs-size-limit", "100MiB", "Read the logs until the given size is reached (e.g. 3MB, 1GiB)")
	f.StringVar(&controllerLogsSizeLimit, "controller-logs-size-limit", "20MB", "The size limit of the controller logs that can be stored in the bundle (e.g. 3MB, 1GiB)")
	f.StringVar(&uploadURL, "upload-url", "", "If provided, where to upload the bundle in addition to creating a copy on disk")
	f.StringVarP(&namespace, "namespace", "n", "redpanda", "The namespace to use to collect the resources from (k8s only)")
	f.StringArrayVarP(&labelSelector, "label-selector", "l", []string{"app.kubernetes.io/name=redpanda"}, "Comma-separated label selectors to filter your resources. e.g: <label>=<value>,<label>=<value> (k8s only)")
	f.StringArrayVarP(&partitionFlag, "partition", "p", nil, "Comma-separated partition IDs; when provided, rpk saves extra admin API requests for those partitions. Check help for extended usage")

	return cmd
}

// uploadBundle will send the file located in 'filepath' by issuing a PUT
// request to the 'uploadURL'.
func uploadBundle(ctx context.Context, filepath, uploadURL string) error {
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

func parsePartitionFlag(flags []string) (filters []topicPartitionFilter, rerr error) {
	for _, flag := range flags {
		ns, topic, partitions, err := out.ParsePartitionString(flag)
		if err != nil {
			return nil, err
		}
		if topic == "" {
			return nil, fmt.Errorf("you must provide a topic")
		}
		filters = append(filters, topicPartitionFilter{
			namespace:    ns,
			topic:        topic,
			partitionsID: partitions,
		})
	}
	return
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

const bundleHelpText = `'rpk debug bundle' collects environment data that can help debug and diagnose
issues with a redpanda cluster, a broker, or the machine it's running on. It
then bundles the collected data into a ZIP file, called a diagnostics bundle.

The files and directories in the diagnostics bundle differ depending on the 
environment in which Redpanda is running:

COMMON FILES

 - Kafka metadata: Broker configs, topic configs, start/committed/end offsets,
   groups, group commits.

 - Controller logs: The controller logs directory up to a limit set by
   --controller-logs-size-limit flag

 - Data directory structure: A file describing the data directory's contents.

 - Redpanda configuration: The redpanda configuration file (redpanda.yaml;
   SASL credentials are stripped).

 - Runtime system information (/proc): Process information, by reading /proc 
  files like core count, cache, frequency, interrupts, mounted file systems, 
  memory usage, kernel command line.

 - Resource usage data: CPU usage percentage, free memory available for the
   redpanda process.

 - Clock drift: The ntp clock delta (using pool.ntp.org as a reference) & round
   trip time.

 - Admin API calls: Multiple requests to gather information such as: Cluster and
   broker configurations, cluster health data, balancer status, cloud storage
   status, and license key information.

 - Broker metrics: The broker's Prometheus metrics, fetched through its
   admin API (/metrics and /public_metrics).

BARE-METAL

 - Kernel: The kernel logs ring buffer (syslog) and parameters (sysctl).

 - DNS: The DNS info as reported by 'dig', using the hosts in
   /etc/resolv.conf.

 - Disk usage: The disk usage for the data directory, as output by 'du'.

 - redpanda logs: The node's redpanda logs written to journald. If --logs-since 
   or --logs-until are passed, then only the logs within the resulting time frame
   will be included.

 - Socket info: The active sockets data output by 'ss'.

 - Running process info: As reported by 'top'.

 - Virtual memory stats: As reported by 'vmstat'.

 - Network config: As reported by 'ip addr'.

 - lspci: List the PCI buses and the devices connected to them.

 - dmidecode: The DMI table contents. Only included if this command is run
   as root.

KUBERNETES

 - Kubernetes Resources: Kubernetes manifests for all resources in the given 
   Kubernetes namespace (via --namespace).

 - redpanda logs: Logs of each Pod in the the given Kubernetes namespace. If 
   --logs-since is passed, only the logs within the given timeframe are 
   included.

EXTRA REQUESTS FOR PARTITIONS

You can provide a list of partitions to save additional admin API requests
specifically for those partitions.

The partition flag accepts the format {namespace}/[topic]/[partitions...]
where the namespace is optional, if the namespace is not provided, rpk will 
assume 'kafka'. For example:

Topic 'foo', partitions 1, 2 and 3:
  --partitions foo/1,2,3

Namespace _redpanda-internal, topic 'bar', partition 2
  --partitions _redpanda-internal/bar/2

If you have an upload URL from the Redpanda support team, provide it in the 
--upload-url flag to upload your diagnostics bundle to Redpanda.
`
