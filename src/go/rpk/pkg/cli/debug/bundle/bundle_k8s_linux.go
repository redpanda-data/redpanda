// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

//go:build linux

package bundle

import (
	"archive/zip"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/spf13/afero"
	"go.uber.org/zap"
	k8score "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

func executeK8SBundle(ctx context.Context, bp bundleParams) error {
	fmt.Println("Creating bundle file...")
	mode := os.FileMode(0o755)
	f, err := bp.fs.OpenFile(
		bp.path,
		os.O_CREATE|os.O_WRONLY,
		mode,
	)
	if err != nil {
		return fmt.Errorf("unable to create bundle file: %v", err)
	}
	defer f.Close()

	var grp multierror.Group

	w := zip.NewWriter(f)
	defer w.Close()

	ps := &stepParams{
		fs:       bp.fs,
		w:        w,
		timeout:  bp.timeout,
		fileRoot: strings.TrimSuffix(filepath.Base(bp.path), ".zip"),
	}
	var errs *multierror.Error

	steps := []step{
		saveCPUInfo(ps),
		saveCmdLine(ps),
		saveConfig(ps, bp.yActual),
		saveControllerLogDir(ps, bp.y, bp.controllerLogLimitBytes),
		saveDataDirStructure(ps, bp.y),
		saveDiskUsage(ctx, ps, bp.y),
		saveInterrupts(ps),
		saveK8SLogs(ctx, ps, bp.namespace, bp.logsSince, bp.logsLimitBytes, bp.labelSelector),
		saveK8SResources(ctx, ps, bp.namespace, bp.labelSelector),
		saveKafkaMetadata(ctx, ps, bp.cl),
		saveKernelSymbols(ps),
		saveMdstat(ps),
		saveMountedFilesystems(ps),
		saveNTPDrift(ps),
		saveResourceUsageData(ps, bp.y),
		saveSlabInfo(ps),
		saveUname(ctx, ps),
	}

	adminAddresses, err := adminAddressesFromK8S(ctx, bp.namespace)
	if err != nil {
		zap.L().Sugar().Debugf("unable to get admin API addresses from the k8s API: %v", err)
	}
	if len(adminAddresses) == 0 {
		adminAddresses = []string{fmt.Sprintf("127.0.0.1:%v", config.DefaultAdminPort)}
	}
	steps = append(steps, []step{
		saveClusterAdminAPICalls(ctx, ps, bp.fs, bp.p, adminAddresses, bp.partitions),
		saveSingleAdminAPICalls(ctx, ps, bp.fs, bp.p, adminAddresses, bp.metricsInterval),
	}...)

	for _, s := range steps {
		grp.Go(s)
	}

	stepErrs := grp.Wait()
	if stepErrs != nil || errs != nil {
		errs = multierror.Append(errs, stepErrs.ErrorOrNil())
		err := writeFileToZip(ps, "errors.txt", []byte(errs.Error()))
		if err != nil {
			errs = multierror.Append(errs, err)
		}
		fmt.Println(errs.Error())
	}

	fmt.Printf("Debug bundle saved to %q\n", f.Name())
	return nil
}

func k8sClientset() (*kubernetes.Clientset, error) {
	k8sCfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("unable to get kubernetes cluster configuration: %v", err)
	}

	return kubernetes.NewForConfig(k8sCfg)
}

// k8sPodList will create a clientset using the config object which uses the
// service account kubernetes gives to pods (InClusterConfig) and the list of
// pods in the given namespace.
func k8sPodList(ctx context.Context, namespace string, labelSelector map[string]string) (*kubernetes.Clientset, *k8score.PodList, error) {
	clientset, err := k8sClientset()
	if err != nil {
		return nil, nil, fmt.Errorf("unable to create kubernetes client: %v", err)
	}

	pods, err := clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: labels.Set(labelSelector).String(),
	})
	if err != nil {
		return nil, nil, fmt.Errorf("unable to get pods in the %q namespace: %v", namespace, err)
	}
	return clientset, pods, nil
}

// adminAddressesFromK8S returns the admin API host:port list by querying the
// K8S Api.
func adminAddressesFromK8S(ctx context.Context, namespace string) ([]string, error) {
	// This is intended to run only in a k8s cluster:
	cl, err := k8sClientset()
	if err != nil {
		return nil, fmt.Errorf("unable to create kubernetes client: %v", err)
	}

	var svc k8score.Service
	services, err := cl.CoreV1().Services(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("unable to list services: %v", err)
	}
	// To get the service name we use the service that have None as ClusterIP
	// this is the case in both our helm deployment and k8s operator.
	for _, s := range services.Items {
		if s.Spec.ClusterIP == k8score.ClusterIPNone {
			svc = s
			break // no need to keep looping.
		}
	}

	// And list the pods based on the service label selector.
	pods, err := cl.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: labels.SelectorFromSet(svc.Spec.Selector).String(),
	})
	if err != nil {
		return nil, fmt.Errorf("unable to list pods in the service %q: %v", svc.Name, err)
	}

	clusterDomain := getClusterDomain()
	// Get the admin addresses from ContainerPort.
	var adminAddresses []string
	for _, p := range pods.Items {
		for _, port := range p.Spec.Containers[0].Ports {
			if port.Name == "admin" {
				fqdn := fmt.Sprintf("%v.%v.%v.svc.%v", p.Spec.Hostname, svc.Name, p.Namespace, clusterDomain)
				a := fmt.Sprintf("%v:%v", fqdn, port.ContainerPort)
				adminAddresses = append(adminAddresses, a)
			}
		}
	}

	if len(adminAddresses) == 0 {
		return nil, fmt.Errorf("could not find any exposed 'admin' container port for the pods in the %q namespace", namespace)
	}

	return adminAddresses, nil
}

// getClusterDomain returns Kubernetes cluster domain, default to
// "cluster.local.".
func getClusterDomain() string {
	const apiSvc = "kubernetes.default.svc"

	cname, err := net.LookupCNAME(apiSvc)
	if err != nil {
		return "cluster.local."
	}
	clusterDomain := strings.TrimPrefix(cname, apiSvc+".")

	return clusterDomain
}

// saveClusterAdminAPICalls saves per-cluster Admin API requests in the 'admin/'
// directory of the bundle zip.
func saveClusterAdminAPICalls(ctx context.Context, ps *stepParams, fs afero.Fs, p *config.RpkProfile, adminAddresses []string, partitions []topicPartitionFilter) step {
	return func() error {
		p = &config.RpkProfile{
			KafkaAPI: config.RpkKafkaAPI{
				SASL: p.KafkaAPI.SASL,
			},
			AdminAPI: config.RpkAdminAPI{
				Addresses: adminAddresses,
				TLS:       p.AdminAPI.TLS,
			},
		}
		cl, err := adminapi.NewClient(fs, p)
		if err != nil {
			return fmt.Errorf("unable to initialize admin client: %v", err)
		}

		var grp multierror.Group
		reqFuncs := []func() error{
			func() error { return requestAndSave(ctx, ps, "admin/brokers.json", cl.Brokers) },
			func() error { return requestAndSave(ctx, ps, "admin/health_overview.json", cl.GetHealthOverview) },
			func() error { return requestAndSave(ctx, ps, "admin/license.json", cl.GetLicenseInfo) },
			func() error { return requestAndSave(ctx, ps, "admin/reconfigurations.json", cl.Reconfigurations) },
			func() error { return requestAndSave(ctx, ps, "admin/features.json", cl.GetFeatures) },
			func() error { return requestAndSave(ctx, ps, "admin/uuid.json", cl.ClusterUUID) },
			func() error {
				return requestAndSave(ctx, ps, "admin/automated_recovery.json", cl.PollAutomatedRecoveryStatus)
			},
			func() error {
				return requestAndSave(ctx, ps, "admin/cloud_storage_lifecycle.json", cl.CloudStorageLifecycle)
			},
			func() error {
				return requestAndSave(ctx, ps, "admin/partition_balancer_status.json", cl.GetPartitionStatus)
			},
			func() error {
				// Need to wrap this function because cl.Config receives an additional 'includeDefaults' param.
				f := func(ctx context.Context) (adminapi.Config, error) {
					return cl.Config(ctx, true)
				}
				return requestAndSave(ctx, ps, "admin/cluster_config.json", f)
			}, func() error {
				f := func(ctx context.Context) (adminapi.ConfigStatusResponse, error) {
					return cl.ClusterConfigStatus(ctx, true)
				}
				return requestAndSave(ctx, ps, "admin/cluster_config_status.json", f)
			}, func() error {
				f := func(ctx context.Context) ([]adminapi.ClusterPartition, error) {
					return cl.AllClusterPartitions(ctx, true, false) // include defaults, and include disabled.
				}
				return requestAndSave(ctx, ps, "admin/cluster_partitions.json", f)
			},
		}
		if partitions != nil {
			extraFuncs := saveExtraFuncs(ctx, ps, cl, partitions)
			reqFuncs = append(reqFuncs, extraFuncs...)
		}
		for _, f := range reqFuncs {
			grp.Go(f)
		}
		errs := grp.Wait()
		return errs.ErrorOrNil()
	}
}

// saveSingleAdminAPICalls saves per-node admin API requests in the 'admin/'
// directory of the bundle zip.
func saveSingleAdminAPICalls(ctx context.Context, ps *stepParams, fs afero.Fs, p *config.RpkProfile, adminAddresses []string, metricsInterval time.Duration) step {
	return func() error {
		var rerrs *multierror.Error
		var funcs []func() error
		for _, a := range adminAddresses {
			a := a
			p = &config.RpkProfile{
				KafkaAPI: config.RpkKafkaAPI{
					SASL: p.KafkaAPI.SASL,
				},
				AdminAPI: config.RpkAdminAPI{
					Addresses: []string{a},
					TLS:       p.AdminAPI.TLS,
				},
			}
			cl, err := adminapi.NewClient(fs, p)
			if err != nil {
				rerrs = multierror.Append(rerrs, fmt.Errorf("unable to initialize admin client for %q: %v", a, err))
				continue
			}

			aName := sanitizeName(a)
			r := []func() error{
				func() error {
					return requestAndSave(ctx, ps, fmt.Sprintf("admin/node_config_%v.json", aName), cl.RawNodeConfig)
				},
				func() error {
					return requestAndSave(ctx, ps, fmt.Sprintf("admin/cluster_view_%v.json", aName), cl.ClusterView)
				},
				func() error {
					return requestAndSave(ctx, ps, fmt.Sprintf("admin/maintenance_status_%v.json", aName), cl.MaintenanceStatus)
				},
				func() error {
					return requestAndSave(ctx, ps, fmt.Sprintf("admin/raft_status_%v.json", aName), cl.RaftRecoveryStatus)
				},
				func() error {
					return requestAndSave(ctx, ps, fmt.Sprintf("admin/partition_leader_table_%v.json", aName), cl.PartitionLeaderTable)
				},
				func() error {
					return requestAndSave(ctx, ps, fmt.Sprintf("admin/is_node_isolated_%v.json", aName), cl.IsNodeIsolated)
				},
				func() error {
					return requestAndSave(ctx, ps, fmt.Sprintf("admin/controller_status_%v.json", aName), cl.ControllerStatus)
				},
				func() error {
					return requestAndSave(ctx, ps, fmt.Sprintf("admin/disk_stat_data_%v.json", aName), cl.DiskData)
				},
				func() error {
					return requestAndSave(ctx, ps, fmt.Sprintf("admin/disk_stat_cache_%v.json", aName), cl.DiskCache)
				},
				func() error {
					err := requestAndSave(ctx, ps, fmt.Sprintf("metrics/%v/t0_metrics.txt", aName), cl.PrometheusMetrics)
					if err != nil {
						return err
					}
					select {
					case <-time.After(metricsInterval):
						return requestAndSave(ctx, ps, fmt.Sprintf("metrics/%v/t1_metrics.txt", aName), cl.PrometheusMetrics)
					case <-ctx.Done():
						return requestAndSave(ctx, ps, fmt.Sprintf("metrics/%v/t1_metrics.txt", aName), cl.PrometheusMetrics)
					}
				},
				func() error {
					err := requestAndSave(ctx, ps, fmt.Sprintf("metrics/%v/t0_public_metrics.txt", aName), cl.PublicMetrics)
					if err != nil {
						return err
					}
					select {
					case <-time.After(metricsInterval):
						return requestAndSave(ctx, ps, fmt.Sprintf("metrics/%v/t1_public_metrics.txt", aName), cl.PublicMetrics)
					case <-ctx.Done():
						return requestAndSave(ctx, ps, fmt.Sprintf("metrics/%v/t1_public_metrics.txt", aName), cl.PublicMetrics)
					}
				},
			}
			funcs = append(funcs, r...)
		}

		var grp multierror.Group
		for _, f := range funcs {
			grp.Go(f)
		}
		errs := grp.Wait()
		if errs != nil {
			rerrs = multierror.Append(rerrs, errs)
		}
		return rerrs.ErrorOrNil()
	}
}

// saveK8SResources will issue a GET request to the K8S API to a set of fixed
// resources that we want to include in the bundle.
func saveK8SResources(ctx context.Context, ps *stepParams, namespace string, labelSelector map[string]string) step {
	return func() error {
		clientset, pods, err := k8sPodList(ctx, namespace, labelSelector)
		if err != nil {
			return err
		}
		// This is a safeguard, so we don't end up saving empty request for
		// namespace who don't have any pods.
		if len(pods.Items) == 0 {
			return fmt.Errorf("skipping resource collection, no pods found in the %q namespace", namespace)
		}

		// We use the restInterface because it's the most straightforward
		// approach to get namespaced resources already parsed as json.
		restInterface := clientset.CoreV1().RESTClient()

		var grp multierror.Group
		for _, r := range []string{
			"configmaps",
			"endpoints",
			"events",
			"limitranges",
			"persistentvolumeclaims",
			"pods",
			"replicationcontrollers",
			"resourcequotas",
			"serviceaccounts",
			"services",
		} {
			r := r
			cb := func(ctx context.Context) ([]byte, error) {
				request := restInterface.Get().Namespace(namespace)
				return request.Name(r).Do(ctx).Raw()
			}
			grp.Go(func() error { return requestAndSave(ctx, ps, fmt.Sprintf("k8s/%v.json", r), cb) })
		}

		errs := grp.Wait()
		return errs.ErrorOrNil()
	}
}

func saveK8SLogs(ctx context.Context, ps *stepParams, namespace, since string, logsLimitBytes int, labelSelector map[string]string) step {
	return func() error {
		clientset, pods, err := k8sPodList(ctx, namespace, labelSelector)
		if err != nil {
			return err
		}
		podsInterface := clientset.CoreV1().Pods(namespace)

		limitBytes := int64(logsLimitBytes)
		logOpts := &k8score.PodLogOptions{
			LimitBytes: &limitBytes,
			Container:  "redpanda",
		}

		if len(since) > 0 {
			st, err := parseJournalTime(since, time.Now())
			if err != nil {
				return fmt.Errorf("unable to save K8S logs: %v", err)
			}
			sinceTime := metav1.NewTime(st)
			logOpts.SinceTime = &sinceTime
		}

		var grp multierror.Group
		for _, p := range pods.Items {
			p := p
			cb := func(ctx context.Context) ([]byte, error) {
				return podsInterface.GetLogs(p.Name, logOpts).Do(ctx).Raw()
			}

			grp.Go(func() error { return requestAndSave(ctx, ps, fmt.Sprintf("logs/%v.txt", p.Name), cb) })
		}

		errs := grp.Wait()
		return errs.ErrorOrNil()
	}
}

// requestAndSave receives a callback function f to be executed and marshals the
// response into a json object that is stored in the zip writer.
func requestAndSave[T1 any](ctx context.Context, ps *stepParams, filename string, f func(ctx context.Context) (T1, error)) error {
	object, err := f(ctx)
	if err != nil {
		return fmt.Errorf("unable to issue request for %q: %v", filename, err)
	}

	switch t := any(object).(type) {
	case []byte:
		err = writeFileToZip(ps, filename, t)
		if err != nil {
			return fmt.Errorf("unable to save output for %q: %v", filename, err)
		}
	default:
		b, err := json.Marshal(object)
		if err != nil {
			return fmt.Errorf("unable to marshal broker response for %q: %v", filename, err)
		}
		err = writeFileToZip(ps, filename, b)
		if err != nil {
			return fmt.Errorf("unable to save output for %q: %v", filename, err)
		}
	}
	return nil
}

// parseJournalTime parses the time given in 'str' relative to 'now' following
// the systemd.time specification that is used by journalctl.
func parseJournalTime(str string, now time.Time) (time.Time, error) {
	/*
		From `man journalctl`:

		Date specifications should be of the format "2012-10-30 18:17:16". If
		the time part is omitted, "00:00:00" is assumed. If only the seconds
		component is omitted, ":00" is assumed. If the date component is
		omitted, the current day is assumed. Alternatively the strings
		"yesterday", "today", "tomorrow" are understood, which refer to 00:00:00
		of the day before the current day, the current day, or the day after the
		current day, respectively. "now" refers to the current time. Finally,
		relative times may be specified, prefixed with "-" or "+", referring to
		times before or after the current time, respectively.
	*/

	// First we ensure that we don't have any leading/trailing whitespace.
	str = strings.TrimSpace(str)

	// Will match YYYY-MM-DD, where Y,M and D are digits.
	ymd := regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`).FindStringSubmatch(str)

	// Will match YYYY-MM-DD HH:MM:SS or YYYY-MM-DD HH:MM
	//   - index 0: the full match.
	//   - index 1: the seconds, if present.
	ymdOptSec := regexp.MustCompile(`^\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}(:\d{2})?`).FindStringSubmatch(str)

	switch {
	// YYYY-MM-DD
	case len(ymd) > 0:
		return time.ParseInLocation("2006-01-02", str, time.Local)

	// YYYY-MM-DD HH:MM:SS or YYYY-MM-DD HH:MM
	case len(ymdOptSec) > 0:
		layout := "2006-01-02 15:04:05" // full match.
		if len(ymdOptSec[1]) == 0 {
			layout = "2006-01-02 15:04" // no seconds.
		}
		return time.ParseInLocation(layout, str, time.Local)

	case str == "now":
		return now, nil

	case str == "yesterday":
		y, m, d := now.AddDate(0, 0, -1).Date()
		return time.Date(y, m, d, 0, 0, 0, 0, time.Local), nil

	case str == "today":
		y, m, d := now.Date()
		return time.Date(y, m, d, 0, 0, 0, 0, time.Local), nil

	// This is either a relative time (+/-) or an error
	default:
		dur, err := time.ParseDuration(str)
		if err != nil {
			return time.Time{}, fmt.Errorf("unable to parse time %q: %v", str, err)
		}
		adjustedTime := now.Add(dur)
		return adjustedTime, nil
	}
}

// sanitizeName replace any of the following characters with "-": "<", ">", ":",
// `"`, "/", "|", "?", "*". This is to avoid having forbidden names in Windows
// environments.
func sanitizeName(name string) string {
	forbidden := []string{"<", ">", ":", `"`, "/", `\`, "|", "?", "*"}
	r := name
	for _, s := range forbidden {
		r = strings.Replace(r, s, "-", -1)
	}
	return r
}

func saveExtraFuncs(ctx context.Context, ps *stepParams, cl *adminapi.AdminAPI, partitionFilters []topicPartitionFilter) (funcs []func() error) {
	for _, tpf := range partitionFilters {
		tpf := tpf
		for _, p := range tpf.partitionsID {
			p := p
			funcs = append(funcs, []func() error{
				func() error {
					f := func(ctx context.Context) (adminapi.Partition, error) {
						return cl.GetPartition(ctx, tpf.namespace, tpf.topic, p)
					}
					return requestAndSave(ctx, ps, fmt.Sprintf("partitions/info_%v_%v_%v.json", tpf.namespace, tpf.topic, p), f)
				},
				func() error {
					f := func(ctx context.Context) (adminapi.DebugPartition, error) {
						return cl.DebugPartition(ctx, tpf.namespace, tpf.topic, p)
					}
					return requestAndSave(ctx, ps, fmt.Sprintf("partitions/debug_%v_%v_%v.json", tpf.namespace, tpf.topic, p), f)
				},
				func() error {
					f := func(ctx context.Context) (adminapi.CloudStorageStatus, error) {
						return cl.CloudStorageStatus(ctx, tpf.topic, strconv.Itoa(p))
					}
					return requestAndSave(ctx, ps, fmt.Sprintf("partitions/cloud_status_%v_%v.json", tpf.topic, p), f)
				},
				func() error {
					f := func(ctx context.Context) (adminapi.CloudStorageManifest, error) {
						return cl.CloudStorageManifest(ctx, tpf.topic, p)
					}
					return requestAndSave(ctx, ps, fmt.Sprintf("partitions/cloud_manifest_%v_%v.json", tpf.topic, p), f)
				},
				func() error {
					f := func(ctx context.Context) (adminapi.CloudStorageAnomalies, error) {
						return cl.CloudStorageAnomalies(ctx, tpf.namespace, tpf.topic, p)
					}
					return requestAndSave(ctx, ps, fmt.Sprintf("partitions/cloud_anomalies_%v_%v_%v.json", tpf.namespace, tpf.topic, p), f)
				},
			}...)
		}
	}
	return
}
