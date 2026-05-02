// Copyright 2026 Redpanda Data, Inc.
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
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/beevik/ntp"
	"github.com/redpanda-data/common-go/rpadmin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/kafka"
	"github.com/spf13/afero"
	"github.com/twmb/franz-go/pkg/kadm"
	authorizationv1 "k8s.io/api/authorization/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const (
	categoryFile     = "file"
	categoryCommand  = "command"
	categoryK8sRBAC  = "k8s_rbac"
	categoryAdminAPI = "admin_api"
	categoryKafka    = "kafka"
	categoryNetwork  = "network"
)

// probeResult describes the outcome of a single permission/access probe.
type probeResult struct {
	Category string `json:"category" yaml:"category"`
	Resource string `json:"resource" yaml:"resource"`
	OK       bool   `json:"ok" yaml:"ok"`
	Error    string `json:"error,omitempty" yaml:"error,omitempty"`
	Hint     string `json:"hint,omitempty" yaml:"hint,omitempty"`
}

// dryRunResult is the full output of a --dry-run invocation.
type dryRunResult struct {
	Probes []probeResult `json:"probes" yaml:"probes"`
}

// runDryRun executes every permission/access probe that the debug bundle
// would rely on and returns the structured result. It performs no file
// collection and creates no output bundle.
func runDryRun(
	ctx context.Context,
	afs afero.Fs,
	p *config.RpkProfile,
	y *config.RedpandaYaml,
	namespace, outputPath string,
) dryRunResult {
	var probes []probeResult

	procFiles := []struct {
		path string
		hint string
	}{
		{"/proc/slabinfo", "requires root (debug bundle reads /proc/slabinfo for slab allocator info)"},
		{"/proc/cpuinfo", ""},
		{"/proc/interrupts", ""},
		{"/proc/softirqs", ""},
		{"/proc/mounts", ""},
		{"/proc/cmdline", ""},
		{"/proc/mdstat", ""},
		{"/proc/kallsyms", "requires kernel.kptr_restrict=0 or CAP_SYSLOG for non-zero addresses"},
	}
	for _, pf := range procFiles {
		probes = append(probes, probeFileRead(afs, categoryFile, pf.path, pf.hint))
	}

	if y != nil && y.Redpanda.Directory != "" {
		probes = append(probes, probeDirRead(afs, categoryFile, y.Redpanda.Directory,
			"Redpanda data directory; debug bundle walks this tree to describe layout"))
	}

	if outputPath != "" {
		probes = append(probes, probeDirWrite(afs, categoryFile, filepath.Dir(outputPath),
			"debug bundle output directory"))
	}

	isRoot := os.Geteuid() == 0
	commands := []struct {
		name         string
		requiresRoot bool
		hint         string
	}{
		{"dmidecode", true, "dmidecode reads DMI tables via /dev/mem; must run as root"},
		{"journalctl", false, "requires membership in 'adm' or 'systemd-journal' group to read service logs"},
		{"ethtool", false, "some subcommands (e.g., -c) require root"},
		{"df", false, ""},
		{"dig", false, ""},
		{"du", false, ""},
		{"free", false, ""},
		{"ip", false, ""},
		{"lsblk", false, ""},
		{"lspci", false, ""},
		{"ss", false, ""},
		{"sysctl", false, ""},
		{"top", false, ""},
		{"uname", false, ""},
		{"uptime", false, ""},
		{"vmstat", false, ""},
	}
	for _, c := range commands {
		probes = append(probes, probeCommand(c.name, c.requiresRoot, isRoot, c.hint))
	}

	if p != nil {
		for _, addr := range p.AdminAPI.Addresses {
			probes = append(probes, probeAdminAPI(ctx, afs, p, addr))
		}
		probes = append(probes, probeKafka(ctx, afs, p))
	}

	probes = append(probes, probeNTP(ctx, "pool.ntp.org"))

	inK8s := os.Getenv("KUBERNETES_SERVICE_HOST") != "" && os.Getenv("KUBERNETES_SERVICE_PORT") != ""
	if inK8s {
		ns := resolveNamespace(namespace)
		cl, err := k8sClientset()
		if err != nil {
			probes = append(probes, probeResult{
				Category: categoryK8sRBAC,
				Resource: fmt.Sprintf("kubernetes client in namespace %q", ns),
				Error:    fmt.Sprintf("unable to create kubernetes client: %v", err),
				Hint:     "running inside Kubernetes but the in-cluster config is unavailable or invalid; check the ServiceAccount mount and KUBERNETES_SERVICE_* env vars",
			})
		} else {
			k8sListResources := []string{
				"pods",
				"services",
				"configmaps",
				"endpoints",
				"events",
				"limitranges",
				"persistentvolumeclaims",
				"replicationcontrollers",
				"resourcequotas",
				"serviceaccounts",
			}
			for _, resource := range k8sListResources {
				probes = append(probes, probeK8sRBAC(ctx, cl, ns, "list", resource, ""))
			}
			probes = append(probes, probeK8sRBAC(ctx, cl, ns, "get", "pods", "log"))
		}
		probes = append(probes, probeClusterDNS(ctx))
	}

	return dryRunResult{Probes: probes}
}

func probeFileRead(afs afero.Fs, category, path, hint string) probeResult {
	r := probeResult{Category: category, Resource: path, Hint: hint}
	f, err := afs.Open(path)
	if err != nil {
		r.Error = err.Error()
		if errors.Is(err, os.ErrPermission) && hint == "" {
			r.Hint = "permission denied; may require root"
		}
		return r
	}
	defer f.Close()
	// Some /proc entries (e.g. kptr-restricted kallsyms) open for non-root
	// but fail on read; a 1-byte read exercises that path.
	var buf [1]byte
	if _, err := f.Read(buf[:]); err != nil && !errors.Is(err, io.EOF) {
		r.Error = err.Error()
		if errors.Is(err, os.ErrPermission) && hint == "" {
			r.Hint = "readable to open(2) but read(2) denied; may require root"
		}
		return r
	}
	r.OK = true
	r.Hint = ""
	return r
}

func probeDirRead(afs afero.Fs, category, path, hint string) probeResult {
	r := probeResult{Category: category, Resource: path, Hint: hint}
	info, err := afs.Stat(path)
	if err != nil {
		r.Error = err.Error()
		return r
	}
	if !info.IsDir() {
		r.Error = "not a directory"
		return r
	}
	d, err := afs.Open(path)
	if err != nil {
		r.Error = err.Error()
		return r
	}
	defer d.Close()
	if rd, ok := d.(interface {
		Readdirnames(int) ([]string, error)
	}); ok {
		if _, err := rd.Readdirnames(1); err != nil && !errors.Is(err, io.EOF) {
			r.Error = err.Error()
			return r
		}
	}
	r.OK = true
	r.Hint = ""
	return r
}

func probeDirWrite(afs afero.Fs, category, path, hint string) probeResult {
	r := probeResult{Category: category, Resource: path, Hint: hint}
	info, err := afs.Stat(path)
	if err != nil {
		r.Error = err.Error()
		return r
	}
	if !info.IsDir() {
		r.Error = "not a directory"
		return r
	}
	tmp, err := afero.TempFile(afs, path, ".rpk-dry-run-*")
	if err != nil {
		r.Error = err.Error()
		if errors.Is(err, os.ErrPermission) {
			r.Hint = "no write permission; bundle zip cannot be created here"
		}
		return r
	}
	tmp.Close()
	afs.Remove(tmp.Name())
	r.OK = true
	r.Hint = ""
	return r
}

func probeCommand(name string, requiresRoot, isRoot bool, hint string) probeResult {
	r := probeResult{Category: categoryCommand, Resource: name, Hint: hint}
	path, err := exec.LookPath(name)
	if err != nil {
		r.Error = fmt.Sprintf("%s not found in PATH", name)
		return r
	}
	info, err := os.Stat(path)
	if err != nil {
		r.Error = err.Error()
		return r
	}
	if info.Mode()&0o111 == 0 {
		r.Error = "not executable"
		return r
	}
	if requiresRoot && !isRoot {
		r.Error = "command requires root privileges; will fail when debug bundle runs as non-root"
		return r
	}
	r.OK = true
	r.Hint = ""
	return r
}

func probeK8sRBAC(ctx context.Context, cl kubernetes.Interface, namespace, verb, resource, subresource string) probeResult {
	fullResource := resource
	if subresource != "" {
		fullResource = resource + "/" + subresource
	}
	r := probeResult{
		Category: categoryK8sRBAC,
		Resource: fmt.Sprintf("%s %s in namespace %q", verb, fullResource, namespace),
		Hint:     fmt.Sprintf("service account needs '%s' on '%s' in namespace '%s'", verb, fullResource, namespace),
	}
	sar := &authorizationv1.SelfSubjectAccessReview{
		Spec: authorizationv1.SelfSubjectAccessReviewSpec{
			ResourceAttributes: &authorizationv1.ResourceAttributes{
				Namespace:   namespace,
				Verb:        verb,
				Resource:    resource,
				Subresource: subresource,
			},
		},
	}
	resp, err := cl.AuthorizationV1().SelfSubjectAccessReviews().Create(ctx, sar, metav1.CreateOptions{})
	if err != nil {
		r.Error = err.Error()
		return r
	}
	if !resp.Status.Allowed {
		r.Error = "permission denied"
		return r
	}
	r.OK = true
	r.Hint = ""
	return r
}

func probeAdminAPI(ctx context.Context, afs afero.Fs, p *config.RpkProfile, addr string) probeResult {
	r := probeResult{
		Category: categoryAdminAPI,
		Resource: addr,
		Hint:     "admin API connectivity + auth; bundle collects cluster config, health, metrics via this endpoint",
	}
	scoped := *p
	scoped.AdminAPI.Addresses = []string{addr}

	cl, err := adminapi.NewClient(ctx, afs, &scoped, rpadmin.ClientTimeout(5*time.Second))
	if err != nil {
		r.Error = err.Error()
		return r
	}
	if _, err := cl.Brokers(ctx); err != nil {
		r.Error = err.Error()
		return r
	}
	r.OK = true
	r.Hint = ""
	return r
}

func probeKafka(ctx context.Context, afs afero.Fs, p *config.RpkProfile) probeResult {
	brokers := "(default)"
	if len(p.KafkaAPI.Brokers) > 0 {
		brokers = fmt.Sprintf("%v", p.KafkaAPI.Brokers)
	}
	r := probeResult{
		Category: categoryKafka,
		Resource: brokers,
		Hint:     "Kafka broker connectivity + auth; bundle collects topic/group/offset metadata via this connection",
	}
	cl, err := kafka.NewFranzClient(afs, p)
	if err != nil {
		r.Error = err.Error()
		return r
	}
	defer cl.Close()

	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	adm := kadm.NewClient(cl)
	if _, err := adm.BrokerMetadata(cctx); err != nil {
		r.Error = err.Error()
		return r
	}
	r.OK = true
	r.Hint = ""
	return r
}

func probeNTP(ctx context.Context, host string) probeResult {
	r := probeResult{
		Category: categoryNetwork,
		Resource: fmt.Sprintf("NTP %s", host),
		Hint:     "bundle queries NTP for clock-drift; needs UDP/123 egress",
	}
	done := make(chan error, 1)
	go func() {
		_, err := ntp.Query(host)
		done <- err
	}()
	select {
	case <-ctx.Done():
		r.Error = ctx.Err().Error()
		return r
	case err := <-done:
		if err != nil {
			r.Error = err.Error()
			return r
		}
	}
	r.OK = true
	r.Hint = ""
	return r
}

func probeClusterDNS(ctx context.Context) probeResult {
	const name = "kubernetes.default.svc"
	r := probeResult{
		Category: categoryNetwork,
		Resource: fmt.Sprintf("DNS CNAME %s", name),
		Hint:     "in-cluster DNS; bundle uses this to discover the K8s cluster domain",
	}
	var resolver net.Resolver
	if _, err := resolver.LookupCNAME(ctx, name); err != nil {
		r.Error = err.Error()
		return r
	}
	r.OK = true
	r.Hint = ""
	return r
}

func printDryRunText(result dryRunResult) {
	pass, fail := 0, 0
	for _, p := range result.Probes {
		if p.OK {
			pass++
		} else {
			fail++
		}
	}
	fmt.Printf("Debug bundle dry-run: %d probe(s), %d ok, %d issue(s)\n\n", len(result.Probes), pass, fail)

	if fail == 0 {
		fmt.Println("All probes passed. A debug bundle would collect every resource successfully.")
		return
	}

	fmt.Println("Issues detected (these resources will be skipped or fail when generating a real bundle):")
	for _, p := range result.Probes {
		if p.OK {
			continue
		}
		fmt.Printf("  - [%s] %s: %s", p.Category, p.Resource, p.Error)
		if p.Hint != "" {
			fmt.Printf(" (%s)", p.Hint)
		}
		fmt.Println()
	}
}
