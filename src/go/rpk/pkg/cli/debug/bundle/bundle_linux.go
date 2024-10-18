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
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/avast/retry-go"
	"github.com/beevik/ntp"
	"github.com/docker/go-units"
	"github.com/hashicorp/go-multierror"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	osutil "github.com/redpanda-data/redpanda/src/go/rpk/pkg/os"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/system"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/system/syslog"
	"github.com/spf13/afero"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"golang.org/x/sys/unix"
	"gopkg.in/yaml.v3"
)

const linuxUtilsRoot = "utils"

// determineFilepath will process the given path and sets:
//   - File Name: If the path is empty, the filename will be <timestamp>-bundle.zip
//   - File Extension: if no extension is provided we default to .zip
//   - File Location: we check for write permissions in the pwd (for backcompat);
//     if permission is denied we default to $HOME unless isFlag is true.
func determineFilepath(fs afero.Fs, rp *config.RedpandaYaml, path string, isFlag bool) (finalPath string, err error) {
	// if it's empty, use ./<timestamp>-bundle.zip
	if path == "" {
		timestamp := time.Now().Unix()
		if rp.Redpanda.AdvertisedRPCAPI != nil {
			path = fmt.Sprintf("%v-%d-bundle.zip", sanitizeName(rp.Redpanda.AdvertisedRPCAPI.Address), timestamp)
		} else {
			path = fmt.Sprintf("%d-bundle.zip", timestamp)
		}
	} else if isDir, _ := afero.IsDir(fs, path); isDir {
		return "", fmt.Errorf("output file path is a directory, please specify the name of the file")
	}

	// Check for file extension, if extension is empty, defaults to .zip
	switch ext := filepath.Ext(path); ext {
	case ".zip":
		finalPath = path
	case "":
		finalPath = path + ".zip"
	default:
		return "", fmt.Errorf("extension %q not supported", ext)
	}

	// Now we check for write permissions:
	err = unix.Access(filepath.Dir(finalPath), unix.W_OK)
	if err != nil {
		if !errors.Is(err, os.ErrPermission) {
			return "", err
		}
		if isFlag {
			// If the user sets the output flag, we fail if we don't have
			// write permissions
			return "", fmt.Errorf("unable to create bundle file in %q: %v", path, err)
		}
		home, err := os.UserHomeDir()
		if err != nil {
			out.Die("unable to create bundle file in %q due to permission issues and cannot use home directory: %v", path, err)
		}
		// We are here only if the user did not specify a flag so finalpath
		// here is the <timestamp>-bundle.zip string
		finalPath = filepath.Join(home, finalPath)
	}
	return finalPath, nil
}

func executeBundle(ctx context.Context, bp bundleParams) error {
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

	grp := multierror.Group{}

	w := zip.NewWriter(f)
	defer w.Close()

	ps := &stepParams{
		fs:       bp.fs,
		w:        w,
		timeout:  bp.timeout,
		fileRoot: strings.TrimSuffix(filepath.Base(bp.path), ".zip"),
	}

	addrs := bp.y.Rpk.AdminAPI.Addresses

	steps := []step{
		saveCPUInfo(ps),
		saveClusterAdminAPICalls(ctx, ps, bp.fs, bp.p, addrs, bp.partitions),
		saveCmdLine(ps),
		saveConfig(ps, bp.y),
		saveControllerLogDir(ps, bp.y, bp.controllerLogLimitBytes),
		saveDNSData(ctx, ps),
		saveDataDirStructure(ps, bp.y),
		saveDiskUsage(ctx, ps, bp.y),
		saveDmidecode(ctx, ps),
		saveFree(ctx, ps),
		saveIP(ctx, ps),
		saveInterrupts(ps),
		saveKafkaMetadata(ctx, ps, bp.cl),
		saveKernelSymbols(ps),
		saveLogs(ctx, ps, bp.logsSince, bp.logsUntil, bp.logsLimitBytes),
		saveLspci(ctx, ps),
		saveMdstat(ps),
		saveMountedFilesystems(ps),
		saveNTPDrift(ps),
		saveResourceUsageData(ps, bp.y),
		saveSingleAdminAPICalls(ctx, ps, bp.fs, bp.p, addrs, bp.cpuProfilerWait),
		saveMetricsAPICalls(ctx, ps, bp.fs, bp.p, addrs, bp.metricsInterval, bp.metricsSampleCount),
		saveSlabInfo(ps),
		saveSocketData(ctx, ps),
		saveSysctl(ctx, ps),
		saveSyslog(ps),
		saveTopOutput(ctx, ps),
		saveUname(ctx, ps),
		saveVmstat(ctx, ps),
	}

	for _, s := range steps {
		grp.Go(s)
	}

	errs := grp.Wait()
	if errs != nil {
		err := writeFileToZip(ps, "errors.txt", []byte(errs.Error()))
		if err != nil {
			errs = multierror.Append(errs, err)
		}
		fmt.Println(errs.Error())
	}

	fmt.Printf("Debug bundle saved to '%s'\n", bp.path)
	return nil
}

type step func() error

type stepParams struct {
	fs       afero.Fs
	m        sync.Mutex
	w        *zip.Writer
	timeout  time.Duration
	fileRoot string
}

type fileInfo struct {
	Size      string `json:"size"`
	Mode      string `json:"mode"`
	Symlink   string `json:"symlink,omitempty"`
	Error     string `json:"error,omitempty"`
	Modified  string `json:"modified"`
	User      string `json:"user"`
	Group     string `json:"group"`
	SizeBytes int64  `json:"size_bytes"`
}

type limitedWriter struct {
	w          io.Writer
	limitBytes int
	accumBytes int
}

func (l *limitedWriter) Write(p []byte) (int, error) {
	limitReached := false
	if l.accumBytes+len(p) > l.limitBytes {
		p = p[:l.limitBytes-l.accumBytes]
		limitReached = true
	}

	n, err := l.w.Write(p)
	if err != nil {
		return n, err
	}

	l.accumBytes += n

	if limitReached {
		return n, errors.New("output size limit reached")
	}
	return n, nil
}

// Creates a file in the zip writer with name 'filename' and writes 'contents' to it.
func writeFileToZip(ps *stepParams, filename string, contents []byte) error {
	ps.m.Lock()
	defer ps.m.Unlock()

	wr, err := ps.w.CreateHeader(&zip.FileHeader{
		Name:     filepath.Join(ps.fileRoot, filename),
		Method:   zip.Deflate,
		Modified: time.Now(),
	})
	if err != nil {
		return err
	}
	_, err = wr.Write(contents)
	if err != nil {
		return fmt.Errorf("couldn't save '%s': %w", filename, err)
	}
	return nil
}

// writeDirToZip walks the 'srcDir' and writes the content in 'destDir'
// directory in the zip writer. It will exclude the files that match the
// 'exclude' regexp.
func writeDirToZip(ps *stepParams, srcDir, destDir string, exclude *regexp.Regexp) error {
	return filepath.WalkDir(srcDir, func(_ string, f fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !f.IsDir() {
			filename := f.Name()
			if exclude != nil && exclude.MatchString(filename) {
				return nil
			}
			file, err := os.ReadFile(filepath.Join(srcDir, filename))
			if err != nil {
				return err
			}
			err = writeFileToZip(ps, filepath.Join(destDir, filename), file)
			if err != nil {
				return err
			}
		}
		return err
	})
}

// Runs a command and pipes its output to a new file in the zip writer.
func writeCommandOutputToZipLimit(
	rootCtx context.Context,
	ps *stepParams,
	filename string,
	outputLimitBytes int,
	command string,
	args ...string,
) error {
	ps.m.Lock()
	defer ps.m.Unlock()

	ctx, cancel := context.WithTimeout(rootCtx, ps.timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, command, args...)

	// Strip any non-default library path
	cmd.Env = osutil.SystemLdPathEnv()

	wr, err := ps.w.CreateHeader(&zip.FileHeader{
		Name:     filepath.Join(ps.fileRoot, filename),
		Method:   zip.Deflate,
		Modified: time.Now(),
	})
	if err != nil {
		return err
	}

	if outputLimitBytes > 0 {
		wr = &limitedWriter{
			w:          wr,
			limitBytes: outputLimitBytes,
		}
	}

	cmd.Stdout = wr
	cmd.Stderr = wr

	err = cmd.Start()
	if err != nil {
		return err
	}

	err = cmd.Wait()
	if err != nil {
		if !strings.Contains(err.Error(), "broken pipe") {
			return fmt.Errorf("couldn't save '%s': %w; %[1]v contains the full error message", filename, err)
		}
		zap.L().Sugar().Warnf("%v: got '%v' while running '%s'. This is probably due to the command's output exceeding its limit in bytes.", filename, err, cmd)
	}
	return nil
}

// Runs a command and pipes its output to a new file in the zip writer.
func writeCommandOutputToZip(
	ctx context.Context, ps *stepParams, filename, command string, args ...string,
) error {
	return writeCommandOutputToZipLimit(ctx, ps, filename, -1, command, args...)
}

// Parses an error return from kadm, and if the return is a shard errors,
// returns a list of each individual error.
func stringifyKadmErr(err error) []string {
	var ae *kadm.AuthError
	var se *kadm.ShardErrors
	switch {
	case err == nil:
		return nil

	case errors.As(err, &se):
		var errs []string
		for _, err := range se.Errs {
			errs = append(errs, fmt.Sprintf("%s to %s (%d) failed: %s",
				se.Name,
				net.JoinHostPort(err.Broker.Host, strconv.Itoa(int(err.Broker.Port))),
				err.Broker.NodeID,
				err.Err,
			))
		}
		return errs

	case errors.As(err, &ae):
		return []string{fmt.Sprintf("authorization error: %s", err)}

	default:
		return []string{err.Error()}
	}
}

func saveKafkaMetadata(rootCtx context.Context, ps *stepParams, cl *kgo.Client) step {
	return func() error {
		zap.L().Sugar().Debug("Reading Kafka information")

		ctx, cancel := context.WithTimeout(rootCtx, 10*time.Second)
		defer cancel()

		type resp struct {
			Name     string      // the request the response is for
			Response interface{} // a raw response from kadm
			Error    []string    // no error, or one error, or potentially many shard errors
		}
		var resps []resp

		adm := kadm.NewClient(cl)

		meta, err := adm.Metadata(ctx)
		resps = append(resps, resp{
			Name:     "metadata",
			Response: meta,
			Error:    stringifyKadmErr(err),
		})

		tcs, err := adm.DescribeTopicConfigs(ctx, meta.Topics.Names()...)
		resps = append(resps, resp{
			Name:     "topic_configs",
			Response: tcs,
			Error:    stringifyKadmErr(err),
		})

		bcs, err := adm.DescribeBrokerConfigs(ctx, meta.Brokers.NodeIDs()...)
		resps = append(resps, resp{
			Name:     "broker_configs",
			Response: bcs,
			Error:    stringifyKadmErr(err),
		})

		ostart, err := adm.ListStartOffsets(ctx)
		resps = append(resps, resp{
			Name:     "log_start_offsets",
			Response: ostart,
			Error:    stringifyKadmErr(err),
		})

		ocommitted, err := adm.ListCommittedOffsets(ctx)
		resps = append(resps, resp{
			Name:     "last_stable_offsets",
			Response: ocommitted,
			Error:    stringifyKadmErr(err),
		})

		oend, err := adm.ListEndOffsets(ctx)
		resps = append(resps, resp{
			Name:     "high_watermarks",
			Response: oend,
			Error:    stringifyKadmErr(err),
		})

		groups, err := adm.DescribeGroups(ctx)
		resps = append(resps, resp{
			Name:     "groups",
			Response: groups,
			Error:    stringifyKadmErr(err),
		})

		fetched := adm.FetchManyOffsets(ctx, groups.Names()...)
		for _, fetch := range fetched {
			resps = append(resps, resp{
				Name:     fmt.Sprintf("group_commits_%s", fetch.Group),
				Response: fetch.Fetched,
				Error:    stringifyKadmErr(fetch.Err),
			})
		}

		marshal, err := json.Marshal(resps)
		if err != nil {
			return fmt.Errorf("unable to encode kafka admin responses: %v", err)
		}

		return writeFileToZip(ps, "kafka.json", marshal)
	}
}

// Walks the redpanda data directory recursively, and saves to the bundle
// a JSON map where the keys are the file/ dir paths, and the values are
// objects containing their data: size, mode, the file or dir it points to
// if the current file is a symlink, the time it was modified, its owner and
// its group, as well as an error message if reading that specific file failed.
func saveDataDirStructure(ps *stepParams, y *config.RedpandaYaml) step {
	return func() error {
		files := make(map[string]*fileInfo)
		err := walkDir(y.Redpanda.Directory, files)
		if err != nil {
			return fmt.Errorf("couldn't walk '%s': %w", y.Redpanda.Directory, err)
		}
		bs, err := json.Marshal(files)
		if err != nil {
			return fmt.Errorf(
				"couldn't encode the '%s' directory structure as JSON: %w",
				y.Redpanda.Directory,
				err,
			)
		}
		return writeFileToZip(ps, "data-dir.txt", bs)
	}
}

// Writes the config file to the bundle, redacting SASL credentials.
func saveConfig(ps *stepParams, y *config.RedpandaYaml) step {
	return func() error {
		// Redact SASL credentials
		redacted := "(REDACTED)"
		if y.Rpk.KafkaAPI.SASL != nil {
			y.Rpk.KafkaAPI.SASL.User = redacted
			y.Rpk.KafkaAPI.SASL.Password = redacted
		}
		// We want to redact any blindly decoded parameters.
		redactOtherMap(y.Other)
		redactOtherMap(y.Redpanda.Other)
		redactServerTLSSlice(y.Redpanda.KafkaAPITLS)
		redactServerTLSSlice(y.Redpanda.AdminAPITLS)
		if y.SchemaRegistry != nil {
			for _, server := range y.SchemaRegistry.SchemaRegistryAPITLS {
				redactOtherMap(server.Other)
			}
		}
		if y.Pandaproxy != nil {
			redactOtherMap(y.Pandaproxy.Other)
			redactServerTLSSlice(y.Pandaproxy.PandaproxyAPITLS)
		}
		if y.PandaproxyClient != nil {
			redactOtherMap(y.PandaproxyClient.Other)
			y.PandaproxyClient.SCRAMPassword = &redacted
			y.PandaproxyClient.SCRAMUsername = &redacted
		}
		if y.SchemaRegistryClient != nil {
			redactOtherMap(y.SchemaRegistryClient.Other)
			y.SchemaRegistryClient.SCRAMPassword = &redacted
			y.SchemaRegistryClient.SCRAMUsername = &redacted
		}

		bs, err := yaml.Marshal(y)
		if err != nil {
			return fmt.Errorf("couldn't encode the redpanda config as YAML: %w", err)
		}
		return writeFileToZip(ps, "redpanda.yaml", bs)
	}
}

func redactServerTLSSlice(servers []config.ServerTLS) {
	for _, server := range servers {
		redactOtherMap(server.Other)
	}
}

func redactOtherMap(other map[string]interface{}) {
	for k := range other {
		other[k] = "(REDACTED)"
	}
}

// Saves the contents of '/proc/cpuinfo'.
func saveCPUInfo(ps *stepParams) step {
	return func() error {
		bs, err := afero.ReadFile(ps.fs, "/proc/cpuinfo")
		if err != nil {
			return err
		}
		return writeFileToZip(ps, "proc/cpuinfo", bs)
	}
}

// Saves the contents of '/proc/interrupts'.
func saveInterrupts(ps *stepParams) step {
	return func() error {
		bs, err := afero.ReadFile(ps.fs, "/proc/interrupts")
		if err != nil {
			return err
		}
		return writeFileToZip(ps, "proc/interrupts", bs)
	}
}

// Saves the contents of '/proc/mounts'.
func saveMountedFilesystems(ps *stepParams) step {
	return func() error {
		bs, err := afero.ReadFile(ps.fs, "/proc/mounts")
		if err != nil {
			return err
		}
		return writeFileToZip(ps, "proc/mounts", bs)
	}
}

// Saves the contents of '/proc/slabinfo'. Requires Sudo.
func saveSlabInfo(ps *stepParams) step {
	return func() error {
		bs, err := afero.ReadFile(ps.fs, "/proc/slabinfo")
		if err != nil {
			if errors.Is(err, fs.ErrPermission) {
				return fmt.Errorf("%v: you may need to run the command as root to read this file", err)
			}
			return err
		}
		return writeFileToZip(ps, "proc/slabinfo", bs)
	}
}

// Saves the contents of '/proc/cmdline'.
func saveCmdLine(ps *stepParams) step {
	return func() error {
		bs, err := afero.ReadFile(ps.fs, "/proc/cmdline")
		if err != nil {
			return err
		}
		return writeFileToZip(ps, "proc/cmdline", bs)
	}
}

// Saves the contents of '/proc/mdstat'.
func saveMdstat(ps *stepParams) step {
	return func() error {
		bs, err := afero.ReadFile(ps.fs, "/proc/mdstat")
		if err != nil {
			return err
		}
		return writeFileToZip(ps, "proc/mdstat", bs)
	}
}

// Saves the contents of '/proc/kallsyms'.
func saveKernelSymbols(ps *stepParams) step {
	return func() error {
		bs, err := afero.ReadFile(ps.fs, "/proc/kallsyms")
		if err != nil {
			return err
		}
		return writeFileToZip(ps, "proc/kallsyms", bs)
	}
}

// Writes a file containing memory, disk & CPU usage metrics for a local
// redpanda process.
func saveResourceUsageData(ps *stepParams, y *config.RedpandaYaml) step {
	return func() error {
		res, err := system.GatherMetrics(ps.fs, ps.timeout, y)
		if system.IsErrRedpandaDown(err) {
			return fmt.Errorf("omitting resource usage metrics: %w", err)
		}
		if err != nil {
			return fmt.Errorf("error gathering resource usage metrics: %w", err)
		}
		bs, err := json.Marshal(res)
		if err != nil {
			return fmt.Errorf("couldn't encode resource usage metrics: %w", err)
		}
		return writeFileToZip(ps, "resource-usage.json", bs)
	}
}

// Queries 'pool.ntp.org' and writes a file with the reported RTT, time & precision.
func saveNTPDrift(ps *stepParams) step {
	return func() error {
		const (
			host    = "pool.ntp.org"
			retries = 3
		)

		var (
			response  *ntp.Response
			localTime time.Time
			err       error
		)

		queryNTP := func() error {
			localTime = time.Now()
			response, err = ntp.Query(host)
			return err
		}

		err = retry.Do(
			queryNTP,
			retry.Attempts(retries),
			retry.DelayType(retry.FixedDelay),
			retry.Delay(1*time.Second),
			retry.LastErrorOnly(true),
			retry.OnRetry(func(n uint, err error) {
				zap.L().Sugar().Debugf("Couldn't retrieve NTP data from %s: %v", host, err)
				zap.L().Sugar().Debugf("Retrying (%d retries left)", retries-n)
			}),
		)
		if err != nil {
			return fmt.Errorf("error querying '%s': %w", host, err)
		}

		result := struct {
			Host            string        `json:"host"`
			RoundTripTimeMs int64         `json:"roundTripTimeMs"`
			RemoteTimeUTC   time.Time     `json:"remoteTimeUTC"`
			LocalTimeUTC    time.Time     `json:"localTimeUTC"`
			PrecisionMs     int64         `json:"precisionMs"`
			Offset          time.Duration `json:"offset"`
		}{
			Host:            host,
			RoundTripTimeMs: response.RTT.Milliseconds(),
			RemoteTimeUTC:   response.Time.UTC(),
			LocalTimeUTC:    localTime.UTC(),
			PrecisionMs:     response.Precision.Milliseconds(),
			Offset:          response.ClockOffset,
		}

		marshalled, err := json.Marshal(result)
		if err != nil {
			return fmt.Errorf("couldn't marshal the NTP response: %w", err)
		}

		return writeFileToZip(
			ps,
			filepath.Join(linuxUtilsRoot, "ntp.txt"),
			marshalled,
		)
	}
}

func saveSyslog(ps *stepParams) step {
	return func() error {
		entries, err := syslog.ReadAll()
		if err != nil {
			return err
		}
		return writeFileToZip(ps, filepath.Join(linuxUtilsRoot, "syslog.txt"), entries)
	}
}

// Saves the output of `dig`.
func saveDNSData(ctx context.Context, ps *stepParams) step {
	return func() error {
		return writeCommandOutputToZip(ctx, ps, filepath.Join(linuxUtilsRoot, "dig.txt"), "dig")
	}
}

// Saves the output of `uname -a`.
func saveUname(ctx context.Context, ps *stepParams) step {
	return func() error {
		return writeCommandOutputToZip(ctx, ps, filepath.Join(linuxUtilsRoot, "uname.txt"), "uname", "-a")
	}
}

// Saves the disk usage total within redpanda's data directory.
func saveDiskUsage(ctx context.Context, ps *stepParams, y *config.RedpandaYaml) step {
	return func() error {
		return writeCommandOutputToZip(
			ctx,
			ps,
			filepath.Join(linuxUtilsRoot, "du.txt"),
			"du", "-h", y.Redpanda.Directory,
		)
	}
}

// Writes the journald redpanda logs, if available, to the bundle.
func saveLogs(ctx context.Context, ps *stepParams, since, until string, logsLimitBytes int) step {
	return func() error {
		args := []string{"--no-pager", "-u", "redpanda"}
		if since != "" {
			args = append(args, "--since", since)
		}
		if until != "" {
			args = append(args, "--until", until)
		}
		return writeCommandOutputToZipLimit(
			ctx,
			ps,
			"redpanda.log",
			logsLimitBytes,
			"journalctl",
			args...,
		)
	}
}

// Saves the output of `ss`.
func saveSocketData(ctx context.Context, ps *stepParams) step {
	return func() error {
		return writeCommandOutputToZip(ctx, ps, filepath.Join(linuxUtilsRoot, "ss.txt"), "ss")
	}
}

// Saves the output of `top`.
func saveTopOutput(ctx context.Context, ps *stepParams) step {
	return func() error {
		return writeCommandOutputToZip(
			ctx,
			ps,
			filepath.Join(linuxUtilsRoot, "top.txt"),
			"top", "-b", "-n", "10", "-H", "-d", "1",
		)
	}
}

// Saves the output of `vmstat`.
func saveVmstat(ctx context.Context, ps *stepParams) step {
	return func() error {
		return writeCommandOutputToZip(
			ctx,
			ps,
			filepath.Join(linuxUtilsRoot, "vmstat.txt"),
			"vmstat", "-w", "1", "10",
		)
	}
}

// Saves the output of `ip addr`.
func saveIP(ctx context.Context, ps *stepParams) step {
	return func() error {
		return writeCommandOutputToZip(
			ctx,
			ps,
			filepath.Join(linuxUtilsRoot, "ip.txt"),
			"ip", "addr",
		)
	}
}

// Saves the output of `lspci`.
func saveLspci(ctx context.Context, ps *stepParams) step {
	return func() error {
		return writeCommandOutputToZip(
			ctx,
			ps,
			filepath.Join(linuxUtilsRoot, "lspci.txt"),
			"lspci",
		)
	}
}

// Saves the output of `dmidecode`.
func saveDmidecode(ctx context.Context, ps *stepParams) step {
	return func() error {
		return writeCommandOutputToZip(
			ctx,
			ps,
			filepath.Join(linuxUtilsRoot, "dmidecode.txt"),
			"dmidecode",
		)
	}
}

// Saves the output of `sysctl -a`.
func saveSysctl(ctx context.Context, ps *stepParams) step {
	return func() error {
		return writeCommandOutputToZip(
			ctx,
			ps,
			filepath.Join(linuxUtilsRoot, "sysctl.txt"),
			"sysctl", "-a",
		)
	}
}

// Saves the output of `free`.
func saveFree(ctx context.Context, ps *stepParams) step {
	return func() error {
		return writeCommandOutputToZip(
			ctx,
			ps,
			filepath.Join(linuxUtilsRoot, "free.txt"),
			"free",
		)
	}
}

// fileSize is an auxiliary type that contains the path and size of a file.
type fileSize struct {
	path string
	size int64
}

// walkSizeDir walks the directory tree rooted at the given path and calculates
// the size of each file in bytes. It also excludes files whose names match
// the given regular expression 'exclude'.
//
// The function returns a slice of fileSize struct, each of which contains the
// path and size of a file, as well as the total size of the directory in bytes.
func walkSizeDir(dir string, exclude *regexp.Regexp) (files []fileSize, size int64, err error) {
	err = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			if !exclude.MatchString(info.Name()) {
				size += info.Size()
				files = append(files, fileSize{
					path: path,
					size: info.Size(),
				})
			}
		}
		return nil
	})
	return files, size, err
}

// sortControllerLogDir takes a slice of fileSize structs that represents the
// controller logs directory and sorts it by the base_offset and term integers
// in the filenames. Filenames should follow this format:
// {base_offset}-{term}-{version}. If multiple files have the same base_offset,
// then the function will sort them based on their term.
func sortControllerLogDir(dir []fileSize) {
	// Will match the controller log filename with the form
	// {base_offset}-{term}-{version}
	//   - index 0: the full match.
	//   - index 1: base_offset.
	//   - index 2: term.
	offsetRE := regexp.MustCompile(`^([0-9]{1,16})-([0-9]{1,16})-v[0-9].log$`)

	sort.Slice(dir, func(i, j int) bool {
		filename1 := filepath.Base(dir[i].path)
		filename2 := filepath.Base(dir[j].path)

		f1 := offsetRE.FindStringSubmatch(filename1)
		f2 := offsetRE.FindStringSubmatch(filename2)

		// One of the filenames is corrupted and don't follow the pattern
		// {base_offset}-{term}-{version}. We want those files to be at the
		// head.
		if len(f1) == 0 {
			if len(f2) == 0 {
				// If both are corrupted, sort alphabetically.
				return filename1 < filename2
			}
			return true
		}
		if len(f2) == 0 {
			return false
		}

		// Here, we parse the base_offset. Any errors can be safely ignored
		// because if it is a controller log, the string will be parsed
		// correctly, otherwise, the value will be 0 and the slice won't be
		// sorted.
		offset1, _ := strconv.Atoi(f1[1])
		offset2, _ := strconv.Atoi(f2[1])

		// If the base offsets are different, sort by base_offset.
		if offset1 != offset2 {
			return offset1 < offset2
		}

		// If they are the same, we sort based on the term
		term1, _ := strconv.Atoi(f1[2])
		term2, _ := strconv.Atoi(f2[2])

		return term1 < term2
	})
}

// sliceControllerDir takes a slice of fileSize structs and a byte size limit
// (logLimitBytes). It returns a slice with the files that fit within the limit,
// copied from both the head and tail of the input slice.
func sliceControllerDir(cFiles []fileSize, logLimitBytes int64) (slice []fileSize) {
	// We start copying the files from the head until we reach the first half of
	// the limit:
	var headSize int64
	half := logLimitBytes / 2
	for _, cLog := range cFiles {
		if headSize+cLog.size > half {
			break
		}
		slice = append(slice, cLog)
		headSize += cLog.size
	}

	// Now from the tail until we fill the remaining bytes:
	var tailSize int64
	// We don't use half since headSize could be < than half.
	remainingBytes := logLimitBytes - headSize
	for i, alreadyTaken := len(cFiles)-1, len(slice); i > alreadyTaken; i-- {
		cLog := cFiles[i]

		if tailSize+cLog.size > remainingBytes {
			break
		}
		slice = append(slice, cLog)
		tailSize += cLog.size
	}
	return slice
}

func saveControllerLogDir(ps *stepParams, y *config.RedpandaYaml, logLimitBytes int) step {
	return func() error {
		if y.Redpanda.Directory == "" {
			return fmt.Errorf("failed to save controller logs: 'redpanda.data_directory' is empty on the provided configuration file")
		}
		controllerDir := filepath.Join(y.Redpanda.Directory, "redpanda", "controller", "0_0")

		// We don't need the .base_index files to parse out the messages.
		exclude := regexp.MustCompile(`^*.base_index$`)
		cFiles, size, err := walkSizeDir(controllerDir, exclude)
		if err != nil {
			return fmt.Errorf("unable to save controller logs: %v", err)
		}

		// Our decoding tools look for the base of the data directory, and it
		// searches for the expected directory: redpanda/controller/0_0. If we
		// use this folder structure, we will make the life easier to the users
		// who wish to decode the controller logs using our tools.
		baseDestDir := filepath.Join("controller-logs", "redpanda", "controller", "0_0")
		if int(size) < logLimitBytes {
			return writeDirToZip(ps, controllerDir, baseDestDir, exclude)
		}

		fmt.Printf("WARNING: controller logs directory size is too big (%v). Saving a slice of the logs; you can adjust the limit by changing --controller-logs-size-limit flag\n", units.HumanSize(float64(size)))

		// If the total size of the logs exceeds the specified limit, we will
		// reduce the size of the controller log directory. Specifically, we
		// will keep the first and last 'limit/2' bytes of the log files,
		// discarding the middle section to bring the total size of the logs
		// under the limit.
		sortControllerLogDir(cFiles)
		slice := sliceControllerDir(cFiles, int64(logLimitBytes))

		for _, cLog := range slice {
			file, err := os.ReadFile(cLog.path)
			if err != nil {
				return fmt.Errorf("unable to save controller logs: %v", err)
			}
			err = writeFileToZip(ps, filepath.Join(baseDestDir, filepath.Base(cLog.path)), file)
			if err != nil {
				return fmt.Errorf("unable to save controller logs: %v", err)
			}
		}
		return nil
	}
}

func walkDir(root string, files map[string]*fileInfo) error {
	return filepath.WalkDir(
		root,
		func(path string, d fs.DirEntry, readErr error) error {
			// Prevent infinite loops.
			if _, exists := files[path]; exists {
				return nil
			}

			i := new(fileInfo)
			files[path] = i

			// If the directory's contents couldn't be read, skip it.
			if readErr != nil {
				i.Error = readErr.Error()
				return fs.SkipDir
			}

			info, err := d.Info()
			if err != nil {
				i.Error = err.Error()
				// If reading a directory failed, then skip it altogether.
				if d.IsDir() {
					return fs.SkipDir
				}
				// If it's just a file, just return and move to the
				// next entry.
				return nil
			}

			bSize := info.Size()
			i.SizeBytes = bSize
			i.Size = units.HumanSize(float64(bSize))
			i.Mode = info.Mode().String()
			i.Modified = info.ModTime().String()

			// The user and group are only available through the
			// underlying syscall object.
			sys, ok := info.Sys().(*syscall.Stat_t)
			if ok {
				u, err := user.LookupId(fmt.Sprint(sys.Uid))
				if err == nil {
					i.User = u.Name
				} else {
					i.User = fmt.Sprintf("user lookup failed for UID %d: %v", sys.Uid, err)
				}
				g, err := user.LookupGroupId(fmt.Sprint(sys.Gid))
				if err == nil {
					i.Group = g.Name
				} else {
					i.Group = fmt.Sprintf("group lookup failed for GID %d: %v", sys.Gid, err)
				}
			}

			// If it's a symlink, save the dir or file it points to.
			// If the file it points to is a directory, follow it and then
			// call `walk` using it as the root.
			isSymlink := info.Mode().Type()&fs.ModeSymlink != 0
			if !isSymlink {
				return nil
			}

			dest, err := os.Readlink(path)
			if err != nil {
				i.Symlink = "unresolvable"
				i.Error = err.Error()
			}
			i.Symlink = dest

			fInfo, err := os.Stat(dest)
			if err != nil {
				files[dest] = &fileInfo{Error: err.Error()}
			} else if fInfo.IsDir() {
				return walkDir(dest, files)
			}

			return nil
		},
	)
}
