// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

//go:build linux

package debug

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
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/avast/retry-go"
	"github.com/beevik/ntp"
	"github.com/docker/go-units"
	"github.com/hashicorp/go-multierror"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/api/admin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	osutil "github.com/redpanda-data/redpanda/src/go/rpk/pkg/os"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/system"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/system/syslog"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"gopkg.in/yaml.v3"
)

func executeBundle(
	ctx context.Context,
	fs afero.Fs,
	conf *config.Config,
	cl *kgo.Client,
	admin *admin.AdminAPI,
	logsSince, logsUntil string,
	logsLimitBytes int,
	timeout time.Duration,
) error {
	mode := os.FileMode(0o755)
	timestamp := time.Now().Unix()
	filename := fmt.Sprintf("%d-bundle.zip", timestamp)
	f, err := fs.OpenFile(
		filename,
		os.O_CREATE|os.O_WRONLY,
		mode,
	)
	if err != nil {
		return fmt.Errorf("couldn't create bundle file: %w", err)
	}
	defer f.Close()

	grp := multierror.Group{}

	w := zip.NewWriter(f)
	defer w.Close()

	ps := &stepParams{
		fs:      fs,
		w:       w,
		timeout: timeout,
	}

	steps := []step{
		saveKafkaMetadata(ctx, ps, cl),
		saveDataDirStructure(ps, conf),
		saveConfig(ps, conf),
		saveCPUInfo(ps),
		saveInterrupts(ps),
		saveResourceUsageData(ps, conf),
		saveNTPDrift(ps),
		saveSyslog(ps),
		savePrometheusMetrics(ctx, ps, admin),
		saveDNSData(ctx, ps),
		saveDiskUsage(ctx, ps, conf),
		saveLogs(ctx, ps, logsSince, logsUntil, logsLimitBytes),
		saveSocketData(ctx, ps),
		saveTopOutput(ctx, ps),
		saveVmstat(ctx, ps),
		saveIP(ctx, ps),
		saveLspci(ctx, ps),
		saveDmidecode(ctx, ps),
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
		log.Info(errs.Error())
	}

	log.Infof("Debug bundle saved to '%s'", filename)
	return nil
}

type step func() error

type stepParams struct {
	fs      afero.Fs
	m       sync.Mutex
	w       *zip.Writer
	timeout time.Duration
}

type fileInfo struct {
	Size     string `json:"size"`
	Mode     string `json:"mode"`
	Symlink  string `json:"symlink,omitempty"`
	Error    string `json:"error,omitempty"`
	Modified string `json:"modified"`
	User     string `json:"user"`
	Group    string `json:"group"`
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

	wr, err := ps.w.Create(filename)
	if err != nil {
		return err
	}
	_, err = wr.Write(contents)
	if err != nil {
		return fmt.Errorf("couldn't save '%s': %w", filename, err)
	}
	return nil
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

	wr, err := ps.w.Create(filename)
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
			return fmt.Errorf("couldn't save '%s': %w", filename, err)
		}
		log.Debugf(
			"Got '%v' while running '%s'. This is probably due to the"+
				" command's output exceeding its limit in bytes.",
			err,
			cmd,
		)
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
		log.Debug("Reading Kafka information")

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
func saveDataDirStructure(ps *stepParams, conf *config.Config) step {
	return func() error {
		files := make(map[string]*fileInfo)
		err := walkDir(conf.Redpanda.Directory, files)
		if err != nil {
			return fmt.Errorf("couldn't walk '%s': %w", conf.Redpanda.Directory, err)
		}
		bs, err := json.Marshal(files)
		if err != nil {
			return fmt.Errorf(
				"couldn't encode the '%s' directory structure as JSON: %w",
				conf.Redpanda.Directory,
				err,
			)
		}
		return writeFileToZip(ps, "data-dir.txt", bs)
	}
}

// Writes the config file to the bundle, redacting SASL credentials.
func saveConfig(ps *stepParams, conf *config.Config) step {
	return func() error {
		// Redact SASL credentials
		redacted := "(REDACTED)"
		if conf.Rpk.KafkaAPI.SASL != nil {
			conf.Rpk.KafkaAPI.SASL.User = redacted
			conf.Rpk.KafkaAPI.SASL.Password = redacted
		}
		if conf.Rpk.SASL != nil {
			conf.Rpk.SASL.User = redacted
			conf.Rpk.SASL.Password = redacted
		}
		// We want to redact any blindly decoded parameters.
		redactOtherMap(conf.Other)
		redactOtherMap(conf.Redpanda.Other)
		redactServerTLSSlice(conf.Redpanda.RPCServerTLS)
		redactServerTLSSlice(conf.Redpanda.KafkaAPITLS)
		redactServerTLSSlice(conf.Redpanda.AdminAPITLS)
		if conf.SchemaRegistry != nil {
			for _, server := range conf.SchemaRegistry.SchemaRegistryAPITLS {
				redactOtherMap(server.Other)
			}
		}
		if conf.Pandaproxy != nil {
			redactOtherMap(conf.Pandaproxy.Other)
			redactServerTLSSlice(conf.Pandaproxy.PandaproxyAPITLS)
		}
		if conf.PandaproxyClient != nil {
			redactOtherMap(conf.PandaproxyClient.Other)
		}
		if conf.SchemaRegistryClient != nil {
			redactOtherMap(conf.SchemaRegistryClient.Other)
		}

		bs, err := yaml.Marshal(conf)
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

// Writes a file containing memory, disk & CPU usage metrics for a local
// redpanda process.
func saveResourceUsageData(ps *stepParams, conf *config.Config) step {
	return func() error {
		res, err := system.GatherMetrics(ps.fs, ps.timeout, *conf)
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
				log.Debugf("Couldn't retrieve NTP data from %s: %v", host, err)
				log.Debugf("Retrying (%d retries left)", retries-n)
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
			"ntp.txt",
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
		return writeFileToZip(ps, "syslog.txt", entries)
	}
}

// Queries the given admin API address for prometheus metrics.
func savePrometheusMetrics(ctx context.Context, ps *stepParams, admin *admin.AdminAPI) step {
	return func() error {
		raw, err := admin.PrometheusMetrics(ctx)
		if err != nil {
			return fmt.Errorf("unable to fetch metrics from the admin API: %w", err)
		}
		return writeFileToZip(ps, "prometheus-metrics.txt", raw)
	}
}

// Saves the output of `dig`.
func saveDNSData(ctx context.Context, ps *stepParams) step {
	return func() error {
		return writeCommandOutputToZip(ctx, ps, "dig.txt", "dig")
	}
}

// Saves the disk usage total within redpanda's data directory.
func saveDiskUsage(ctx context.Context, ps *stepParams, conf *config.Config) step {
	return func() error {
		return writeCommandOutputToZip(
			ctx,
			ps,
			"du.txt",
			"du", "-h", conf.Redpanda.Directory,
		)
	}
}

// TODO: What if running inside a container/ k8s?
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
		return writeCommandOutputToZip(ctx, ps, "ss.txt", "ss")
	}
}

// Saves the output of `top`.
func saveTopOutput(ctx context.Context, ps *stepParams) step {
	return func() error {
		return writeCommandOutputToZip(
			ctx,
			ps,
			"top.txt",
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
			"vmstat.txt",
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
			"ip.txt",
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
			"lspci.txt",
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
			"dmidecode.txt",
			"dmidecode",
		)
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

			i.Size = units.HumanSize(float64(info.Size()))
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
