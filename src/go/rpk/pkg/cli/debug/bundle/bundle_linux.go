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
	"bufio"
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
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/debug/common"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	osutil "github.com/redpanda-data/redpanda/src/go/rpk/pkg/os"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/system"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/system/syslog"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/utils"
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
			path = fmt.Sprintf("%v-%d-bundle.zip", common.SanitizeName(rp.Redpanda.AdvertisedRPCAPI.Address), timestamp)
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
		fs:        bp.fs,
		w:         w,
		timeout:   bp.timeout,
		fileRoot:  strings.TrimSuffix(filepath.Base(bp.path), ".zip"),
		sharedBuf: make([]byte, 32*1024), // buffer for shared use. 32 KiB is the default used in io.Copy.
	}

	addrs := bp.y.Rpk.AdminAPI.Addresses

	steps := []step{
		saveCPUInfo(ps),
		saveClusterAdminAPICalls(ctx, ps, bp.fs, bp.p, addrs, bp.partitions, bp.connectionLimit),
		saveCmdLine(ps),
		saveConfig(ps, bp.y),
		saveControllerLogDir(ps, bp.y, bp.controllerLogLimitBytes),
		saveCrashReports(ps, bp.y),
		saveDNSData(ctx, ps),
		saveDataDirStructure(ps, bp.y),
		saveDf(ctx, ps),
		saveDiskUsage(ctx, ps, bp.y),
		saveDmidecode(ctx, ps),
		saveFree(ctx, ps),
		saveIP(ctx, ps),
		saveEthtool(ctx, ps),
		saveInterrupts(ps),
		saveKafkaMetadata(ctx, ps, bp.cl),
		saveKernelSymbols(ps),
		saveLogs(ctx, ps, bp.logsSince, bp.logsUntil, bp.logsLimitBytes),
		saveLspci(ctx, ps),
		saveLsblk(ctx, ps),
		saveMdstat(ps),
		saveMountedFilesystems(ps),
		saveNTPDrift(ps),
		saveResourceUsageData(ps, bp.y),
		saveSingleAdminAPICalls(ctx, ps, bp.fs, bp.p, addrs, bp.cpuProfilerWait),
		saveMetricsAPICalls(ctx, ps, bp.fs, bp.p, addrs, bp.metricsInterval, bp.metricsSampleCount),
		saveStartupLog(ps, bp.y),
		saveSlabInfo(ps),
		saveSocketData(ctx, ps),
		saveSoftwareInterrupts(ps),
		saveSysctl(ctx, ps),
		saveSyslog(ps),
		saveTopOutput(ctx, ps),
		saveUname(ctx, ps),
		saveUptime(ctx, ps),
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
		errs.ErrorFormat = errorFormat
		fmt.Println(errs.Error())
	}

	fmt.Printf("Debug bundle saved to '%s'\n", bp.path)
	return nil
}

func errorFormat(errs []error) string {
	if len(errs) == 1 {
		return fmt.Sprintf("Debug bundle successfully generated. 1 diagnostic failed:\n\t* %s\n\n", errs[0])
	}

	points := make([]string, len(errs))
	for i, err := range errs {
		points[i] = fmt.Sprintf("* %s", err)
	}

	return fmt.Sprintf(
		"Debug bundle successfully generated. %d diagnostics failed:\n\t%s\n\n",
		len(errs), strings.Join(points, "\n\t"))
}

type step func() error

type stepParams struct {
	fs        afero.Fs
	m         sync.Mutex
	w         *zip.Writer
	timeout   time.Duration
	fileRoot  string
	sharedBuf []byte // shared buffer for writing to zip files.
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

// writeStreamToZip creates a file in the zip writer with name 'filename' and
// streams data from reader to it.
func writeStreamToZip(ps *stepParams, filename string, reader io.Reader) error {
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
	_, err = io.CopyBuffer(wr, reader, ps.sharedBuf)
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
			file, err := ps.fs.Open(filepath.Join(srcDir, filename))
			if err != nil {
				return err
			}
			err = writeStreamToZip(ps, filepath.Join(destDir, filename), file)
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

		adm := kadm.NewClient(cl)
		defer adm.Close()

		// We stream to a temporary file, and then write that file to the zip.
		// This to avoid:
		//    1. Keeping the whole response in memory, which can be large.
		//    2. Locking the zip writer for the whole duration of the request,
		//       which can be long.
		tempFile, err := afero.TempFile(ps.fs, "", "kafka-admin-*")
		if err != nil {
			return err
		}
		defer func() {
			// Best effort cleanup of the temporary file.
			if err := ps.fs.Remove(tempFile.Name()); err != nil {
				zap.L().Sugar().Errorf("unable to remove temporary file %s: %v", tempFile.Name(), err)
			}
		}()

		bufw := bufio.NewWriter(tempFile)
		if _, err := bufw.WriteString("["); err != nil {
			return err
		}
		encoder := json.NewEncoder(bufw)
		first := true
		encode := func(v any) error {
			if !first {
				if _, err := bufw.WriteString(","); err != nil {
					return err
				}
			}
			first = false
			return encoder.Encode(v)
		}

		meta, err := adm.Metadata(ctx)
		if e := encode(resp{"metadata", meta, stringifyKadmErr(err)}); e != nil {
			return fmt.Errorf("unable to encode metadata response: %v", e)
		}

		tcs, err := adm.DescribeTopicConfigs(ctx, meta.Topics.Names()...)
		if e := encode(resp{"topic_configs", tcs, stringifyKadmErr(err)}); e != nil {
			return fmt.Errorf("unable to encode topic configs response: %v", e)
		}

		bcs, err := adm.DescribeBrokerConfigs(ctx, meta.Brokers.NodeIDs()...)
		if e := encode(resp{"broker_configs", bcs, stringifyKadmErr(err)}); e != nil {
			return fmt.Errorf("unable to encode broker configs response: %v", e)
		}

		ostart, err := adm.ListStartOffsets(ctx)
		if e := encode(resp{"log_start_offsets", ostart, stringifyKadmErr(err)}); e != nil {
			return fmt.Errorf("unable to encode log start offsets response: %v", e)
		}

		ocommitted, err := adm.ListCommittedOffsets(ctx)
		if e := encode(resp{"last_stable_offsets", ocommitted, stringifyKadmErr(err)}); e != nil {
			return fmt.Errorf("unable to encode log committed offsets response: %v", e)
		}

		oend, err := adm.ListEndOffsets(ctx)
		if e := encode(resp{"high_watermarks", oend, stringifyKadmErr(err)}); e != nil {
			return fmt.Errorf("unable to encode log end offsets response: %v", e)
		}

		groups, err := adm.DescribeGroups(ctx)
		if e := encode(resp{"groups", groups, stringifyKadmErr(err)}); e != nil {
			return fmt.Errorf("unable to encode groups response: %v", e)
		}

		fetched := adm.FetchManyOffsets(ctx, groups.Names()...)
		for _, fetch := range fetched {
			if e := encode(resp{fmt.Sprintf("group_commits_%s", fetch.Group), fetch.Fetched, stringifyKadmErr(fetch.Err)}); e != nil {
				return fmt.Errorf("unable to encode group commits response: %v", e)
			}
		}

		_, err = bufw.WriteString("]")
		if err != nil {
			return fmt.Errorf("unable to write end of JSON array: %v", err)
		}

		if err := bufw.Flush(); err != nil {
			return fmt.Errorf("unable to flush to temporary file: %v", err)
		}
		f, err := ps.fs.Open(tempFile.Name())
		if err != nil {
			return err
		}
		defer f.Close()
		return writeStreamToZip(ps, "kafka.json", f)
	}
}

// Walks the redpanda data directory recursively, and saves to the bundle
// a JSON map where the keys are the file/ dir paths, and the values are
// objects containing their data: size, mode, the file or dir it points to
// if the current file is a symlink, the time it was modified, its owner and
// its group, as well as an error message if reading that specific file failed.
func saveDataDirStructure(ps *stepParams, y *config.RedpandaYaml) step {
	return func() error {
		// We stream to a temporary file, and then write that file to the zip.
		// This to avoid:
		//    1. Keeping the whole directory structure in memory, which can be large.
		//    2. Locking the zip writer for the whole duration of the directory walk,
		//       which can be long.
		tempFile, err := afero.TempFile(ps.fs, "", "data-dir-*")
		if err != nil {
			return err
		}
		defer func() {
			// Best effort cleanup of the temporary file.
			if err := ps.fs.Remove(tempFile.Name()); err != nil {
				zap.L().Sugar().Errorf("unable to remove temporary file %s: %v", tempFile.Name(), err)
			}
		}()

		bufw := bufio.NewWriter(tempFile)
		if _, err := bufw.WriteString("{"); err != nil {
			return err
		}
		encoder := json.NewEncoder(bufw)
		first := true
		encodeEntry := func(path string, info *fileInfo) error {
			if !first {
				if _, err := bufw.WriteString(","); err != nil {
					return err
				}
			}
			first = false

			// Encode the key-value pair manually to match the original format
			pathJSON, err := json.Marshal(path)
			if err != nil {
				return err
			}
			if _, err := bufw.Write(pathJSON); err != nil {
				return err
			}
			if _, err := bufw.WriteString(":"); err != nil {
				return err
			}
			return encoder.Encode(info)
		}

		err = walkDirStreaming(y.Redpanda.Directory, encodeEntry)
		if err != nil {
			return fmt.Errorf("couldn't walk '%s': %w", y.Redpanda.Directory, err)
		}

		_, err = bufw.WriteString("}")
		if err != nil {
			return fmt.Errorf("unable to write end of JSON object: %v", err)
		}

		if err := bufw.Flush(); err != nil {
			return fmt.Errorf("unable to flush to temporary file: %v", err)
		}
		f, err := ps.fs.Open(tempFile.Name())
		if err != nil {
			return err
		}
		defer f.Close()
		return writeStreamToZip(ps, "data-dir.txt", f)
	}
}

// Writes the config file to the bundle, redacting SASL credentials.
func saveConfig(ps *stepParams, y *config.RedpandaYaml) step {
	return func() error {
		yCp, err := createRedpandaConfigCopy(y)
		if err != nil {
			return err
		}
		// Redact SASL credentials
		redacted := "(REDACTED)"
		if yCp.Rpk.KafkaAPI.SASL != nil {
			yCp.Rpk.KafkaAPI.SASL.User = redacted
			yCp.Rpk.KafkaAPI.SASL.Password = redacted
		}
		// We want to redact any blindly decoded parameters.
		redactOtherMap(yCp.Other)
		redactOtherMap(yCp.Redpanda.Other)
		redactServerTLSSlice(yCp.Redpanda.KafkaAPITLS)
		redactServerTLSSlice(yCp.Redpanda.AdminAPITLS)
		if yCp.SchemaRegistry != nil {
			for _, server := range yCp.SchemaRegistry.SchemaRegistryAPITLS {
				redactOtherMap(server.Other)
			}
		}
		if yCp.Pandaproxy != nil {
			redactOtherMap(yCp.Pandaproxy.Other)
			redactServerTLSSlice(yCp.Pandaproxy.PandaproxyAPITLS)
		}
		if yCp.PandaproxyClient != nil {
			redactOtherMap(yCp.PandaproxyClient.Other)
			yCp.PandaproxyClient.SCRAMPassword = &redacted
			yCp.PandaproxyClient.SCRAMUsername = &redacted
		}
		if yCp.SchemaRegistryClient != nil {
			redactOtherMap(yCp.SchemaRegistryClient.Other)
			yCp.SchemaRegistryClient.SCRAMPassword = &redacted
			yCp.SchemaRegistryClient.SCRAMUsername = &redacted
		}

		bs, err := yaml.Marshal(yCp)
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

func createRedpandaConfigCopy(y *config.RedpandaYaml) (*config.RedpandaYaml, error) {
	bs, err := yaml.Marshal(y)
	if err != nil {
		return nil, fmt.Errorf("unable to serialize the loaded redpanda config as YAML: %v", err)
	}
	var cp config.RedpandaYaml
	err = yaml.Unmarshal(bs, &cp)
	if err != nil {
		return nil, fmt.Errorf("unable to decode the redpanda config: %v", err)
	}
	return &cp, nil
}

// Saves the contents of '/proc/cpuinfo'.
func saveCPUInfo(ps *stepParams) step {
	return func() error {
		f, err := ps.fs.Open("/proc/cpuinfo")
		if err != nil {
			return err
		}
		defer f.Close()
		return writeStreamToZip(ps, "proc/cpuinfo", f)
	}
}

// Saves the contents of '/proc/interrupts'.
func saveInterrupts(ps *stepParams) step {
	return func() error {
		f, err := ps.fs.Open("/proc/interrupts")
		if err != nil {
			return err
		}
		defer f.Close()
		return writeStreamToZip(ps, "proc/interrupts", f)
	}
}

// Saves the contents of '/proc/softirqs/'.
func saveSoftwareInterrupts(ps *stepParams) step {
	return func() error {
		f, err := ps.fs.Open("/proc/softirqs")
		if err != nil {
			return err
		}
		defer f.Close()
		return writeStreamToZip(ps, "proc/softirqs", f)
	}
}

// Saves the contents of '/proc/mounts'.
func saveMountedFilesystems(ps *stepParams) step {
	return func() error {
		f, err := ps.fs.Open("/proc/mounts")
		if err != nil {
			return err
		}
		defer f.Close()
		return writeStreamToZip(ps, "proc/mounts", f)
	}
}

// Saves the output of `df -aT`.
func saveDf(ctx context.Context, ps *stepParams) step {
	return func() error {
		return writeCommandOutputToZip(ctx, ps, filepath.Join(linuxUtilsRoot, "df.txt"), "df", "-aT")
	}
}

// Saves the contents of '/proc/slabinfo'. Requires Sudo.
func saveSlabInfo(ps *stepParams) step {
	return func() error {
		f, err := ps.fs.Open("/proc/slabinfo")
		if err != nil {
			if errors.Is(err, fs.ErrPermission) {
				return fmt.Errorf("%v: you may need to run the command as root to read this file", err)
			}
			return err
		}
		defer f.Close()
		return writeStreamToZip(ps, "proc/slabinfo", f)
	}
}

// Saves the contents of '/proc/cmdline'.
func saveCmdLine(ps *stepParams) step {
	return func() error {
		f, err := ps.fs.Open("/proc/cmdline")
		if err != nil {
			return err
		}
		defer f.Close()
		return writeStreamToZip(ps, "proc/cmdline", f)
	}
}

// Saves the contents of '/proc/mdstat'.
func saveMdstat(ps *stepParams) step {
	return func() error {
		f, err := ps.fs.Open("/proc/mdstat")
		if err != nil {
			return err
		}
		defer f.Close()
		return writeStreamToZip(ps, "proc/mdstat", f)
	}
}

// Saves the contents of '/proc/kallsyms'.
func saveKernelSymbols(ps *stepParams) step {
	return func() error {
		f, err := ps.fs.Open("/proc/kallsyms")
		if err != nil {
			return err
		}
		defer f.Close()
		return writeStreamToZip(ps, "proc/kallsyms", f)
	}
}

// Writes a file containing memory, disk & CPU usage metrics for a local
// redpanda process.
func saveResourceUsageData(ps *stepParams, y *config.RedpandaYaml) step {
	return func() error {
		res, err := system.GatherMetrics(ps.fs, 1*time.Second, y)
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

// Saves various ethtool outputs from all interfaces.
func saveEthtool(ctx context.Context, ps *stepParams) step {
	return func() error {
		netDir := "/sys/class/net"
		interfaces := utils.ListFilesInPath(ps.fs, netDir)
		for _, iface := range interfaces {
			if exists, err := afero.Exists(ps.fs, filepath.Join(netDir, iface, "device")); err != nil || !exists {
				// skip virtual interfaces
				continue
			}
			commands := []string{"i", "l", "c"}
			for _, cmd := range commands {
				// ignore errors, some of these commands are very likely expected to fail
				writeCommandOutputToZip(
					ctx,
					ps,
					filepath.Join(linuxUtilsRoot, "ethtool", fmt.Sprintf("%s_%s.txt", iface, cmd)),
					"ethtool", fmt.Sprintf("-%s", cmd), iface,
				)
			}
		}

		return nil
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

// Saves the output of `lsblk --all`.
func saveLsblk(ctx context.Context, ps *stepParams) step {
	return func() error {
		return writeCommandOutputToZip(
			ctx,
			ps,
			filepath.Join(linuxUtilsRoot, "lsblk.txt"),
			"lsblk", "--all",
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

// Saves the output of `uptime`.
func saveUptime(ctx context.Context, ps *stepParams) step {
	return func() error {
		return writeCommandOutputToZip(ctx, ps, filepath.Join(linuxUtilsRoot, "uptime.txt"), "uptime")
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
			f, err := ps.fs.Open(cLog.path)
			if err != nil {
				return fmt.Errorf("unable to save controller logs: %v", err)
			}
			err = writeStreamToZip(ps, filepath.Join(baseDestDir, filepath.Base(cLog.path)), f)
			f.Close()
			if err != nil {
				return fmt.Errorf("unable to save controller logs: %v", err)
			}
		}
		return nil
	}
}

func saveStartupLog(ps *stepParams, y *config.RedpandaYaml) step {
	return func() error {
		if y.Redpanda.Directory == "" {
			return fmt.Errorf("failed to save startup_log: 'redpanda.data_directory' is empty on the provided configuration file")
		}
		path := filepath.Join(y.Redpanda.Directory, "startup_log")
		exists, err := afero.Exists(ps.fs, path)
		if err != nil {
			return fmt.Errorf("failed to save startup_log: unable to check existence of startup_log: %v", err)
		}
		if !exists {
			return fmt.Errorf("skipping startup_log collection: unable to find file %q", path)
		}
		content, err := ps.fs.Open(path)
		if err != nil {
			return fmt.Errorf("failed to save startup_log: unable to read startup_log: %v", err)
		}
		err = writeStreamToZip(ps, "startup_log", content)
		if err != nil {
			return fmt.Errorf("failed to save startup_log: %v", err)
		}
		return nil
	}
}

func saveCrashReports(ps *stepParams, y *config.RedpandaYaml) step {
	return func() error {
		if y.Redpanda.Directory == "" {
			return fmt.Errorf("failed to save crash_reports: 'redpanda.data_directory' is empty on the provided configuration file")
		}
		crashReportDir := filepath.Join(y.Redpanda.Directory, "crash_reports")
		exists, err := afero.Exists(ps.fs, crashReportDir)
		if err != nil {
			return fmt.Errorf("failed to save crash_reports: unable to check existence of the crash_reports directory")
		}
		if !exists {
			return fmt.Errorf("skipping crash_reports collection: directory %q does not exists", crashReportDir)
		}
		err = writeDirToZip(ps, crashReportDir, "crash_reports", nil)
		if err != nil {
			return fmt.Errorf("failed to save crash_reports: %v", err)
		}
		return nil
	}
}

// walkDirStreaming walks the directory tree and streams each file's info
// immediately via the encodeEntry callback, avoiding memory accumulation.
func walkDirStreaming(root string, encodeEntry func(string, *fileInfo) error) error {
	visited := make(map[string]struct{})

	var walkFn func(string) error
	walkFn = func(currentRoot string) error {
		return filepath.WalkDir(
			currentRoot,
			func(path string, d fs.DirEntry, readErr error) error {
				// Prevent infinite loops.
				if _, ok := visited[path]; ok {
					return nil
				}
				visited[path] = struct{}{}

				fileEntry := new(fileInfo)

				// If the directory's contents couldn't be read, skip it.
				if readErr != nil {
					fileEntry.Error = readErr.Error()
					if err := encodeEntry(path, fileEntry); err != nil {
						return err
					}
					return fs.SkipDir
				}

				info, err := d.Info()
				if err != nil {
					fileEntry.Error = err.Error()
					if err := encodeEntry(path, fileEntry); err != nil {
						return err
					}
					// If reading a directory failed, then skip it altogether.
					if d.IsDir() {
						return fs.SkipDir
					}
					// If it's just a file, just return and move to the
					// next entry.
					return nil
				}

				bSize := info.Size()
				fileEntry.SizeBytes = bSize
				fileEntry.Size = units.HumanSize(float64(bSize))
				fileEntry.Mode = info.Mode().String()
				fileEntry.Modified = info.ModTime().String()

				// The user and group are only available through the
				// underlying syscall object.
				if sys, ok := info.Sys().(*syscall.Stat_t); ok {
					u, err := user.LookupId(fmt.Sprint(sys.Uid))
					if err != nil {
						fileEntry.User = fmt.Sprintf("user lookup failed for UID %d: %v", sys.Uid, err)
					} else {
						fileEntry.User = u.Name
					}
					g, err := user.LookupGroupId(fmt.Sprint(sys.Gid))
					if err != nil {
						fileEntry.Group = fmt.Sprintf("group lookup failed for GID %d: %v", sys.Gid, err)
					} else {
						fileEntry.Group = g.Name
					}
				}

				// If it's a symlink, save the dir or file it points to.
				isSymlink := info.Mode().Type()&fs.ModeSymlink != 0
				if isSymlink {
					dest, err := os.Readlink(path)
					if err != nil {
						fileEntry.Symlink = "unresolvable"
						fileEntry.Error = err.Error()
					} else {
						fileEntry.Symlink = dest

						// Stream the destination info if it's not already visited.
						fInfo, err := os.Stat(dest)
						if err != nil {
							if _, ok := visited[dest]; !ok {
								destInfo := &fileInfo{Error: err.Error()}
								if encodeErr := encodeEntry(dest, destInfo); encodeErr != nil {
									return encodeErr
								}
								visited[dest] = struct{}{}
							}
						} else if _, ok := visited[dest]; fInfo.IsDir() && !ok {
							// Recursively walk the symlinked directory.
							if walkErr := walkFn(dest); walkErr != nil {
								return walkErr
							}
						}
					}
				}

				// Stream this entry
				return encodeEntry(path, fileEntry)
			},
		)
	}

	return walkFn(root)
}
