// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// Package plugin contains functions to load plugin information from a
// filesystem.
package plugin

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/httpapi"
	rpkos "github.com/redpanda-data/redpanda/src/go/rpk/pkg/os"
	"github.com/spf13/afero"
)

// FlagAutoComplete is the expected flag if a plugin has the rpk.ac- name
// prefix.
const FlagAutoComplete = "--help-autocomplete"

type pluginName struct {
	autocomplete bool
	managed      bool
	isPlugin     bool
	rest         string
}

func (p pluginName) filename() string {
	name := ".rpk"
	if p.autocomplete {
		name += ".ac"
	} else if p.managed {
		name += ".managed"
	}
	name += "-" + p.rest
	return name
}

// 0: whole match
// 1: ".managed" or ".ac", if present
// 2: rest of the name
var reName = regexp.MustCompile(`^.rpk(\.ac|\.managed)?-(.+)`)

func calcName(name string) pluginName {
	m := reName.FindStringSubmatch(name)
	if len(m) == 0 {
		return pluginName{}
	}
	return pluginName{
		isPlugin:     true,
		autocomplete: m[1] == ".ac",
		managed:      m[1] == ".managed",
		rest:         m[2],
	}
}

// IsAutoComplete returns whether the given basename of a file is an
// autocomplete plugin.
func IsAutoComplete(name string) bool {
	return calcName(filepath.Base(name)).autocomplete
}

// IsManaged returns whether the given basename of a file is a managed plugin.
func IsManaged(name string) bool {
	return calcName(filepath.Base(name)).managed
}

// IsSamePluginPath returns true if an old plugin path is the "same" as a new
// plugin path. Two paths are the same if they live in the same directory and
// have the same non-prefix name. The prefix is ignored.
func IsSamePluginPath(oldPath, newPath string) bool {
	ldir, rdir := filepath.Dir(oldPath), filepath.Dir(newPath)
	lbase, rbase := filepath.Base(oldPath), filepath.Base(newPath)
	l, r := calcName(lbase), calcName(rbase)
	return ldir == rdir && l.rest == r.rest
}

// Plugin groups information about a plugin's location on disk with the
// arguments to call the plugin.
//
// Plugins are searched in path order, and any unique name that comes first
// shadows any duplicate names.
//
// If a plugin filepath is /foo/bar/rpk.ac-baz-boz_biz, then the arguments will
// be baz-boz biz.
type Plugin struct {
	// Name is the original name of the plugin.
	Name string

	// Path is the path to the plugin binary on disk.
	Path string

	// Arguments is the set of arguments that should be used to call this
	// plugin.
	Arguments []string

	// ShadowedPaths are other plugin filepaths that are shadowed by this
	// plugin.
	ShadowedPaths []string

	// Managed returns whether this is an rpk internally managed plugin
	// and should not auto-install any help.
	Managed bool
}

// FullName returns the full name of the plugin, joining the arguments with
// "_".
func (p *Plugin) FullName() string { return strings.Join(p.Arguments, "_") }

// Plugins is a handy alias for a slice of plugins.
type Plugins []Plugin

// Sort sorts a slice of plugins.
func (ps Plugins) Sort() {
	sort.Slice(ps, func(i, j int) bool { return ps[i].Name < ps[j].Name })
}

// Find returns the given plugin if it exists.
func (ps Plugins) Find(name string) (*Plugin, bool) {
	for _, p := range ps {
		if p.FullName() == name {
			return &p, true
		}
	}
	return nil, false
}

// NameToArgs converts a plugin command name into its arguments.
func NameToArgs(command string) []string {
	return strings.Split(command, "_")
}

// DefaultBinPath returns HOME-DIR/.local/bin where HOME-DIR is the directory
// returned by os.UserHomeDir.
func DefaultBinPath() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("unable to get your home directory: %v", err)
	}
	return filepath.Join(home, ".local", "bin"), nil
}

// UserPaths returns the user PATH list where the plugin may live.
func UserPaths() []string {
	defaultPath, err := DefaultBinPath()
	pathList := filepath.SplitList(os.Getenv("PATH"))
	if err != nil {
		// If there is an error getting the default bin path we will only look
		// for the binary in the $PATH.
		return pathList
	}
	return append([]string{defaultPath}, pathList...)
}

// ListPlugins returns all plugins found in fs across the given search
// directories. The returned plugins are ordered by the raw args it
// would take to execute them: [baz, baz_biz, foo, foo_bar].
func ListPlugins(fs afero.Fs, searchDirs []string) Plugins {
	searchDirs = uniqueTrimmedStrs(searchDirs)

	uniquePlugins := make(map[string]int) // plugin name (e.g., mm3 or cloud) => index into plugins

	type rawNamePlugin struct {
		Plugin
		rawName string
	}
	var plugins []rawNamePlugin
	for _, searchDir := range searchDirs {
		infos, err := afero.ReadDir(fs, searchDir)
		if err != nil {
			continue // unable to read dir; skip
		}
		for _, info := range infos {
			if info.IsDir() {
				continue
			}

			name := calcName(info.Name())
			if !name.isPlugin {
				continue
			}
			if info.Mode()&0o111 == 0 {
				continue // not executable; skip
			}

			arguments := NameToArgs(name.rest)

			fullPath := filepath.Join(searchDir, info.Name())
			priorAt, exists := uniquePlugins[name.rest]
			if exists {
				prior := &plugins[priorAt]
				prior.ShadowedPaths = append(prior.ShadowedPaths, fullPath)
				continue
			}

			uniquePlugins[name.rest] = len(plugins)
			plugins = append(plugins, rawNamePlugin{
				Plugin: Plugin{
					Name:      arguments[0],
					Path:      fullPath,
					Arguments: arguments,
					Managed:   name.managed,
				},
				rawName: name.rest,
			})
		}
	}

	sort.Slice(plugins, func(i, j int) bool {
		return plugins[i].rawName < plugins[j].rawName
	})

	r := make([]Plugin, len(plugins))
	for i, p := range plugins {
		r[i] = p.Plugin
	}
	return r
}

// Returns the unique strings in `in`, preserving order.
//
// Order preservation is important for search paths: a higher priority plugin
// (path search wise) will shadow a lower priority one.
func uniqueTrimmedStrs(in []string) []string {
	seen := make(map[string]bool)
	keep := in[:0]
	for i := 0; i < len(in); i++ {
		path := in[i]
		path = strings.TrimSpace(path)
		if seen[path] || len(path) == 0 {
			continue
		}
		seen[path] = true
		keep = append(keep, path)
	}
	return keep
}

// Sha256Path returns the sha256sum for the file at path.
//
// The intent of this function is to shasum plugin binaries.
func Sha256Path(fs afero.Fs, path string) (string, error) {
	f, err := fs.Open(path)
	if err != nil {
		return "", fmt.Errorf("unable to open %q: %v", path, err)
	}

	h := sha256.New()
	if _, err = io.Copy(h, f); err != nil {
		return "", fmt.Errorf("unable to sha256sum %q: %v", path, err)
	}

	return hex.EncodeToString(h.Sum(nil)[:]), nil
}

// WriteBinary writes a plugin with the given name into the destination
// directory with the provided binary contents.
//
// This returns the filepath that was written, or an error if any step of the
// process fails. If the process fails after the binary has been written to a
// temporary directory, the file is left on disk for user inspection.
func WriteBinary(fs afero.Fs, name, dstDir string, contents []byte, autocomplete, managed bool) (string, error) {
	dstBase := (pluginName{
		autocomplete: autocomplete,
		managed:      managed,
		isPlugin:     true,
		rest:         name,
	}).filename()
	dst := filepath.Join(dstDir, dstBase)
	return dst, rpkos.ReplaceFile(fs, dst, contents, 0o755)
}

// Download downloads a plugin at the given URL and ensures that the shasum of
// the binary is as expected either before or after decompression / untarring.
//
// If the url ends in ".gz", this unzips the binary before shasumming. If the
// url ends in ".tar.gz", this unzips, then untars ONE file, then shasums.
func Download(ctx context.Context, url string, isKnownCompressed bool, expShaPrefix string) ([]byte, error) {
	cl := httpapi.NewClient(
		httpapi.HTTPClient(&http.Client{
			Timeout: 100 * time.Second,
		}),
	)

	var raw []byte
	if err := cl.Get(ctx, url, nil, &raw); err != nil {
		return nil, fmt.Errorf("unable to download plugin: %w", err)
	}

	plugin := io.Reader(bytes.NewReader(raw))
	if strings.HasSuffix(url, ".gz") || isKnownCompressed {
		gzr, err := gzip.NewReader(plugin)
		if err != nil {
			return nil, fmt.Errorf("unable to create gzip reader: %w", err)
		}
		defer gzr.Close()
		plugin = gzr
		url = strings.TrimSuffix(url, ".gz")
	}
	if strings.HasSuffix(url, ".tar") {
		untar := tar.NewReader(plugin)
		if _, err := untar.Next(); err != nil {
			return nil, fmt.Errorf("unable to advance to first tar file: %w", err)
		}
		plugin = untar
	}

	var err error
	if raw, err = io.ReadAll(plugin); err != nil {
		return nil, fmt.Errorf("unable to read plugin: %w", err)
	}

	shasum := sha256.Sum256(raw)
	gotSha := strings.ToLower(hex.EncodeToString(shasum[:]))
	expShaPrefix = strings.ToLower(expShaPrefix)

	if !strings.HasPrefix(gotSha, expShaPrefix) {
		return nil, fmt.Errorf("checksum of plugin %s does not contain expected plugin %s", gotSha, expShaPrefix)
	}

	return raw, nil
}
