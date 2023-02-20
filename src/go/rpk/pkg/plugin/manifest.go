// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package plugin

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"sort"
	"time"

	"gopkg.in/yaml.v3"
)

// Manifest represents a plugin manifest, which is essentially a list of
// plugins available for install.
type Manifest struct {
	// Version is a YYYY-MM-DD version of the manifest. We use this to
	// determine how to parse the manifest. We currently only understand
	// one version; if the version does not match what we expect, then we
	// know that rpk needs to be updated.
	Version string `yaml:"api_version"`

	// Plugins contains all plugins available for downloading.
	Plugins []ManifestPlugin `yaml:"plugins"`
}

// FindEntry looks for a plugin with the given name and returns it.
func (m *Manifest) FindEntry(name string) (ManifestPlugin, error) {
	for _, p := range m.Plugins {
		if p.Name == name {
			return p, nil
		}
	}
	return ManifestPlugin{}, fmt.Errorf("unable to find plugin %q", name)
}

// The client we use for downloading plugin manifests and plugins. At most, we
// may increase the timeout or make it configurable later.
var client = &http.Client{Timeout: 30 * time.Second}

// DownloadManifest downloads a plugin manifest at the given URL and returns
// its parsed form.
//
// This returns an error if the download fails at all or if we do not know how
// to parse the manifest. For the latter, the user's rpk must be out of date,
// and the returned error indicates they should update.
func DownloadManifest(url string) (*Manifest, error) {
	req, err := http.NewRequestWithContext(
		context.Background(),
		http.MethodGet,
		url,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("unable to create manifest request: %v", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("unable to request manifest: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("unable to read manifest body: %v", err)
	}
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("unsuccessful manifest response %s: %q", http.StatusText(resp.StatusCode), body)
	}
	var m Manifest
	if err := yaml.Unmarshal(body, &m); err != nil {
		return nil, fmt.Errorf("unable to decode manifest body: %v", err)
	}

	const understand = "2021-07-27"
	if m.Version != understand {
		return nil, fmt.Errorf("plugin manifest indicates version %q, and we can only understand %q; please update rpk", m.Version, understand)
	}

	sort.Slice(m.Plugins, func(i, j int) bool {
		return m.Plugins[i].Name < m.Plugins[j].Name
	})

	return &m, nil
}

// ManifestPlugin is an entry of a plugin available to install.
type ManifestPlugin struct {
	// Name is the name of a plugin.
	Name string `yaml:"name"`

	// Description is a short one-liner description of the plugin suitable
	// for output to a console.
	Description string `yaml:"description"`

	// Path is the base request path of the plugin, such as
	// plugins/vectorized/cloud.
	Path string `yaml:"path"`

	// Compression indicates what type of compression is used on the plugin
	// binary. We currently only understand either no compression ("") or
	// "gzip".
	Compression string `yaml:"compression"`

	// HelpAutoComplete indicates whether the plugin supports the
	// --help-autocomplete flag.
	HelpAutoComplete bool `yaml:"help_autocomplete"`

	// OSArchShas contains the binaries that are supported per
	// ${os}_${arch}, and their sha256 sums if we download and decompressed
	// the binary.
	OSArchShas map[string]string `yaml:"os_arch_shas"`
}

// PathShaForUser is a shortcut for PathSha for the current user's os / arch.
func (p *ManifestPlugin) PathShaForUser() (path, sha string, err error) {
	return p.PathSha(runtime.GOOS, runtime.GOARCH)
}

// PathSha returns the download path and the sha256 sum of the binary for the
// given OS and arch. If the path does not exist (i.e. the os / arch is
// unsupported), this returns an error.
func (p *ManifestPlugin) PathSha(
	os, arch string,
) (path, sha string, err error) {
	if p.Name == "" {
		return "", "", errors.New("invalid plugin has no name")
	}
	if p.Path == "" {
		return "", "", fmt.Errorf("plugin %s is missing a download path", p.Name)
	}
	if p.OSArchShas == nil {
		return "", "", fmt.Errorf("plugin %s has no os / arch's to evaluate for downloading", p.Name)
	}

	need := fmt.Sprintf("%s_%s", os, arch)
	sha, ok := p.OSArchShas[need]
	if !ok {
		return "", "", fmt.Errorf("plugin %s: unable to find binary to download for os / arch %s", p.Name, need)
	}

	return fmt.Sprintf("/%s/%s/%s/%s", p.Path, need, sha, p.Name), sha, nil
}

// DownloadForUser is a shortcut for Download for the current user's
// os / arch.
func (p *ManifestPlugin) DownloadForUser(baseURL string) ([]byte, error) {
	return p.Download(baseURL, runtime.GOOS, runtime.GOARCH)
}

// Download downloads and decompresses the plugin represented by this manifest
// entry.
func (p *ManifestPlugin) Download(baseURL, os, host string) ([]byte, error) {
	var decompress bool
	switch p.Compression {
	case "":
		// do nothing
	case "gzip":
		decompress = true
	default:
		return nil, fmt.Errorf("plugin uses compression %q, which is not supported", p.Compression)
	}

	path, sha, err := p.PathSha(os, host)
	if err != nil {
		return nil, err // error already annotated
	}

	u := baseURL + path
	return Download(context.Background(), u, decompress, sha)
}
