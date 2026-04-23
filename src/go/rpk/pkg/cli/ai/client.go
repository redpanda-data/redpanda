// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package ai

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"runtime"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/httpapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/plugin"
)

const pluginBaseURL = "https://rpk-plugins.redpanda.com"

type rpaiArtifact struct {
	Path   string `json:"path"`
	Sha256 string `json:"sha256"`
}

type archive struct {
	Version   string                  `json:"version"`
	IsLatest  bool                    `json:"is_latest"`
	Artifacts map[string]rpaiArtifact `json:"artifacts"`
}

type rpaiManifest struct {
	Archives []archive `json:"archives"`
}

func (m *rpaiManifest) LatestArtifact() (rpaiArtifact, string, error) {
	osArch := runtime.GOOS + "-" + runtime.GOARCH
	for _, a := range m.Archives {
		if a.IsLatest {
			if artifact, ok := a.Artifacts[osArch]; ok {
				return artifact, a.Version, nil
			}
			return rpaiArtifact{}, "", fmt.Errorf("no artifact found for os-arch: %s in our latest release. Please report this issue with Redpanda Support", osArch)
		}
	}
	return rpaiArtifact{}, "", errors.New("no latest artifact found. Please report this issue with Redpanda Support")
}

func (m *rpaiManifest) ArtifactVersion(version string) (rpaiArtifact, error) {
	osArch := runtime.GOOS + "-" + runtime.GOARCH
	for _, a := range m.Archives {
		if a.Version == version {
			if artifact, ok := a.Artifacts[osArch]; ok {
				return artifact, nil
			}
			return rpaiArtifact{}, fmt.Errorf("no artifact found for os-arch: %s in Redpanda AI CLI version %q. Please report this issue with Redpanda Support", osArch, version)
		}
	}
	return rpaiArtifact{}, fmt.Errorf("unable to find version %q", version)
}

// rpaiRepoClient is a client to query our repository hosting the Redpanda AI
// CLI (rpai) plugin manifest.
type rpaiRepoClient struct {
	cl   *httpapi.Client
	os   string
	arch string
}

func newRepoClient() (*rpaiRepoClient, error) {
	timeout, err := plugin.GetPluginDownloadTimeout()
	if err != nil {
		return nil, err
	}
	return &rpaiRepoClient{
		cl: httpapi.NewClient(
			httpapi.HTTPClient(&http.Client{
				Timeout: timeout,
			}),
		),
		os:   runtime.GOOS,
		arch: runtime.GOARCH,
	}, nil
}

func (c *rpaiRepoClient) Manifest(ctx context.Context) (*rpaiManifest, error) {
	var manifest rpaiManifest
	path := fmt.Sprintf("%v/rpai/manifest.json", getPluginURL())
	err := c.cl.Get(ctx, path, nil, &manifest)
	if err != nil {
		return nil, fmt.Errorf("unable to retrieve Redpanda AI CLI manifest: %v", err)
	}
	return &manifest, nil
}

func getPluginURL() string {
	url := pluginBaseURL
	if repoURL := os.Getenv("RPK_PLUGIN_REPOSITORY"); repoURL != "" {
		url = repoURL
	}
	return url
}
