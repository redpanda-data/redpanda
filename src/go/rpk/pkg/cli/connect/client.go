// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package connect

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"runtime"
	"time"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/httpapi"
)

const pluginBaseURL = "https://rpk-plugins.redpanda.com"

type connectArtifact struct {
	Path   string `json:"path"`
	Sha256 string `json:"sha256"`
}

type archive struct {
	Version   string                     `json:"version"`
	IsLatest  bool                       `json:"is_latest"`
	Artifacts map[string]connectArtifact `json:"artifacts"`
}

type connectManifest struct {
	Archives []archive `json:"archives"`
}

func (c *connectManifest) LatestArtifact() (connectArtifact, string, error) {
	osArch := runtime.GOOS + "-" + runtime.GOARCH
	for _, a := range c.Archives {
		if a.IsLatest {
			if artifact, ok := a.Artifacts[osArch]; ok {
				return artifact, a.Version, nil
			} else {
				return connectArtifact{}, "", fmt.Errorf("no artifact found for os-arch: %s in our latest release. Please report this issue with Redpanda Support", osArch)
			}
		}
	}
	return connectArtifact{}, "", errors.New("no latest artifact found. Please report this issue with Redpanda Support")
}

func (c *connectManifest) ArtifactVersion(version string) (connectArtifact, error) {
	osArch := runtime.GOOS + "-" + runtime.GOARCH
	for _, a := range c.Archives {
		if a.Version == version {
			if artifact, ok := a.Artifacts[osArch]; ok {
				return artifact, nil
			} else {
				return connectArtifact{}, fmt.Errorf("no artifact found for os-arch: %s in Redpanda Connect version %q. Please report this issue with Redpanda Support", osArch, version)
			}
		}
	}
	return connectArtifact{}, fmt.Errorf("unable to find version %q", version)
}

// connectRepoClient is a client to connect against our repository containing
// the Redpanda Connect packages.
type connectRepoClient struct {
	cl   *httpapi.Client
	os   string
	arch string
}

func newRepoClient() (*connectRepoClient, error) {
	timeout := 240 * time.Second
	if t := os.Getenv("RPK_PLUGIN_DOWNLOAD_TIMEOUT"); t != "" {
		duration, err := time.ParseDuration(t)
		if err != nil {
			return nil, fmt.Errorf("unable to parse RPK_PLUGIN_DOWNLOAD_TIMEOUT: %v", err)
		}
		timeout = duration
	}
	return &connectRepoClient{
		cl: httpapi.NewClient(
			httpapi.HTTPClient(&http.Client{
				Timeout: timeout,
			}),
		),
		os:   runtime.GOOS,
		arch: runtime.GOARCH,
	}, nil
}

func (c *connectRepoClient) Manifest(ctx context.Context) (*connectManifest, error) {
	var manifest connectManifest
	err := c.cl.Get(ctx, fmt.Sprintf("%v/connect/manifest.json", getPluginURL()), nil, &manifest)
	if err != nil {
		return nil, fmt.Errorf("unable to retrieve Redpanda Connect manifest: %v", err)
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
