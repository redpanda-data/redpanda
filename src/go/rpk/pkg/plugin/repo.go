// Copyright 2026 Redpanda Data, Inc.
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
	"net/http"
	"os"
	"runtime"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/httpapi"
)

// RepoBaseURL is the canonical host for managed-plugin manifests. Callers
// override it via the RPK_PLUGIN_REPOSITORY environment variable, typically
// during development or when pointing at a staging bucket before a manifest
// has been promoted to prod.
const RepoBaseURL = "https://rpk-plugins.redpanda.com"

// RepoArtifact is a single os-arch entry within a RepoArchive. Path is the
// fully qualified URL of the binary tarball; Sha256 is the expected sha256 of
// the binary after decompression (the compressed-blob sha is not used).
type RepoArtifact struct {
	Path   string `json:"path"`
	Sha256 string `json:"sha256"`
}

// RepoArchive is one published version of a managed plugin, keyed by os-arch.
type RepoArchive struct {
	Version   string                  `json:"version"`
	IsLatest  bool                    `json:"is_latest"`
	Artifacts map[string]RepoArtifact `json:"artifacts"`
}

// RepoManifest is the top-level shape of `<repo>/<plugin>/manifest.json`.
type RepoManifest struct {
	Archives []RepoArchive `json:"archives"`
}

// LatestArtifact returns the RepoArtifact for the running os/arch from the
// archive marked is_latest, along with that archive's version string. The
// supplied displayName is only used in error messages (e.g. "Redpanda
// Connect", "Redpanda AI CLI").
func (m *RepoManifest) LatestArtifact(displayName string) (RepoArtifact, string, error) {
	osArch := runtime.GOOS + "-" + runtime.GOARCH
	for _, a := range m.Archives {
		if a.IsLatest {
			if artifact, ok := a.Artifacts[osArch]; ok {
				return artifact, a.Version, nil
			}
			return RepoArtifact{}, "", fmt.Errorf("no artifact found for os-arch %s in the latest %s release. Please report this issue with Redpanda Support", osArch, displayName)
		}
	}
	return RepoArtifact{}, "", errors.New("no latest artifact found. Please report this issue with Redpanda Support")
}

// ArtifactVersion returns the RepoArtifact for the running os/arch from the
// archive matching the given version. displayName is used in error messages.
func (m *RepoManifest) ArtifactVersion(displayName, version string) (RepoArtifact, error) {
	osArch := runtime.GOOS + "-" + runtime.GOARCH
	for _, a := range m.Archives {
		if a.Version == version {
			if artifact, ok := a.Artifacts[osArch]; ok {
				return artifact, nil
			}
			return RepoArtifact{}, fmt.Errorf("no artifact found for os-arch %s in %s version %q. Please report this issue with Redpanda Support", osArch, displayName, version)
		}
	}
	return RepoArtifact{}, fmt.Errorf("unable to find version %q", version)
}

// RepoClient fetches manifests from a managed-plugin repository — typically
// rpk-plugins.redpanda.com or an RPK_PLUGIN_REPOSITORY override.
type RepoClient struct {
	cl          *httpapi.Client
	slug        string
	displayName string
}

// NewRepoClient returns a RepoClient configured for the given plugin slug
// (e.g. "connect", "connect-fips", "rpai"). displayName is woven into the
// errors returned from Manifest so users see "Redpanda Connect" or "Redpanda
// AI CLI" rather than the slug.
func NewRepoClient(slug, displayName string) (*RepoClient, error) {
	timeout, err := GetPluginDownloadTimeout()
	if err != nil {
		return nil, err
	}
	return &RepoClient{
		cl: httpapi.NewClient(
			httpapi.HTTPClient(&http.Client{
				Timeout: timeout,
			}),
		),
		slug:        slug,
		displayName: displayName,
	}, nil
}

// Manifest fetches the published manifest for this client's plugin slug.
func (c *RepoClient) Manifest(ctx context.Context) (*RepoManifest, error) {
	var manifest RepoManifest
	url := fmt.Sprintf("%s/%s/manifest.json", repoBaseURL(), c.slug)
	if err := c.cl.Get(ctx, url, nil, &manifest); err != nil {
		return nil, fmt.Errorf("unable to retrieve %s manifest: %v", c.displayName, err)
	}
	return &manifest, nil
}

// repoBaseURL returns RepoBaseURL or the RPK_PLUGIN_REPOSITORY override.
func repoBaseURL() string {
	if repoURL := os.Getenv("RPK_PLUGIN_REPOSITORY"); repoURL != "" {
		return repoURL
	}
	return RepoBaseURL
}
