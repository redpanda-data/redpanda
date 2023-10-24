// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package cloudapi

import (
	"context"
	"errors"
	"sort"
	"time"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/httpapi"
)

const installpackPath = "/api/v1/clusters-resources/install-pack-versions"

// InstallPackArtifact contains an artifact name and version.
type InstallPackArtifact struct {
	Name           string `json:"name"`
	Version        string `json:"version"`
	Location       string `json:"location"`
	ChecksumSHA256 string `json:"checksumSha256"`
}

type InstallPackArtifacts []InstallPackArtifact

// Find returns the artifact if it exists.
func (as InstallPackArtifacts) Find(name string) (InstallPackArtifact, bool) {
	for _, a := range as {
		if a.Name == name {
			return a, true
		}
	}
	return InstallPackArtifact{}, false
}

// InstallPack contains pinned versions for an install pack.
type InstallPack struct {
	ID              string               `json:"id"`
	Artifacts       InstallPackArtifacts `json:"artifacts"`
	Version         string               `json:"version"`
	Certified       bool                 `json:"certified"`
	RedpandaVersion string               `json:"redpandaVersion"`
	ReleasedAt      time.Time            `json:"releasedAt"`
}

// InstallPack returns a specific installpack version.
func (cl *Client) InstallPack(ctx context.Context, version string) (p InstallPack, err error) {
	path := httpapi.Pathfmt(installpackPath+"/%s", version)
	return p, cl.cl.Get(ctx, path, nil, &p)
}

// InstallPacks returns a list of available installpack versions.
func (cl *Client) InstallPacks(ctx context.Context) (l []InstallPack, err error) {
	return l, cl.cl.Get(ctx, installpackPath, nil, &l)
}

// LatestInstallPack retrieves the latest installpack according to the
// releasedAt field.
func (cl *Client) LatestInstallPack(ctx context.Context) (InstallPack, error) {
	ipvs, err := cl.InstallPacks(ctx)
	if err != nil {
		return InstallPack{}, err
	}
	if len(ipvs) < 1 {
		return InstallPack{}, errors.New("no install-pack versions found")
	}
	sort.Slice(ipvs, func(i, j int) bool {
		return ipvs[i].ReleasedAt.After(ipvs[j].ReleasedAt)
	})
	return ipvs[0], nil
}
