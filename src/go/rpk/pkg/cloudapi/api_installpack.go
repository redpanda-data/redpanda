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

// Find returns the arftifact if it exists.
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
	ID        string               `json:"id"`
	Artifacts InstallPackArtifacts `json:"artifacts"`
}

// Cluster returns information about a Redpanda cluster.
func (cl *Client) InstallPack(ctx context.Context, version string) (p InstallPack, err error) {
	path := httpapi.Pathfmt(installpackPath+"/%s", version)
	return p, cl.cl.Get(ctx, path, nil, &p)
}
