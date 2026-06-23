// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package k8s

import "github.com/redpanda-data/redpanda/src/go/rpk/pkg/plugin"

// pluginSlug is the on-disk + manifest-path identifier for the rpk k8s
// plugin: the binary lands at ~/.local/bin/.rpk.managed-k8s and the manifest
// lives at <repo>/k8s/manifest.json.
const (
	pluginSlug  = "k8s"
	displayName = "Redpanda Kubernetes plugin"
)

func newRepoClient() (*plugin.RepoClient, error) {
	return plugin.NewRepoClient(pluginSlug, displayName)
}
