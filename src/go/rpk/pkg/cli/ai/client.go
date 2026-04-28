// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package ai

import "github.com/redpanda-data/redpanda/src/go/rpk/pkg/plugin"

// rpaiPluginSlug is the on-disk + manifest-path identifier for the rpk ai
// plugin: the binary lands at ~/.local/bin/.rpk.managed-rpai and the manifest
// lives at <repo>/rpai/manifest.json. Lives next to the connect plugin under
// the same managed-plugin host. Kept lowercase + private so we don't surface
// the slug in user-facing strings.
const (
	rpaiPluginSlug  = "rpai"
	rpaiDisplayName = "Redpanda AI CLI"
)

func newRepoClient() (*plugin.RepoClient, error) {
	return plugin.NewRepoClient(rpaiPluginSlug, rpaiDisplayName)
}
