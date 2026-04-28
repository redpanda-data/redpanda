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

// rpaiPluginSlug is the path component used in the manifest URL —
// `<repo>/rpai/manifest.json`. Lives next to the connect plugin under the
// same managed-plugin host.
const (
	rpaiPluginSlug  = "rpai"
	rpaiDisplayName = "Redpanda AI CLI"
)

func newRepoClient() (*plugin.RepoClient, error) {
	return plugin.NewRepoClient(rpaiPluginSlug, rpaiDisplayName)
}
