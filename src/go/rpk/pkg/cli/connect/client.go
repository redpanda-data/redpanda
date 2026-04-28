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
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/fips"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/plugin"
)

const (
	connectPluginSlug     = "connect"
	connectFipsPluginSlug = "connect-fips"
	connectDisplayName    = "Redpanda Connect"
)

// newRepoClient returns a managed-plugin manifest client targeting the
// connect repo. When FIPS mode is enabled the client points at the
// connect-fips manifest, which lists the FIPS-validated builds.
func newRepoClient() (*plugin.RepoClient, error) {
	slug := connectPluginSlug
	if fips.IsEnabled() {
		slug = connectFipsPluginSlug
	}
	return plugin.NewRepoClient(slug, connectDisplayName)
}
