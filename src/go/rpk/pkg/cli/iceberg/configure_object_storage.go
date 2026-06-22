// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package iceberg

import (
	"context"

	"github.com/redpanda-data/common-go/rpadmin"
)

type objectStorageFlow struct{}

func (*objectStorageFlow) name() string {
	return "Object storage (filesystem catalog, no external server)"
}

func (*objectStorageFlow) collect(
	ctx context.Context,
	cl *rpadmin.AdminAPI,
	current rpadmin.Config,
	_ rpadmin.ConfigSchema,
) (map[string]string, bool, error) {
	for {
		baseLocation, err := promptStringWithCurrent(
			"iceberg_catalog_base_location",
			"redpanda-iceberg-catalog",
			"Catalog base location (path within the cloud storage bucket):",
			current,
		)
		if err != nil {
			return nil, false, err
		}
		if baseLocation == "" {
			baseLocation = "redpanda-iceberg-catalog"
		}
		overrides := map[string]string{
			"iceberg_catalog_type":          "object_storage",
			"iceberg_catalog_base_location": baseLocation,
		}
		retry, validated := promptCheckpoint(ctx, cl, overrides)
		if !retry {
			return overrides, validated, nil
		}
	}
}
