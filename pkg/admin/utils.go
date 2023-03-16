// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package admin

import (
	"context"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/api/admin"
)

// CentralConfigFeatureName is the name of the centralized configuration feature in Redpanda
const CentralConfigFeatureName = "central_config"

// IsFeatureActive is a helper function that checks if a given feature is active in the cluster
func IsFeatureActive(
	ctx context.Context, c AdminAPIClient, name string,
) (bool, error) {
	res, err := c.GetFeatures(ctx)
	if err != nil {
		return false, err
	}
	for _, f := range res.Features {
		if f.Name == name {
			return f.State == admin.FeatureStateActive, nil
		}
	}
	return false, nil
}
