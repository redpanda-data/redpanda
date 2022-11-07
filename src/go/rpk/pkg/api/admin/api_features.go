// Copyright 2021 Redpanda Data, Inc.
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
	"net/http"
)

// FeatureState enumerates the possible states of a feature.
type FeatureState string

const (
	FeatureStateActive      FeatureState = "active"
	FeatureStatePreparing   FeatureState = "preparing"
	FeatureStateAvailable   FeatureState = "available"
	FeatureStateUnavailable FeatureState = "unavailable"
	FeatureStateDisabled    FeatureState = "disabled"
)

// Feature contains information on the state of a feature.
type Feature struct {
	Name      string       `json:"name"`
	State     FeatureState `json:"state"`
	WasActive bool         `json:"was_active"`
}

// FeaturesResponse contains information on the features available on a Redpanda cluster.
type FeaturesResponse struct {
	ClusterVersion int       `json:"cluster_version"`
	Features       []Feature `json:"features"`
}

type License struct {
	Loaded     bool              `json:"loaded"`
	Properties LicenseProperties `json:"license"`
}

type LicenseProperties struct {
	Version      int    `json:"format_version"`
	Organization string `json:"org"`
	Type         string `json:"type"`
	Expires      int64  `json:"expires"`
	Checksum     string `json:"sha256"`
}

// GetFeatures returns information about the available features.
func (a *AdminAPI) GetFeatures(ctx context.Context) (FeaturesResponse, error) {
	var features FeaturesResponse
	return features, a.sendAny(
		ctx,
		http.MethodGet,
		"/v1/features",
		nil,
		&features)
}

func (a *AdminAPI) GetLicenseInfo(ctx context.Context) (License, error) {
	var license License
	return license, a.sendAny(ctx, http.MethodGet, "/v1/features/license", nil, &license)
}

func (a *AdminAPI) SetLicense(ctx context.Context, license interface{}) error {
	return a.sendToLeader(ctx, http.MethodPut, "/v1/features/license", license, nil)
}
