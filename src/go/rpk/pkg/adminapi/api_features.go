// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package adminapi

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

// LicenseStatus is an enum for different licensing states in a Redpanda cluster.
type LicenseStatus string

const (
	// LicenseStatusValid represents a valid license.
	LicenseStatusValid LicenseStatus = "valid"
	// LicenseStatusExpired represents a valid, but already expired license.
	LicenseStatusExpired LicenseStatus = "expired"
	// LicenseStatusNotPresent means no license is installed.
	LicenseStatusNotPresent LicenseStatus = "not_present"
)

// EnterpriseFeaturesResponse describes the response schema to a GET /v1/features/enterprise
// request. This endpoint reports the license status and enterprise features in use.
type EnterpriseFeaturesResponse struct {
	// LicenseStatus describes the status of the Redpanda enterprise license.
	LicenseStatus LicenseStatus `json:"license_status"`
	// Violation indicates a license violation. It evaluates to true if LicenseStatus
	// is not LicenseStatusValid and one or more enterprise features are enabled.
	Violation bool `json:"violation"`
	// Features is a list of enterprise features (name and whether in use).
	Features []EnterpriseFeature `json:"features"`
}

// EnterpriseFeature is a Redpanda enterprise feature with its name, and whether it is in use.
type EnterpriseFeature struct {
	// Name of the enterprise feature. Follows snake case naming. For example: "audit_logging".
	Name string `json:"name"`
	// Enabled is true if the feature is in use.
	Enabled bool `json:"enabled"`
}

// GetFeatures returns information about the available features.
func (a *AdminAPI) GetFeatures(ctx context.Context) (FeaturesResponse, error) {
	var features FeaturesResponse
	return features, a.sendToLeader(
		ctx,
		http.MethodGet,
		"/v1/features",
		nil,
		&features)
}

// GetEnterpriseFeatures reports enterprise features in use as well as the license status.
func (a *AdminAPI) GetEnterpriseFeatures(ctx context.Context) (EnterpriseFeaturesResponse, error) {
	var response EnterpriseFeaturesResponse
	return response, a.sendAny(
		ctx,
		http.MethodGet,
		"/v1/features/enterprise",
		nil,
		&response)
}

func (a *AdminAPI) GetLicenseInfo(ctx context.Context) (License, error) {
	var license License
	return license, a.sendToLeader(ctx, http.MethodGet, "/v1/features/license", nil, &license)
}

func (a *AdminAPI) SetLicense(ctx context.Context, license interface{}) error {
	return a.sendToLeader(ctx, http.MethodPut, "/v1/features/license", license, nil)
}
