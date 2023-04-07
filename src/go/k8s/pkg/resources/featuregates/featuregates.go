// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// Package featuregates provides information on Redpanda versions where
// features are enabled.
package featuregates

import (
	"fmt"

	"github.com/Masterminds/semver/v3"
)

//nolint:stylecheck // the linter suggests camel case for one letter!?!?
var (
	V21_11  = mustSemVer("v21.11.0")
	V22_1   = mustSemVer("v22.1.0")
	V22_2_1 = mustSemVer("v22.2.1")
	V22_3   = mustSemVer("v22.3.0")
	V23_2   = mustSemVer("v23.2.0")
)

// ShadowIndex feature gate should be removed in 3 version starting
// from v21.11.x where cloud cache directory was introduced
// TODO in future remove this if statement GH-2631
func ShadowIndex(version string) bool {
	return atLeastVersion(V21_11, version)
}

// MetricsQueryParamName feature gate changes the metrics `name` query param
// Feature gate should be removed when v23.1 is deprecated.
func MetricsQueryParamName(version string) bool {
	return atLeastVersion(V23_2, version)
}

// InternalTopicReplication feature gate should be removed when the operator
// will no longer support versions less than v22.3.1
func InternalTopicReplication(version string) bool {
	return atLeastVersion(V22_3, version)
}

// CentralizedConfiguration feature gate should be removed when the operator
// will no longer support 21.x or older versions
func CentralizedConfiguration(version string) bool {
	return atLeastVersion(V22_1, version)
}

// ClusterHealth feature gate should be removed when the operator
// will no longer support 21.x or older versions
func ClusterHealth(version string) bool {
	return atLeastVersion(V22_1, version)
}

// MaintenanceMode feature gate should be removed when the operator
// will no longer support 21.x or older versions
func MaintenanceMode(version string) bool {
	return atLeastVersion(V22_1, version)
}

// PerListenerAuthorization feature gate should be removed when the operator
// will no longer support 22.2.1 or older versions
func PerListenerAuthorization(version string) bool {
	return atLeastVersion(V22_2_1, version)
}

// EmptySeedStartCluster feature gate should be removed when v22.2 is no longer supported
func EmptySeedStartCluster(version string) bool {
	return atLeastVersion(V22_3, version)
}

// RackAwareness feature gate prevents enabling rack awareness
// or setting the rack id on redpanda versions older than 22.1
func RackAwareness(version string) bool {
	return atLeastVersion(V22_1, version)
}

// atLeastVersion tells if the given version is greater or equal than the
// minVersion.
// All semver incompatible versions (such as "dev" or "latest") and non-version
// tags (such as v0.0.0-xxx) are always considered newer than minVersion.
func atLeastVersion(minVersion *semver.Version, version string) bool {
	v, err := semver.NewVersion(version)
	if err != nil {
		return true
	}
	if v.Major() == 0 && v.Minor() == 0 && v.Patch() == 0 {
		return true
	}
	return v.Major() == minVersion.Major() && v.Minor() >= minVersion.Minor() || v.Major() > minVersion.Major()
}

func mustSemVer(version string) *semver.Version {
	v, err := semver.NewVersion(version)
	if err != nil {
		panic(fmt.Sprintf("version %q is not semver compatible", version))
	}
	return v
}
