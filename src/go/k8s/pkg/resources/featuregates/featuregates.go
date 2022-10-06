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

var (
	v22_1  = mustSemVer("v22.1.0")
	v21_11 = mustSemVer("v21.11.0")
)

// ShadowIndex feature gate should be removed in 3 version starting
// from v21.11.x where cloud cache directory was introduced
// TODO in future remove this if statement GH-2631
func ShadowIndex(version string) bool {
	return atLeastVersion(v21_11, version)
}

// CentralizedConfiguration feature gate should be removed when the operator
// will no longer support 21.x or older versions
func CentralizedConfiguration(version string) bool {
	return atLeastVersion(v22_1, version)
}

// MaintenanceMode feature gate should be removed when the operator
// will no longer support 21.x or older versions
func MaintenanceMode(version string) bool {
	return atLeastVersion(v22_1, version)
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
