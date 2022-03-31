// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package featuregates

import "github.com/Masterminds/semver/v3"

const (
	centralizedConfigMajor = uint64(22)
	centralizedConfigMinor = uint64(1)
)

// CentralizedConfiguration feature gate should be removed when the operator
// will no longer support 21.x or older versions
func CentralizedConfiguration(version string) bool {
	if version == "dev" {
		// development version contains this feature
		return true
	}
	v, err := semver.NewVersion(version)
	if err != nil {
		return false
	}

	return v.Major() == centralizedConfigMajor && v.Minor() >= centralizedConfigMinor || v.Major() > centralizedConfigMajor
}
