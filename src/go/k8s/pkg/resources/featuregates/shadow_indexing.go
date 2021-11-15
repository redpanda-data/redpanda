// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// Package featuregates gives ability to control k8s resource creation
// based on the Redpanda version
package featuregates

import "github.com/Masterminds/semver/v3"

const (
	major = uint64(21)
	minor = uint64(10)
)

// ShadowIndex feature gate should be removed in 3 version starting
// from v21.10.x where cloud cache directory was introduced
// TODO in version/month 22.01 remove this if statement GH-2631
func ShadowIndex(version string) bool {
	v, err := semver.NewVersion(version)
	if err != nil {
		return false
	}

	return v.Major() == major && v.Minor() >= minor || v.Major() > major
}
