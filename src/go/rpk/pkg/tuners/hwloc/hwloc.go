// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package hwloc

type HwLoc interface {
	All() (string, error)
	CalcSingle(mask string) (string, error)
	Calc(mask string, location string) (string, error)
	Distribute(numberOfElements uint) ([]string, error)
	DistributeRestrict(numberOfElements uint, mask string) ([]string, error)
	GetNumberOfCores(mask string) (uint, error)
	GetNumberOfPUs(mask string) (uint, error)
	GetPhysIntersection(firstMask string, secondMask string) ([]uint, error)
	CheckIfMaskIsEmpty(mask string) bool
	IsSupported() bool
}
