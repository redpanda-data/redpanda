// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

//go:build !windows

package ethtool

import "github.com/safchain/ethtool"

type EthtoolWrapper interface {
	DriverName(string) (string, error)
	Features(string) (map[string]bool, error)
	Change(string, map[string]bool) error
}

func NewEthtoolWrapper() (EthtoolWrapper, error) {
	return ethtool.NewEthtool()
}
