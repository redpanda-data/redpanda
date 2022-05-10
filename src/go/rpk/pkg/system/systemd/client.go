// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package systemd

import (
	"strings"

	"github.com/spf13/afero"
)

type ActiveState int

// systemctl --state=help
// https://www.freedesktop.org/software/systemd/man/systemctl.html
const (
	ActiveStateActive ActiveState = iota
	ActiveStateReloading
	ActiveStateInactive
	ActiveStateFailed
	ActiveStateActivating
	ActiveStateDeactivating
	ActiveStateMaintenance
	ActiveStateUnknown
)

func (s ActiveState) String() string {
	switch s {
	case ActiveStateActive:
		return "active"
	case ActiveStateReloading:
		return "reloading"
	case ActiveStateInactive:
		return "inactive"
	case ActiveStateFailed:
		return "failed"
	case ActiveStateActivating:
		return "activating"
	case ActiveStateDeactivating:
		return "deactivating"
	case ActiveStateMaintenance:
		return "maintenance"
	}
	return "unknown"
}

func toActiveState(s string) ActiveState {
	switch strings.Trim(s, ` "`) {
	case "active":
		return ActiveStateActive
	case "reloading":
		return ActiveStateReloading
	case "inactive":
		return ActiveStateInactive
	case "failed":
		return ActiveStateFailed
	case "activating":
		return ActiveStateActivating
	case "deactivating":
		return ActiveStateDeactivating
	case "maintenance":
		return ActiveStateMaintenance
	}
	return ActiveStateUnknown
}

type LoadState int

const (
	LoadStateStub = iota
	LoadStateLoaded
	LoadStateNotFound
	LoadStateBadSetting
	LoadStateError
	LoadStateMerged
	LoadStateMasked
	LoadStateUnknown
)

func (s LoadState) String() string {
	switch s {
	case LoadStateStub:
		return "stub"
	case LoadStateLoaded:
		return "loaded"
	case LoadStateNotFound:
		return "not-found"
	case LoadStateBadSetting:
		return "bad-setting"
	case LoadStateError:
		return "error"
	case LoadStateMerged:
		return "merged"
	case LoadStateMasked:
		return "masked"
	}
	return "unknown"
}

func toLoadState(s string) LoadState {
	switch strings.Trim(s, ` "`) {
	case "stub":
		return LoadStateStub
	case "loaded":
		return LoadStateLoaded
	case "not-found":
		return LoadStateNotFound
	case "bad-setting":
		return LoadStateBadSetting
	case "error":
		return LoadStateError
	case "merged":
		return LoadStateMerged
	case "masked":
		return LoadStateMasked
	}
	return LoadStateUnknown
}

type Client interface {
	Shutdown() error
	StartUnit(name string) error
	UnitState(name string) (LoadState, ActiveState, error)
	LoadUnit(fs afero.Fs, body, name string) error
}

func UnitPath(name string) string {
	return "/etc/systemd/system/" + name
}

func IsLoaded(s LoadState) bool {
	return s == LoadStateLoaded
}

func IsActive(s ActiveState) bool {
	return s == ActiveStateActive
}
