// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package tuners

type Severity byte

const (
	Fatal = iota
	Warning
)

func (s Severity) String() string {
	switch s {
	case Fatal:
		return "Fatal"
	case Warning:
		return "Warning"
	}
	panic("Wrong checker severity")
}

type CheckResult struct {
	CheckerID CheckerID
	IsOk      bool
	Err       error
	Current   string
	Desc      string
	Severity  Severity
	Required  string
}

type Checker interface {
	ID() CheckerID
	GetDesc() string
	Check() *CheckResult
	GetRequiredAsString() string
	GetSeverity() Severity
}
