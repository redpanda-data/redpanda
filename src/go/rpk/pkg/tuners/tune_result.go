// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package tuners

type TuneResult interface {
	IsFailed() bool
	Error() error
	IsRebootRequired() bool
}

type tuneResult struct {
	err            error
	rebootRequired bool
}

func NewTuneError(err error) TuneResult {
	return &tuneResult{err: err}
}

func NewTuneResult(rebootRequired bool) TuneResult {
	return &tuneResult{rebootRequired: rebootRequired}
}

func (result *tuneResult) IsFailed() bool {
	return result.err != nil
}

func (result *tuneResult) Error() error {
	return result.err
}

func (result *tuneResult) IsRebootRequired() bool {
	return result.rebootRequired
}
