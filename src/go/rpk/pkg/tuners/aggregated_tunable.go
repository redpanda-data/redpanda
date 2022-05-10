// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package tuners

func NewAggregatedTunable(tunables []Tunable) Tunable {
	return &aggregatedTunable{tunables}
}

type aggregatedTunable struct {
	tunables []Tunable
}

func (t *aggregatedTunable) CheckIfSupported() (supported bool, reason string) {
	for _, tunable := range t.tunables {
		supported, reason := tunable.CheckIfSupported()
		if !supported {
			return false, reason
		}
	}
	return true, ""
}

func (t *aggregatedTunable) Tune() TuneResult {
	needReboot := false
	for _, tunable := range t.tunables {
		result := tunable.Tune()
		if result.IsFailed() {
			return result
		}
		if result.IsRebootRequired() {
			needReboot = true
		}
	}
	return NewTuneResult(needReboot)
}
