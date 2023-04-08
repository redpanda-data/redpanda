// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package tuners

import (
	"errors"
	"fmt"

	"go.uber.org/zap"
)

func NewCheckedTunable(
	checker Checker,
	tuneAction func() TuneResult,
	supportedAction func() (supported bool, reason string),
	disablePostTuneCheck bool,
) Tunable {
	return &checkedTunable{
		checker:              checker,
		tuneAction:           tuneAction,
		supportedAction:      supportedAction,
		disablePostTuneCheck: disablePostTuneCheck,
	}
}

type checkedTunable struct {
	checker              Checker
	tuneAction           func() TuneResult
	supportedAction      func() (supported bool, reason string)
	disablePostTuneCheck bool
}

func (t *checkedTunable) CheckIfSupported() (supported bool, reason string) {
	return t.supportedAction()
}

func (t *checkedTunable) Tune() TuneResult {
	zap.L().Sugar().Debugf("Checking '%s'", t.checker.GetDesc())
	result := t.checker.Check()
	if result.Err != nil {
		return NewTuneError(result.Err)
	}

	if result.IsOk {
		zap.L().Sugar().Debugf("Check '%s' passed, skipping tuning", t.checker.GetDesc())
		return NewTuneResult(false)
	}

	tuneResult := t.tuneAction()
	if tuneResult.Error() != nil {
		return NewTuneError(tuneResult.Error())
	}
	if !t.disablePostTuneCheck {
		postTuneResult := t.checker.Check()
		if !postTuneResult.IsOk {
			severity := t.checker.GetSeverity()
			msg := fmt.Sprintf(
				"check '%s' failed after its associated tuners ran. Severity: %s, required value: '%s', current value: '%v'",
				t.checker.GetDesc(),
				severity,
				t.checker.GetRequiredAsString(),
				result.Current,
			)
			if severity == Fatal {
				return NewTuneError(errors.New(msg))
			}
		}
	}
	return tuneResult
}
