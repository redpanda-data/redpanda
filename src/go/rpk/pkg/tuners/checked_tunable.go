package tuners

import (
	"fmt"

	log "github.com/sirupsen/logrus"
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

	log.Debugf("Checking '%s'", t.checker.GetDesc())
	result := t.checker.Check()
	if result.Err != nil {
		return NewTuneError(result.Err)
	}

	if result.IsOk {
		log.Debugf("Check '%s' passed, skipping tunning", t.checker.GetDesc())
		return NewTuneResult(false)
	}

	tuneResult := t.tuneAction()
	if tuneResult.Error() != nil {
		return NewTuneError(tuneResult.Error())
	}
	if !t.disablePostTuneCheck {
		postTuneResult := t.checker.Check()
		if !postTuneResult.IsOk {
			err := fmt.Errorf("System tuning was not succesfull, "+
				"check '%s' failed, required: '%s', current '%v'",
				t.checker.GetDesc(),
				t.checker.GetRequiredAsString(), result.Current)
			return NewTuneError(err)
		}
	}
	return tuneResult
}
