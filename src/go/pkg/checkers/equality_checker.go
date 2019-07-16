package checkers

import (
	"fmt"
	"reflect"
)

// NewEqualityChecker creates a checker that will return valid result if value
// returned by getCurrent function is equal to required value. This checker uses
// reflect.DeepEqual to comparte the values
func NewEqualityChecker(
	desc string,
	critical bool,
	required interface{},
	getCurrent func() (interface{}, error),
) Checker {
	return &equalityChecker{
		desc:       desc,
		required:   required,
		getCurrent: getCurrent,
		isCritical: critical,
	}
}

type equalityChecker struct {
	Checker
	desc       string
	isCritical bool
	required   interface{}
	getCurrent func() (interface{}, error)
}

func (c *equalityChecker) GetDesc() string {
	return c.desc
}

func (c *equalityChecker) IsCritical() bool {
	return c.isCritical
}

func (c *equalityChecker) GetRequiredAsString() string {
	return fmt.Sprint(c.required)
}

func (c *equalityChecker) Check() *CheckResult {
	current, err := c.getCurrent()
	if err != nil {
		return &CheckResult{
			IsOk: false,
			Err:  err,
		}
	}
	return &CheckResult{
		IsOk:    reflect.DeepEqual(c.required, current),
		Current: fmt.Sprint(current),
	}
}
