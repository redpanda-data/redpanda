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
	"fmt"
	"reflect"
)

// NewEqualityChecker creates a checker that will return valid result if value
// returned by getCurrent function is equal to required value. This checker uses
// reflect.DeepEqual to compare the values.
func NewEqualityChecker(
	id CheckerID,
	desc string,
	severity Severity,
	required interface{},
	getCurrent func() (interface{}, error),
) Checker {
	return &equalityChecker{
		id:         id,
		desc:       desc,
		required:   required,
		getCurrent: getCurrent,
		severity:   severity,
	}
}

type equalityChecker struct {
	id         CheckerID
	desc       string
	severity   Severity
	required   interface{}
	getCurrent func() (interface{}, error)
}

func (c *equalityChecker) ID() CheckerID {
	return c.id
}

func (c *equalityChecker) GetDesc() string {
	return c.desc
}

func (c *equalityChecker) GetSeverity() Severity {
	return c.severity
}

func (c *equalityChecker) GetRequiredAsString() string {
	return fmt.Sprint(c.required)
}

func (c *equalityChecker) Check() *CheckResult {
	res := &CheckResult{
		CheckerID: c.ID(),
		Desc:      c.GetDesc(),
		Severity:  c.GetSeverity(),
		Required:  c.GetRequiredAsString(),
	}
	current, err := c.getCurrent()
	if err != nil {
		res.Err = err
		return res
	}
	res.IsOk = reflect.DeepEqual(c.required, current)
	res.Current = fmt.Sprint(current)
	return res
}
