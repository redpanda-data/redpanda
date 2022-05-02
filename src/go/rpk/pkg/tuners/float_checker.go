// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package tuners

import "fmt"

func NewFloatChecker(
	id CheckerID,
	desc string,
	severity Severity,
	check func(float64) bool,
	renderRequired func() string,
	getCurrent func() (float64, error),
) Checker {
	return &floatChecker{
		id:             id,
		desc:           desc,
		check:          check,
		renderRequired: renderRequired,
		getCurrent:     getCurrent,
		severity:       severity,
	}
}

type floatChecker struct {
	id             CheckerID
	desc           string
	check          func(float64) bool
	renderRequired func() string
	getCurrent     func() (float64, error)
	severity       Severity
}

func (c *floatChecker) ID() CheckerID {
	return c.id
}

func (c *floatChecker) GetDesc() string {
	return c.desc
}

func (c *floatChecker) GetSeverity() Severity {
	return c.severity
}

func (c *floatChecker) GetRequiredAsString() string {
	return c.renderRequired()
}

func (c *floatChecker) Check() *CheckResult {
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
	res.IsOk = c.check(current)
	res.Current = fmt.Sprintf("%.2f", current)
	return res
}
