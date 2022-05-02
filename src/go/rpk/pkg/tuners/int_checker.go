// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package tuners

import "strconv"

func NewIntChecker(
	id CheckerID,
	desc string,
	severity Severity,
	check func(int) bool,
	renderRequired func() string,
	getCurrent func() (int, error),
) Checker {
	return &intChecker{
		id:             id,
		desc:           desc,
		check:          check,
		renderRequired: renderRequired,
		getCurrent:     getCurrent,
		severity:       severity,
	}
}

type intChecker struct {
	id             CheckerID
	desc           string
	check          func(int) bool
	renderRequired func() string
	getCurrent     func() (int, error)
	severity       Severity
}

func (c *intChecker) ID() CheckerID {
	return c.id
}

func (c *intChecker) GetDesc() string {
	return c.desc
}

func (c *intChecker) GetSeverity() Severity {
	return c.severity
}

func (c *intChecker) GetRequiredAsString() string {
	return c.renderRequired()
}

func (c *intChecker) Check() *CheckResult {
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
	res.Current = strconv.Itoa(current)
	return res
}
