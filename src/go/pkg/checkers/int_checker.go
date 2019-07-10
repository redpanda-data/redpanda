package checkers

import "strconv"

func NewIntChecker(
	desc string,
	isCritical bool,
	check func(int) bool,
	renderRequired func() string,
	getCurrent func() (int, error),
) Checker {
	return &intChecker{
		desc:           desc,
		check:          check,
		renderRequired: renderRequired,
		getCurrent:     getCurrent,
		isCritical:     isCritical,
	}
}

type intChecker struct {
	Checker
	desc           string
	check          func(int) bool
	renderRequired func() string
	getCurrent     func() (int, error)
	isCritical     bool
}

func (c *intChecker) GetDesc() string {
	return c.desc
}

func (c *intChecker) IsCritical() bool {
	return c.isCritical
}

func (c *intChecker) GetRequiredAsString() string {
	return c.renderRequired()
}

func (c *intChecker) Check() *CheckResult {
	current, err := c.getCurrent()
	if err != nil {
		return &CheckResult{
			IsOk: false,
			Err:  err,
		}
	}
	return &CheckResult{
		IsOk:    c.check(current),
		Current: strconv.Itoa(current),
	}
}
