package checkers

import "fmt"

func NewFloatChecker(
	desc string,
	severity Severity,
	check func(float64) bool,
	renderRequired func() string,
	getCurrent func() (float64, error),
) Checker {
	return &floatChecker{
		desc:           desc,
		check:          check,
		renderRequired: renderRequired,
		getCurrent:     getCurrent,
		severity:       severity,
	}
}

type floatChecker struct {
	Checker
	desc           string
	check          func(float64) bool
	renderRequired func() string
	getCurrent     func() (float64, error)
	severity       Severity
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
	current, err := c.getCurrent()
	if err != nil {
		return &CheckResult{
			IsOk: false,
			Err:  err,
		}
	}
	return &CheckResult{
		IsOk:    c.check(current),
		Current: fmt.Sprintf("%.2f", current),
	}
}
