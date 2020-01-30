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

func (c *intChecker) Id() CheckerID {
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
		CheckerId: c.Id(),
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
