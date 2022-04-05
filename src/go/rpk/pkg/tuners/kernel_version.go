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

const (
	ExpectedKernelVersion string = "4.19"
)

func NewKernelVersionChecker(
	getCurrent func() (string, error),
) kernelVersionChecker {
	return kernelVersionChecker{getCurrent: getCurrent}
}

type kernelVersionChecker struct {
	getCurrent func() (string, error)
}

func (c kernelVersionChecker) Id() CheckerID {
	return KernelVersion
}

func (c kernelVersionChecker) GetDesc() string {
	return "Kernel Version"
}

func (c kernelVersionChecker) GetSeverity() Severity {
	return Warning
}

func (c kernelVersionChecker) GetRequiredAsString() string {
	return "4.19"
}

func (c kernelVersionChecker) Check() *CheckResult {
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
	res.Current = current

	var cnt, major, minor, patch int
	cnt, res.Err = fmt.Sscanf(current, "%d.%d.%d", &major, &minor, &patch)
	if cnt != 3 {
		res.Err = fmt.Errorf("%s", "failed to parse kernel version")
		return res
	}

	if major < 4 || major == 4 && minor < 19 {
		res.Err = fmt.Errorf("%s", "kernel version is too old")
		return res
	}

	res.IsOk = true
	return res
}
