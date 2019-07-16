package checkers

type CheckResult struct {
	IsOk    bool
	Err     error
	Current string
}

type Checker interface {
	GetDesc() string
	Check() *CheckResult
	GetRequiredAsString() string
	IsCritical() bool
}
