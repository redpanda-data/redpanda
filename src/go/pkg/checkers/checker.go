package checkers

type Severity byte

const (
	Fatal = iota
	Warning
)

func (s Severity) String() string {
	switch s {
	case Fatal:
		return "Fatal"
	case Warning:
		return "Warning"
	}
	panic("Wrong checker severity")
}

type CheckResult struct {
	IsOk    bool
	Err     error
	Current string
}

type Checker interface {
	GetDesc() string
	Check() *CheckResult
	GetRequiredAsString() string
	GetSeverity() Severity
}
