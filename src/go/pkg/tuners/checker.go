package tuners

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
	IsOk     bool
	Err      error
	Current  string
	Desc     string
	Severity Severity
	Required string
}

type Checker interface {
	Id() CheckerID
	GetDesc() string
	Check() *CheckResult
	GetRequiredAsString() string
	GetSeverity() Severity
}
