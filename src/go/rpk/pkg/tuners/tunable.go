package tuners

type Tunable interface {
	CheckIfSupported() (supported bool, reason string)
	Tune() TuneResult
}
