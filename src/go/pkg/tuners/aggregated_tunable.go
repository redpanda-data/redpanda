package tuners

func NewAggregatedTunable(tunables []Tunable) Tunable {
	return &aggregatedTunable{
		tunables: tunables,
	}
}

type aggregatedTunable struct {
	Tunable
	tunables []Tunable
}

func (t *aggregatedTunable) CheckIfSupported() (supported bool, reason string) {
	for _, tunable := range t.tunables {
		supported, reason := tunable.CheckIfSupported()
		if !supported {
			return false, reason
		}
	}
	return true, ""
}

func (t *aggregatedTunable) Tune() TuneResult {
	var needReboot = false
	for _, tunable := range t.tunables {
		result := tunable.Tune()
		if result.IsFailed() {
			return result
		}
		if result.IsRebootRequired() {
			needReboot = true
		}
	}
	return NewTuneResult(needReboot)
}
