package tuners

type TuneResult interface {
	IsFailed() bool
	GetError() error
	IsRebootRequired() bool
}

type tuneResult struct {
	err            error
	rebootRequired bool
}

func NewTuneError(err error) TuneResult {
	return &tuneResult{err: err}
}

func NewTuneResult(rebootRequired bool) TuneResult {
	return &tuneResult{rebootRequired: rebootRequired}
}

func (result *tuneResult) IsFailed() bool {
	return result.err != nil
}

func (result *tuneResult) GetError() error {
	return result.err
}

func (result *tuneResult) IsRebootRequired() bool {
	return result.rebootRequired
}
