package tuners

type TuneResult interface {
	IsFailed() bool
	GetError() error
	IsRebootRequired() bool
}

type errorTuneResult struct {
	err error
}

type tuneResult struct {
	rebootRequired bool
}

func NewTuneError(err error) TuneResult {
	return &errorTuneResult{
		err: err,
	}
}

func NewTuneResult(rebootRequired bool) TuneResult {
	return &tuneResult{
		rebootRequired: rebootRequired,
	}
}

func (result *errorTuneResult) IsFailed() bool {
	return true
}

func (result *errorTuneResult) GetError() error {
	return result.err
}

func (result *errorTuneResult) IsRebootRequired() bool {
	return false
}

func (result *tuneResult) IsFailed() bool {
	return false
}

func (result *tuneResult) GetError() error {
	return nil
}

func (result *tuneResult) IsRebootRequired() bool {
	return result.rebootRequired
}
