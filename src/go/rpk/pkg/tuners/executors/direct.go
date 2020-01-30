package executors

import "vectorized/pkg/tuners/executors/commands"

type directExecutor struct {
	Executor
}

func NewDirectExecutor() Executor {
	return &directExecutor{}
}

func (e *directExecutor) Execute(cmd commands.Command) error {
	return cmd.Execute()
}

func (e *directExecutor) IsLazy() bool {
	return false
}
