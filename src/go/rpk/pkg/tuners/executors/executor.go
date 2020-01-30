package executors

import "vectorized/pkg/tuners/executors/commands"

type Executor interface {
	Execute(commands.Command) error
	IsLazy() bool
}
