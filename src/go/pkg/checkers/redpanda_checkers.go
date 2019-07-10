package checkers

import (
	"vectorized/redpanda"
)

func NewConfigChecker(config *redpanda.Config) Checker {
	return NewEqualityChecker(
		"Config file valid",
		true,
		true,
		func() (interface{}, error) {
			return redpanda.CheckConfig(config), nil
		})
}
