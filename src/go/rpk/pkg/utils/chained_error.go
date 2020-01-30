package utils

import "fmt"

type chainedError struct {
	err error
	msg string
}

func ChainedError(err error, msg string) *chainedError {
	return &chainedError{
		err: err,
		msg: msg,
	}
}

func (chainedError *chainedError) Error() string {
	return fmt.Sprintf("%s - [%s]",
		chainedError.msg, chainedError.err.Error())
}
