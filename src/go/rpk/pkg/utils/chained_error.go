// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

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
