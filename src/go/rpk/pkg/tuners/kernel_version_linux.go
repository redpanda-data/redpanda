// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package tuners

import "syscall"

func UtsnameStr(in []int8) string {
	i, out := 0, make([]byte, 0, len(in))
	for ; i < len(in); i++ {
		if in[i] == 0 {
			break
		}
		out = append(out, byte(in[i]))
	}
	return string(out)
}

func GetKernelVersion() (string, error) {
	var uname syscall.Utsname
	err := syscall.Uname(&uname)
	if err != nil {
		return "", err
	}

	return UtsnameStr(uname.Release[:]), nil
}
