// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package system

import "syscall"

func uname() (string, error) {
	var uname syscall.Utsname
	err := syscall.Uname(&uname)
	if err != nil {
		return "", err
	}
	str := ""
	str += string(int8ToString(uname.Machine)) + " "
	str += string(int8ToString(uname.Release)) + " "
	str += string(int8ToString(uname.Version))
	return str, nil

}
