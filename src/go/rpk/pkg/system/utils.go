// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package system

import (
	"time"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/os"
	log "github.com/sirupsen/logrus"
)

func UnameAndDistro(timeout time.Duration) (string, error) {
	res, err := uname()
	if err != nil {
		return "", err
	}
	cmd := "lsb_release"
	p := os.NewProc()
	ls, err := p.RunWithSystemLdPath(timeout, cmd, "-d", "-s")
	if err != nil {
		log.Debugf("%s failed", cmd)
	}
	if len(ls) == 0 {
		log.Debugf("%s didn't return any output", cmd)
	} else {
		res += " " + ls[0]
	}
	return res, nil
}

func int8ToString(ints [65]int8) string {
	var bs [65]byte
	for i, in := range ints {
		bs[i] = byte(in)
	}
	return string(bs[:])
}
