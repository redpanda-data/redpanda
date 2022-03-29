// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package system

import (
	"errors"

	"github.com/spf13/afero"
)

func getMemInfo(_ afero.Fs) (*MemInfo, error) {
	return nil, errors.New("Memory info collection not available in MacOS")
}
