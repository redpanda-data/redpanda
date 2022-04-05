// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package tuners

import "github.com/spf13/afero"

func NewFileExistanceChecker(
	fs afero.Fs, id CheckerID, desc string, severity Severity, filePath string,
) Checker {
	return NewEqualityChecker(
		id,
		desc,
		severity,
		true,
		func() (interface{}, error) {
			return afero.Exists(fs, filePath)
		})
}
