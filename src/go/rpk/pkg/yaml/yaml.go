// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package yaml

import (
	"github.com/spf13/afero"
	"gopkg.in/yaml.v2"
)

func Persist(fs afero.Fs, in interface{}, file string) error {
	bytes, err := yaml.Marshal(in)
	if err != nil {
		return err
	}
	err = afero.WriteFile(fs, file, bytes, 0o644)
	return err
}

func Read(fs afero.Fs, out interface{}, file string) error {
	content, err := afero.ReadFile(fs, file)
	if err != nil {
		return err
	}
	err = yaml.Unmarshal(content, out)
	if err != nil {
		return err
	}
	return nil
}
