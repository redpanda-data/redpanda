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
	"fmt"
	"regexp"
	"strings"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/utils"
	"github.com/spf13/afero"
)

type RuntimeOptions struct {
	optionsMap map[string]bool
}

func ReadRuntineOptions(fs afero.Fs, path string) (*RuntimeOptions, error) {
	optionsMap := make(map[string]bool)
	lines, err := utils.ReadFileLines(fs, path)
	if err != nil {
		return nil, err
	}
	if len(lines) != 1 {
		return nil, fmt.Errorf("Unable to parse options file '%s'", path)
	}
	activeOptionPattern := regexp.MustCompile(`^\[(.*)\]$`)
	options := strings.Fields(lines[0])

	for _, opt := range options {
		matches := activeOptionPattern.FindAllStringSubmatch(opt, -1)
		if matches != nil {
			optionsMap[matches[0][1]] = true
		} else {
			optionsMap[opt] = false
		}
	}

	// if there is only one option it is active
	if len(options) == 1 {
		optionsMap[options[0]] = true
	}
	return &RuntimeOptions{optionsMap: optionsMap}, nil
}

func (r *RuntimeOptions) GetActive() string {
	for opt, isActive := range r.optionsMap {
		if isActive {
			return opt
		}
	}
	return ""
}

func (r *RuntimeOptions) GetAvailable() []string {
	return utils.GetKeys(r.optionsMap)
}
