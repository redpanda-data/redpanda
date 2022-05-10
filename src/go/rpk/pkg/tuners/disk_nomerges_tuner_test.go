// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package tuners

import (
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/executors"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
)

func TestDeviceNomergesTuner_Tune(t *testing.T) {
	// given
	deviceFeatures := &deviceFeaturesMock{
		getNomergesFeatureFile: func(string) (string, error) {
			return "/sys/devices/pci0000:00/0000:00:1d.0/0000:71:00.0/nvme/fake/queue/nomerges", nil
		},
		getNomerges: func(string) (int, error) {
			return 0, nil
		},
	}
	fs := afero.NewMemMapFs()
	fs.MkdirAll("/sys/devices/pci0000:00/0000:00:1d.0/0000:71:00.0/nvme/fake/queue", 0o644)
	tuner := NewDeviceNomergesTuner(fs, "fake", deviceFeatures, executors.NewDirectExecutor())
	// when
	tuner.Tune()
	// then
	setValue, _ := afero.ReadFile(fs, "/sys/devices/pci0000:00/0000:00:1d.0/0000:71:00.0/nvme/fake/queue/nomerges")
	assert.Equal(t, "2", string(setValue))
}
