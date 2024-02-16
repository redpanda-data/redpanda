// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package shelltool

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMdadmCreate(t *testing.T) {
	cmd, err := MdadmCreate("/dev/md0").
		Verbose().
		Force().
		Run().
		HomeHost("any").
		Level("0").
		ChunkSize(4096).
		DeviceNumber(2).
		Devices("/dev/nvme0n1", "/dev/nvme1n1").Build(context.Background())

	assert.NoError(t, err)
	assert.Equal(t, "/usr/sbin/mdadm --create /dev/md0 --verbose --force --run --homehost any --level 0 --chunk 4096 --raid-devices 2 /dev/nvme0n1 /dev/nvme1n1", cmd.String())
}
