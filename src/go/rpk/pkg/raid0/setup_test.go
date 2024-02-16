// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

//go:build integration

package raid0

import (
	"context"
	"fmt"
	"math/rand"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zaptest"
)

// random creats a random string.
func random(length int) string {
	rand.Seed(time.Now().UnixNano())
	b := make([]byte, length+2)
	rand.Read(b)
	return fmt.Sprintf("%x", b)[2 : length+2]
}

// setup configures 3 loopback disk devices backed by files.
func setup(t *testing.T) []string {
	tmpDir := t.TempDir()
	var devices []string

	for i := 0; i <= 2; i++ {
		cmd := exec.Command(
			"/usr/bin/dd",
			"if=/dev/zero",
			fmt.Sprintf("of=%s/disk%d", tmpDir, i),
			"bs=1M",
			"count=32",
		)
		out, err := cmd.CombinedOutput()
		t.Logf("%q: %s", cmd.String(), string(out))

		assert.NoError(t, err)

		cmd = exec.Command(
			"/usr/sbin/losetup",
			"--show",
			"--find", fmt.Sprintf("%s/disk%d", tmpDir, i),
		)
		out, err = cmd.CombinedOutput()
		t.Logf("%q: %s", cmd.String(), string(out))
		assert.NoError(t, err)

		devices = append(devices, strings.TrimSpace(string(out)))
	}
	return devices
}

// teardown removes loopback devices and their corresponding disk files.
func teardown(t *testing.T, devices []string) {
	for _, d := range devices {
		cmd := exec.Command(
			"/usr/sbin/losetup",
			"--detach", d,
		)
		out, err := cmd.CombinedOutput()
		t.Logf("%q: %s", cmd.String(), string(out))
		assert.NoError(t, err)
	}
}

func TestRaidSetupHappyPath(t *testing.T) {
	devices := setup(t)
	defer teardown(t, devices)

	mountPath := t.TempDir()
	devicePath := fmt.Sprintf("/dev/md/rp%s", random(3))

	raid0 := NewRedpandaRAID(
		afero.NewOsFs(),
		zaptest.NewLogger(t),
		DevicePath(devicePath),
		MemberDevicePaths(devices),
		MountPath(mountPath),
	)

	err := raid0.Setup(context.Background())
	assert.NoError(t, err)

	cmd := exec.Command("/usr/bin/umount", mountPath)
	out, err := cmd.CombinedOutput()
	t.Logf("%q: %s", cmd.String(), string(out))

	cmd = exec.Command("/usr/sbin/mdadm", "--stop", devicePath)
	out, err = cmd.CombinedOutput()
	t.Logf("%q: %s", cmd.String(), string(out))
	assert.NoError(t, err)

	cmd = exec.Command("/usr/bin/systemctl", "stop", raid0.MountUnitName())
	out, err = cmd.CombinedOutput()
	t.Logf("%q: %s", cmd.String(), string(out))

	cmd = exec.Command("/usr/bin/systemctl", "disable", raid0.MountUnitName())
	out, err = cmd.CombinedOutput()
	t.Logf("%q: %s", cmd.String(), string(out))

	cmd = exec.Command("/usr/bin/systemctl", "daemon-reload")
	out, err = cmd.CombinedOutput()
	t.Logf("%q: %s", cmd.String(), string(out))

	cmd = exec.Command("/usr/sbin/update-initramfs", "-u")
	out, err = cmd.CombinedOutput()
	t.Logf("%q: %s", cmd.String(), string(out))
}
