// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package irq

import (
	"fmt"
	"testing"
	"time"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/os"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/executors"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/utils"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

type procMock struct {
	os.Proc
	run       func(command string, args ...string) ([]string, error)
	isRunning func(processName string) bool
}

func (procMock *procMock) RunWithSystemLdPath(
	_ time.Duration, command string, args ...string,
) ([]string, error) {
	return procMock.run(command, args...)
}

func (procMock *procMock) IsRunning(_ time.Duration, processName string) bool {
	return procMock.isRunning(processName)
}

type balanceServiceMock struct {
	BalanceService
	getBannedIRQs func() ([]int, error)
	isRunning     bool
}

func (*balanceServiceMock) BanIRQsAndRestart([]int) error {
	panic("not implemented")
}

func (m *balanceServiceMock) GetBannedIRQs() ([]int, error) {
	return m.getBannedIRQs()
}

func (m *balanceServiceMock) IsRunning() bool {
	return m.isRunning
}

func Test_BalanceService_BanIRQsAndRestart(t *testing.T) {
	running := func(_ string) bool {
		return true
	}
	tests := []struct {
		name                  string
		proc                  os.Proc
		bannedIRQs            []int
		testadataInitFilename string
		configFilename        string
		backupFilename        string
		before                func(afero.Fs)
		assert                func(afero.Fs, string, []string)
	}{
		{
			name: "Shall update the config and then restart IRQ " +
				"balance service with config in /etc/sysconfig/irqbalance & systemd",
			proc: &procMock{
				isRunning: running,
				run: func(command string, args ...string) ([]string, error) {
					require.Equal(t, "systemctl", command)
					require.Equal(t, []string{"try-restart", "irqbalance"}, args)
					return nil, nil
				},
			},
			bannedIRQs:            []int{5, 12, 15},
			testadataInitFilename: "testdata/irqbalance-00-init",
			configFilename:        "/etc/sysconfig/irqbalance",
			backupFilename:        "/etc/sysconfig/irqbalance.vectorized.911727b5a516f8e315b36f97e54facda.bk",
			before: func(fs afero.Fs) {
			},
			assert: func(fs afero.Fs, configFilename string, backupContent []string) {
				require.Equal(t, 2, len(backupContent))
				// Check if IRQs were banned in the file
				fileContent, err := utils.ReadFileLines(fs, configFilename)
				require.NoError(t, err)
				require.Equal(t, 3, len(fileContent))
				require.Equal(t, "IRQBALANCE_ARGS=\" --banirq=5 --banirq=12 --banirq=15\"", fileContent[2])
			},
		},
		{
			name: "Shall add  IRQs to banned list leaving those that were already banned intact",
			proc: &procMock{
				isRunning: running,
				run: func(command string, args ...string) ([]string, error) {
					require.Equal(t, "systemctl", command)
					require.Equal(t, []string{"try-restart", "irqbalance"}, args)
					return nil, nil
				},
			},
			bannedIRQs:            []int{12, 15},
			testadataInitFilename: "testdata/irqbalance-01-init",
			configFilename:        "/etc/sysconfig/irqbalance",
			backupFilename:        "/etc/sysconfig/irqbalance.vectorized.0716a9160d4e30cc6d859900076c4dc8.bk",
			before: func(fs afero.Fs) {
			},
			assert: func(fs afero.Fs, configFilename string, backupContent []string) {
				require.Equal(t, 3, len(backupContent))
				// Check if IRQs were banned in the file
				fileContent, err := utils.ReadFileLines(fs, configFilename)
				require.NoError(t, err)
				require.Equal(t, 3, len(fileContent))
				require.Equal(t, "IRQBALANCE_ARGS=\" --banirq=5 --banirq=12 --banirq=15\"", fileContent[2])
			},
		},
		{
			name: "Shall prevent duplicates in banned IRQs arguments",
			proc: &procMock{
				isRunning: running,
				run: func(command string, args ...string) ([]string, error) {
					require.Equal(t, "systemctl", command)
					require.Equal(t, []string{"try-restart", "irqbalance"}, args)
					return nil, nil
				},
			},
			bannedIRQs:            []int{5, 12, 15},
			testadataInitFilename: "testdata/irqbalance-02-init",
			configFilename:        "/etc/sysconfig/irqbalance",
			backupFilename:        "/etc/sysconfig/irqbalance.vectorized.0716a9160d4e30cc6d859900076c4dc8.bk",
			before: func(fs afero.Fs) {
			},
			assert: func(fs afero.Fs, configFilename string, backupContent []string) {
				require.Equal(t, 3, len(backupContent))
				// Check if IRQs were banned in the file
				fileContent, err := utils.ReadFileLines(fs, configFilename)
				require.NoError(t, err)
				require.Equal(t, 3, len(fileContent))
				require.Equal(t, "IRQBALANCE_ARGS=\" --banirq=5 --banirq=12 --banirq=15\"", fileContent[2])
			},
		},
		{
			name: "Shall update the config and then restart IRQ " +
				"balance service with config in /etc/conf.d/irqbalance & systemd",
			proc: &procMock{
				isRunning: running,
				run: func(command string, args ...string) ([]string, error) {
					require.Equal(t, "systemctl", command)
					require.Equal(t, []string{"try-restart", "irqbalance"}, args)
					return nil, nil
				},
			},
			bannedIRQs:            []int{5, 12, 15},
			testadataInitFilename: "testdata/irqbalance-03-init",
			configFilename:        "/etc/conf.d/irqbalance",
			backupFilename:        "/etc/conf.d/irqbalance.vectorized.911727b5a516f8e315b36f97e54facda.bk",
			before: func(fs afero.Fs) {
				_ = utils.WriteFileLines(fs, []string{"systemd"}, "/proc/1/comm")
			},
			assert: func(fs afero.Fs, configFilename string, backupContent []string) {
				require.Equal(t, 2, len(backupContent))
				// Check if IRQs were banned in the file
				fileContent, err := utils.ReadFileLines(fs, configFilename)
				require.NoError(t, err)
				require.Equal(t, 3, len(fileContent))
				require.Equal(t, "IRQBALANCE_OPTS=\" --banirq=5 --banirq=12 --banirq=15\"", fileContent[2])
			},
		},
		{
			name: "Shall update the config and then restart IRQ " +
				"balance service with config in /etc/conf.d/irqbalance & init daemon",
			proc: &procMock{
				isRunning: running,
				run: func(command string, args ...string) ([]string, error) {
					require.Equal(t, "/etc/init.d/irqbalance", command)
					require.Equal(t, []string{"restart"}, args)
					return nil, nil
				},
			},
			bannedIRQs:            []int{5, 12, 15},
			testadataInitFilename: "testdata/irqbalance-04-init",
			configFilename:        "/etc/conf.d/irqbalance",
			backupFilename:        "/etc/conf.d/irqbalance.vectorized.911727b5a516f8e315b36f97e54facda.bk",
			before: func(fs afero.Fs) {
				_ = utils.WriteFileLines(fs, []string{"init"}, "/proc/1/comm")
			},
			assert: func(fs afero.Fs, configFilename string, backupContent []string) {
				require.Equal(t, 2, len(backupContent))
				// Check if IRQs were banned in the file
				fileContent, err := utils.ReadFileLines(fs, configFilename)
				require.NoError(t, err)
				require.Equal(t, 3, len(fileContent))
				require.Equal(t, "IRQBALANCE_OPTS=\" --banirq=5 --banirq=12 --banirq=15\"", fileContent[2])
			},
		},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprintf("test %02d %s", i, tt.name), func(t *testing.T) {
			fs := afero.NewMemMapFs()
			osfs := afero.NewOsFs()
			testdataLines, err := utils.ReadFileLines(osfs, tt.testadataInitFilename)
			require.NoError(t, err)
			err = utils.WriteFileLines(fs, testdataLines, tt.configFilename)
			require.NoError(t, err)
			tt.before(fs)
			balanceService := NewBalanceService(
				fs,
				tt.proc,
				executors.NewDirectExecutor(),
				time.Duration(10)*time.Second,
			)
			err = balanceService.BanIRQsAndRestart(tt.bannedIRQs)
			require.NoError(t, err)
			// Check if backup is created
			backupFileContent, err := utils.ReadFileLines(fs, tt.backupFilename)
			require.NoError(t, err)
			tt.assert(fs, tt.configFilename, backupFileContent)
		})
	}
}

func Test_balanceService_GetBannedIRQs(t *testing.T) {
	tests := []struct {
		name   string
		before func(afero.Fs)
		want   []int
	}{
		{
			name: "Shall return all banned irq",
			before: func(fs afero.Fs) {
				_ = utils.WriteFileLines(fs,
					[]string{
						"ONE_SHOT=true",
						"#IRQBALANCE_BANNED_CPUS=",
						"IRQBALANCE_ARGS=\"--other --banirq=123 --else=12" +
							"--banirq=34 --banirq=48 --banirq=16\"",
					},
					"/etc/sysconfig/irqbalance")
			},
			want: []int{123, 34, 48, 16},
		},
		{
			name: "Shall return empty list as there are none banned IRQs",
			before: func(fs afero.Fs) {
				_ = utils.WriteFileLines(fs,
					[]string{
						"ONE_SHOT=true",
						"#IRQBALANCE_BANNED_CPUS=",
						"IRQBALANCE_ARGS=\"--other --else --third\"",
					},
					"/etc/sysconfig/irqbalance")
			},
		},
		{
			name: "Shall return empty list as there are no custom options line",
			before: func(fs afero.Fs) {
				_ = utils.WriteFileLines(fs,
					[]string{
						"ONE_SHOT=true",
						"#IRQBALANCE_BANNED_CPUS=",
					},
					"/etc/sysconfig/irqbalance")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := afero.NewMemMapFs()
			balanceService := &balanceService{
				fs:   fs,
				proc: &procMock{},
			}
			tt.before(fs)
			got, err := balanceService.GetBannedIRQs()
			require.NoError(t, err)
			require.Exactly(t, tt.want, got)
		})
	}
}

func TestAreIRQsStaticallyAssigned(t *testing.T) {
	tests := []struct {
		name           string
		irqs           []int
		balanceService BalanceService
		want           bool
	}{
		{
			name: "Shall return true as all requested IRQs are banned",
			balanceService: &balanceServiceMock{
				getBannedIRQs: func() ([]int, error) {
					return []int{12, 56, 87, 34, 46}, nil
				},
				isRunning: true,
			},
			irqs: []int{12, 34},
			want: true,
		},
		{
			name: "Shall return false when some of the requested IRQs are not banned",
			balanceService: &balanceServiceMock{
				getBannedIRQs: func() ([]int, error) {
					return []int{12, 56, 87, 34, 46}, nil
				},
				isRunning: true,
			},
			irqs: []int{12, 134},
			want: false,
		},
		{
			name: "Shall always return true when irqbalance is not running",
			balanceService: &balanceServiceMock{
				isRunning: false,
			},
			irqs: []int{12, 134},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := AreIRQsStaticallyAssigned(tt.irqs, tt.balanceService)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}
