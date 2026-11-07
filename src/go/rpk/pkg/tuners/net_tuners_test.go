// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

//go:build linux

package tuners_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/osutil"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/rpkutil"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/ethtool"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/executors"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/hwloc"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/irq"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/network"
	et "github.com/safchain/ethtool"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func mockNetTunersFactory(
	fs afero.Fs, exec executors.Executor,
) (tuners.NetTunersFactory, error) {
	procFile := irq.NewProcFile(fs)
	proc := osutil.NewProc()
	timeout := 1 * time.Second
	hwlocCmd := hwloc.NewHwLocCmd(proc, timeout)
	eth, err := ethtool.NewEthtoolWrapper()
	if err != nil {
		return nil, err
	}
	return tuners.NewNetTunersFactory(
		fs,
		config.RpkNodeConfig{},
		procFile,
		irq.NewDeviceInfo(fs, procFile),
		eth,
		irq.NewBalanceService(fs, proc, exec, timeout),
		irq.NewCPUMasks(fs, hwlocCmd, exec),
		exec,
		proc,
		"",
	), nil
}

func TestSynBacklogTuner(t *testing.T) {
	tests := []struct {
		name           string
		before         func(fs afero.Fs) error
		expectChange   bool
		expected       int
		expectedErrMsg string
	}{
		{
			name: "it shouldn't do anything if current >= reference",
			before: func(fs afero.Fs) error {
				_, err := rpkutil.WriteBytes(
					fs,
					[]byte("20000000"),
					network.SynBacklogFile,
				)
				return err
			},
		},
		{
			name: "it shouldn't do anything if current == reference",
			before: func(fs afero.Fs) error {
				_, err := rpkutil.WriteBytes(
					fs,
					[]byte("4096"),
					network.SynBacklogFile,
				)
				return err
			},
		},
		{
			name: "it should set the value if current < reference",
			before: func(fs afero.Fs) error {
				_, err := rpkutil.WriteBytes(
					fs,
					[]byte("12"),
					network.SynBacklogFile,
				)
				return err
			},
			expectChange: true,
			expected:     4096,
		},
		{
			name:           "it should fail if the file is missing",
			expectedErrMsg: network.SynBacklogFile,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(st *testing.T) {
			const scriptPath = "/tune.sh"
			fs := afero.NewMemMapFs()
			exec := executors.NewScriptRenderingExecutor(fs, scriptPath)
			if tt.before != nil {
				err := tt.before(fs)
				require.NoError(st, err)
			}
			f, err := mockNetTunersFactory(fs, exec)
			require.NoError(st, err)
			tuner := f.NewSynBacklogTuner()
			res := tuner.Tune()
			if tt.expectedErrMsg != "" {
				require.Contains(st, res.Error().Error(), tt.expectedErrMsg)
				return
			}
			require.NoError(st, res.Error())
			contents, err := afero.ReadFile(fs, scriptPath)
			require.NoError(st, err)
			expected := rpTuningHeader
			if tt.expectChange {
				expected = expected + fmt.Sprintf(`echo '%d' > %s
`,
					tt.expected,
					network.SynBacklogFile,
				)
			}
			require.Exactly(st, expected, string(contents))
		})
	}
}

func TestListenBacklogTuner(t *testing.T) {
	tests := []struct {
		name           string
		before         func(fs afero.Fs) error
		expectChange   bool
		expected       int
		expectedErrMsg string
	}{
		{
			name: "it shouldn't do anything if current >= reference",
			before: func(fs afero.Fs) error {
				_, err := rpkutil.WriteBytes(
					fs,
					[]byte("20000000"),
					network.ListenBacklogFile,
				)
				return err
			},
		},
		{
			name: "it shouldn't do anything if current == reference",
			before: func(fs afero.Fs) error {
				_, err := rpkutil.WriteBytes(
					fs,
					[]byte("4096"),
					network.ListenBacklogFile,
				)
				return err
			},
		},
		{
			name: "it should set the value if current < reference",
			before: func(fs afero.Fs) error {
				_, err := rpkutil.WriteBytes(
					fs,
					[]byte("12"),
					network.ListenBacklogFile,
				)
				return err
			},
			expectChange: true,
			expected:     4096,
		},
		{
			name:           "it should fail if the file is missing",
			expectedErrMsg: network.ListenBacklogFile,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(st *testing.T) {
			const scriptPath = "/tune.sh"
			fs := afero.NewMemMapFs()
			exec := executors.NewScriptRenderingExecutor(fs, scriptPath)
			if tt.before != nil {
				err := tt.before(fs)
				require.NoError(st, err)
			}
			f, err := mockNetTunersFactory(fs, exec)
			require.NoError(st, err)
			tuner := f.NewListenBacklogTuner()
			res := tuner.Tune()
			if tt.expectedErrMsg != "" {
				require.Contains(st, res.Error().Error(), tt.expectedErrMsg)
				return
			}
			require.NoError(st, res.Error())
			contents, err := afero.ReadFile(fs, scriptPath)
			require.NoError(st, err)
			expected := rpTuningHeader
			if tt.expectChange {
				expected = expected + fmt.Sprintf(`echo '%d' > %s
`,
					tt.expected,
					network.ListenBacklogFile,
				)
			}
			require.Exactly(st, expected, string(contents))
		})
	}
}

func TestYaml(t *testing.T) {
	yml := `cpusets:
    irq_mode: dedicated
    redpanda_cpuset: 0-2
    redpanda_cpuset_size: 3
    interrupts_cpuset: "3"
    interrupts_cpuset_size: 1
`

	var cfg tuners.NodeTunerState
	err := yaml.Unmarshal([]byte(yml), &cfg)
	require.NoError(t, err)

	require.Equal(t, irq.Dedicated, cfg.Cpusets.IrqMode)
	require.Equal(t, "0-2", cfg.Cpusets.RedpandaCpuset)
	require.Equal(t, 3, cfg.Cpusets.RedpandaCpusetSize)
	require.Equal(t, "3", cfg.Cpusets.IrqCpuset)
	require.Equal(t, 1, cfg.Cpusets.IrqCpusetSize)

	data, err := yaml.Marshal(&cfg)
	require.NoError(t, err)
	require.Equal(t, yml, string(data))
}

type mockChannelsEthtool struct {
	ethtool.EthtoolWrapper
	currentChannels   et.Channels
	setChannelsCalled bool
	lastSetChannels   et.Channels
}

func (m *mockChannelsEthtool) GetChannels(string) (et.Channels, error) {
	return m.currentChannels, nil
}

func (m *mockChannelsEthtool) SetChannels(_ string, ch et.Channels) (et.Channels, error) {
	m.setChannelsCalled = true
	m.lastSetChannels = ch
	return ch, nil
}

type mockNic struct {
	network.Nic
	name             string
	supportsLowering bool
}

func (m *mockNic) Name() string {
	return m.name
}

func (m *mockNic) SupportsRxTxQueueLowering() (bool, error) {
	return m.supportsLowering, nil
}

func mockNetTunersFactoryWithEthtool(
	fs afero.Fs, exec executors.Executor, eth ethtool.EthtoolWrapper,
) tuners.NetTunersFactory {
	procFile := irq.NewProcFile(fs)
	proc := osutil.NewProc()
	timeout := 1 * time.Second
	hwlocCmd := hwloc.NewHwLocCmd(proc, timeout)
	return tuners.NewNetTunersFactory(
		fs,
		config.RpkNodeConfig{},
		procFile,
		irq.NewDeviceInfo(fs, procFile),
		eth,
		irq.NewBalanceService(fs, proc, exec, timeout),
		irq.NewCPUMasks(fs, hwlocCmd, exec),
		exec,
		proc,
		"",
	)
}

func TestRxTxQueueCountTuner_QueueReductionScriptModeRejects(t *testing.T) {
	const scriptPath = "/tune.sh"
	fs := afero.NewMemMapFs()
	exec := executors.NewScriptRenderingExecutor(fs, scriptPath)

	eth := &mockChannelsEthtool{
		currentChannels: et.Channels{
			CombinedCount: 8,
			MaxCombined:   8,
		},
	}
	f := mockNetTunersFactoryWithEthtool(fs, exec, eth)

	nic := &mockNic{name: "eth0", supportsLowering: true}
	effectiveConfig := network.EffectiveNicConfig{
		Mode:           irq.Dedicated,
		IRQCPUMaskSize: 2,
	}

	tuner := f.NewRxTxQueueCountTuner([]network.Nic{nic}, effectiveConfig)
	res := tuner.Tune()

	require.True(t, res.IsFailed())
	require.Error(t, res.Error())
	require.Contains(t, res.Error().Error(), "cannot generate tuning script when NIC 'eth0' channel count must change")
	require.Contains(t, res.Error().Error(), "current: combined=8")
	require.Contains(t, res.Error().Error(), "target: combined=2")

	// Ensure the generated script does not reference queues that would be removed (e.g. rx-2 through rx-7)
	contents, err := afero.ReadFile(fs, scriptPath)
	require.NoError(t, err)
	for q := 2; q < 8; q++ {
		require.NotContains(t, string(contents), fmt.Sprintf("rx-%d", q))
	}
}

func TestRxTxQueueCountTuner_QueueReductionDirectModeSucceeds(t *testing.T) {
	fs := afero.NewMemMapFs()
	exec := executors.NewDirectExecutor()

	eth := &mockChannelsEthtool{
		currentChannels: et.Channels{
			CombinedCount: 8,
			MaxCombined:   8,
		},
	}
	f := mockNetTunersFactoryWithEthtool(fs, exec, eth)

	nic := &mockNic{name: "eth0", supportsLowering: true}
	effectiveConfig := network.EffectiveNicConfig{
		Mode:           irq.Dedicated,
		IRQCPUMaskSize: 2,
	}

	tuner := f.NewRxTxQueueCountTuner([]network.Nic{nic}, effectiveConfig)
	res := tuner.Tune()

	require.False(t, res.IsFailed())
	require.NoError(t, res.Error())
	require.True(t, eth.setChannelsCalled)
	require.Equal(t, uint32(2), eth.lastSetChannels.CombinedCount)
}

func TestRxTxQueueCountTuner_AlreadyMatchingChannelsScriptMode(t *testing.T) {
	const scriptPath = "/tune.sh"
	fs := afero.NewMemMapFs()
	exec := executors.NewScriptRenderingExecutor(fs, scriptPath)

	eth := &mockChannelsEthtool{
		currentChannels: et.Channels{
			CombinedCount: 2,
			MaxCombined:   8,
		},
	}
	f := mockNetTunersFactoryWithEthtool(fs, exec, eth)

	nic := &mockNic{name: "eth0", supportsLowering: true}
	effectiveConfig := network.EffectiveNicConfig{
		Mode:           irq.Dedicated,
		IRQCPUMaskSize: 2,
	}

	tuner := f.NewRxTxQueueCountTuner([]network.Nic{nic}, effectiveConfig)
	res := tuner.Tune()

	require.False(t, res.IsFailed())
	require.NoError(t, res.Error())
}
