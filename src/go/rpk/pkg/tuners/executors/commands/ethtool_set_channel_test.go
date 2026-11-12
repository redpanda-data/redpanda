// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

//go:build linux

package commands_test

import (
	"bufio"
	"bytes"
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/ethtool"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/executors/commands"
	et "github.com/safchain/ethtool"
	"github.com/stretchr/testify/require"
)

type mockEthtool struct {
	ethtool.EthtoolWrapper
	setChannelsFn func(string, et.Channels) (et.Channels, error)
}

func (m *mockEthtool) SetChannels(intf string, ch et.Channels) (et.Channels, error) {
	if m.setChannelsFn != nil {
		return m.setChannelsFn(intf, ch)
	}
	return ch, nil
}

func TestEthtoolSetChannelCmdExecute(t *testing.T) {
	var calledIntf string
	var calledChannels et.Channels
	mock := &mockEthtool{
		setChannelsFn: func(intf string, ch et.Channels) (et.Channels, error) {
			calledIntf = intf
			calledChannels = ch
			return ch, nil
		},
	}
	channels := et.Channels{
		CombinedCount: 4,
		RxCount:       2,
		TxCount:       2,
		OtherCount:    0,
	}
	cmd := commands.NewEthtoolSetChannelCmd(mock, "eth0", channels)
	err := cmd.Execute()
	require.NoError(t, err)
	require.Equal(t, "eth0", calledIntf)
	require.Equal(t, channels, calledChannels)
}

func TestEthtoolSetChannelCmdRender(t *testing.T) {
	channels := et.Channels{
		CombinedCount: 4,
		RxCount:       2,
		TxCount:       2,
		OtherCount:    1,
	}
	cmd := commands.NewEthtoolSetChannelCmd(nil, "eth0", channels)
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	err := cmd.RenderScript(writer)
	require.NoError(t, err)

	expected := "ethtool -L eth0 combined 4 rx 2 tx 2 other 1\n"
	require.Equal(t, expected, buf.String())
}
