// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package system

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type procMock struct {
	runFunction func(string, ...string) ([]string, error)
}

func (m *procMock) RunWithSystemLdPath(
	_ time.Duration, command string, args ...string,
) ([]string, error) {
	return m.runFunction(command, args...)
}

func (*procMock) IsRunning(_ time.Duration, _ string) bool {
	return true
}

func Test_ntpQuery_checkWithTimedateCtl(t *testing.T) {
	tests := []struct {
		name		string
		ntpOutput	[]string
		want		bool
		wantErr		bool
	}{
		{
			name:	"shall return true when clock is synced",
			ntpOutput: []string{
				"Local time: Tue 2019-07-23 07:17:38 UTC",
				"Universal time: Tue 2019-07-23 07:17:38 UTC",
				"RTC time: Tue 2019-07-23 07:17:38",
				"Time zone: UTC (UTC, +0000)",
				"System clock synchronized: yes",
				"NTP service: active",
				"RTC in local TZ: no",
			},
			want:	true,
		},
		{
			name:	"shall parse timedatectl with legacy output",
			ntpOutput: []string{
				"Local time: Tue 2019-07-23 07:17:38 UTC",
				"Universal time: Tue 2019-07-23 07:17:38 UTC",
				"RTC time: Tue 2019-07-23 07:17:38",
				"Time zone: UTC (UTC, +0000)",
				"NTP synchronized: no",
				"NTP service: active",
				"RTC in local TZ: no",
			},
			want:	false,
		},
		{
			name: "shall return an error when there is no" +
				" info on synchronization in timedatectl output",
			ntpOutput: []string{
				"Local time: Tue 2019-07-23 07:17:38 UTC",
				"Universal time: Tue 2019-07-23 07:17:38 UTC",
				"RTC time: Tue 2019-07-23 07:17:38",
				"Time zone: UTC (UTC, +0000)",
				"NTP service: active",
				"RTC in local TZ: no",
			},
			want:		false,
			wantErr:	true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			proc := &procMock{
				func(_ string, _ ...string) ([]string, error) {
					return tt.ntpOutput, nil
				},
			}
			q := &ntpQuery{proc: proc}
			got, err := q.checkWithTimedateCtl()
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestCheckNtpqOutput(t *testing.T) {
	tests := []struct {
		name		string
		output		[]string
		expectedResult	bool
		expectedError	bool
	}{
		{
			name:	"should return true if it's synced",
			output: []string{
				"     remote           refid      st t when poll reach   delay   offset  jitter",
				"==============================================================================",
				"*metadata.google 71.79.79.71      2 u  236 1024  377    0.738   -0.405   4.483",
			},
			expectedResult:	true,
		},
		{
			name:	"should return false if the output is bad",
			output: []string{
				"     remote           refid      st t when poll reach   delay   offset  jitter",
				"==============================================================================",
			},
			expectedResult:	false,
			expectedError:	true,
		},
		{
			name:	"should return false if the last requests were unsuccessful for each peer",
			output: []string{
				"     remote           refid      st t when poll reach   delay   offset  jitter",
				"==============================================================================",
				"*metadata.google 71.79.79.71      2 u  236 1024  376    0.738   -0.405   4.483",
				"*metadata2.google 71.79.79.72     2 u  236 1024  376    0.738   -0.405   4.483",
			},
			expectedResult:	false,
		},
		{
			name:	"should return true if at least one the last requests was successful",
			output: []string{
				"     remote           refid      st t when poll reach   delay   offset  jitter",
				"==============================================================================",
				"*metadata.google 71.79.79.71      2 u  236 1024  0    0.738   -0.405   4.483",
				"*metadata2.google 71.79.79.72     2 u  236 1024  376    0.738   -0.405   4.483",
				"*metadata3.google 71.79.79.73     2 u  236 1024  377    0.738   -0.405   4.483",
			},
			expectedResult:	true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := checkNtpqOutput(tt.output)
			if tt.expectedError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Exactly(t, tt.expectedResult, res)
		})
	}
}
