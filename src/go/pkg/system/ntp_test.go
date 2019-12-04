package system

import (
	"testing"
	"time"
	"vectorized/pkg/os"
)

type procMock struct {
	os.Proc
	runFunction func(string, ...string) ([]string, error)
}

func (m *procMock) RunWithSystemLdPath(
	_ time.Duration, command string, args ...string,
) ([]string, error) {
	return m.runFunction(command, args...)
}

func Test_ntpQuery_checkWithTimedateCtl(t *testing.T) {
	type fields struct {
		NtpQuery NtpQuery
		proc     os.Proc
	}
	tests := []struct {
		name    string
		fields  fields
		want    bool
		wantErr bool
	}{
		{
			name: "shall return true when clock is synced",
			fields: fields{
				proc: &procMock{
					runFunction: func(string, ...string) ([]string, error) {
						return []string{
							"Local time: Tue 2019-07-23 07:17:38 UTC",
							"Universal time: Tue 2019-07-23 07:17:38 UTC",
							"RTC time: Tue 2019-07-23 07:17:38",
							"Time zone: UTC (UTC, +0000)",
							"System clock synchronized: yes",
							"NTP service: active",
							"RTC in local TZ: no",
						}, nil
					},
				},
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "shall parse timedatectl with legacy output",
			fields: fields{
				proc: &procMock{
					runFunction: func(string, ...string) ([]string, error) {
						return []string{
							"Local time: Tue 2019-07-23 07:17:38 UTC",
							"Universal time: Tue 2019-07-23 07:17:38 UTC",
							"RTC time: Tue 2019-07-23 07:17:38",
							"Time zone: UTC (UTC, +0000)",
							"NTP synchronized: no",
							"NTP service: active",
							"RTC in local TZ: no",
						}, nil
					},
				},
			},
			want:    false,
			wantErr: false,
		},
		{
			name: "shall return an error when there is no" +
				" info on synchronization in timedatectl output",
			fields: fields{
				proc: &procMock{
					runFunction: func(string, ...string) ([]string, error) {
						return []string{
							"Local time: Tue 2019-07-23 07:17:38 UTC",
							"Universal time: Tue 2019-07-23 07:17:38 UTC",
							"RTC time: Tue 2019-07-23 07:17:38",
							"Time zone: UTC (UTC, +0000)",
							"NTP service: active",
							"RTC in local TZ: no",
						}, nil
					},
				},
			},
			want:    false,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := &ntpQuery{
				NtpQuery: tt.fields.NtpQuery,
				proc:     tt.fields.proc,
			}
			got, err := q.checkWithTimedateCtl()
			if (err != nil) != tt.wantErr {
				t.Errorf("timeCtlNtpQuery.IsNtpSynced() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("timeCtlNtpQuery.IsNtpSynced() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCheckNtpqOutput(t *testing.T) {
	tests := []struct {
		name           string
		output         []string
		expectedResult bool
		expectedError  bool
	}{
		{
			name: "should return true if it's synced",
			output: []string{
				"     remote           refid      st t when poll reach   delay   offset  jitter",
				"==============================================================================",
				"*metadata.google 71.79.79.71      2 u  236 1024  377    0.738   -0.405   4.483",
			},
			expectedResult: true,
		},
		{
			name: "should return false if the output is bad",
			output: []string{
				"     remote           refid      st t when poll reach   delay   offset  jitter",
				"==============================================================================",
			},
			expectedResult: false,
			expectedError:  true,
		},
		{
			name: "should return false if the last requests were unsuccessful for each peer",
			output: []string{
				"     remote           refid      st t when poll reach   delay   offset  jitter",
				"==============================================================================",
				"*metadata.google 71.79.79.71      2 u  236 1024  376    0.738   -0.405   4.483",
				"*metadata2.google 71.79.79.72     2 u  236 1024  376    0.738   -0.405   4.483",
			},
			expectedResult: false,
		},
		{
			name: "should return true if at least one the last requests was successful",
			output: []string{
				"     remote           refid      st t when poll reach   delay   offset  jitter",
				"==============================================================================",
				"*metadata.google 71.79.79.71      2 u  236 1024  0    0.738   -0.405   4.483",
				"*metadata2.google 71.79.79.72     2 u  236 1024  376    0.738   -0.405   4.483",
				"*metadata3.google 71.79.79.73     2 u  236 1024  377    0.738   -0.405   4.483",
			},
			expectedResult: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := checkNtpqOutput(tt.output)
			if err != nil && !tt.expectedError {
				t.Errorf("got an unexpected error: %v", err)
			} else if err == nil && tt.expectedError {
				t.Error("expected an error but got nil")
			}
			if res != tt.expectedResult {
				t.Errorf("expected:\n'%t'\ngot:\n'%t'", tt.expectedResult, res)
			}
		})
	}
}
