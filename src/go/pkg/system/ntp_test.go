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
