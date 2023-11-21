package partitions

import (
	"testing"
)

func Test_extractNTP(t *testing.T) {
	tests := []struct {
		name      string
		topic     string
		partition string
		want0     string
		want1     string
		want2     int
		expErr    bool
	}{
		{
			name:      "t/p:r,r,r",
			topic:     "",
			partition: "foo/0:1,2,3",
			want0:     "kafka",
			want1:     "foo",
			want2:     0,
		},
		{
			name:      "t/p:r,r,r",
			topic:     "",
			partition: "foo/0:1,2,3",
			want0:     "kafka",
			want1:     "foo",
			want2:     0,
		},
		{
			name:      "p:r,r,r",
			topic:     "foo",
			partition: "0:1,2,3",
			want0:     "",
			want1:     "",
			want2:     0,
		},
		{
			name:      "ns/t/p:r,r,r",
			topic:     "",
			partition: "redpanda_internal/tx/0:1,2,3",
			want0:     "redpanda_internal",
			want1:     "tx",
			want2:     0,
		},
		{
			name:      "t/p",
			topic:     "",
			partition: "foo/0",
			want2:     -1,
			expErr:    true,
		},
		{
			name:      "t/t/t:r,r,r",
			topic:     "",
			partition: "foo/bar/foo",
			want2:     -1,
			expErr:    true,
		},
		{
			name:      "topic t/p:r,r,r",
			topic:     "foo",
			partition: "bar/0:1,2,3",
			want2:     -1,
			expErr:    true,
		},
		{
			name:      "topic non-digit:r,r,r",
			topic:     "foo",
			partition: "one:1,2,3",
			want2:     -1,
			expErr:    true,
		},
		{
			name:      "t/non-digit:r,r,r",
			topic:     "",
			partition: "foo/one:1,2,3",
			want2:     -1,
			expErr:    true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got0, got1, got2, err := extractNTP(tt.topic, tt.partition)
			gotErr := err != nil
			if gotErr != tt.expErr {
				t.Errorf("got err? %v (%v), exp err? %v", gotErr, err, tt.expErr)
			}
			if got0 != tt.want0 {
				t.Errorf("extractNTP() got = %v, want %v", got0, tt.want0)
			}
			if got1 != tt.want1 {
				t.Errorf("extractNTP() got1 = %v, want %v", got1, tt.want1)
			}
			if got2 != tt.want2 {
				t.Errorf("extractNTP() got2 = %v, want %v", got2, tt.want2)
			}
		})
	}
}
