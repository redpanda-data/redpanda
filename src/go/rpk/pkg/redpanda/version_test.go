package redpanda

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVersionFromString(t *testing.T) {
	for _, test := range []struct {
		name   string
		in     string
		exp    Version
		expErr bool
	}{
		{name: "with v", in: "v22.3.4", exp: Version{22, 3, 4}},
		{name: "with v and text", in: "v22.3.4 - 9eefb907c43bf1cfeb0783808c224385c857c0d4-dirty", exp: Version{22, 3, 4}},
		{name: "with v and rc", in: "v22.3.13-rc1", exp: Version{22, 3, 13}},
		{name: "with v, with rc and text", in: "v22.3.13-rc1 - 29e2b111d1d94d6d1f6cc591457ed03119edf0e6-dirty", exp: Version{22, 3, 13}},
		{name: "without v", in: "22.3.4", exp: Version{22, 3, 4}},
		{name: "without v and text", in: "22.3.4 - 9eefb907c43bf1cfeb0783808c224385c857c0d4-dirty", exp: Version{22, 3, 4}},
		{name: "without v and rc", in: "22.3.13-rc1", exp: Version{22, 3, 13}},
		{name: "without v, with rc and text", in: "22.3.13-rc1 - 29e2b111d1d94d6d1f6cc591457ed03119edf0e6-dirty", exp: Version{22, 3, 13}},
		{name: "incomplete", in: "22.3", expErr: true},
		{name: "random string", in: "random", expErr: true},
		{name: "3 digits year", in: "v222.11.1", expErr: true},
		{name: "3 digits feature", in: "v11.222.1", expErr: true},
		{name: "3 digits patch", in: "v11.11.223", expErr: true},
		{name: "non-digit version", in: "AB.C.D", expErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := VersionFromString(test.in)
			if test.expErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.exp, got)
		})
	}
}

func TestVersion_Less(t *testing.T) {
	for _, test := range []struct {
		name string
		a    Version
		b    Version
		exp  bool
	}{
		{"equal", Version{12, 1, 1}, Version{12, 1, 1}, false},
		{"equal empty", Version{}, Version{}, false},
		{"higher year", Version{23, 1, 1}, Version{22, 1, 1}, false},
		{"higher feature", Version{22, 2, 1}, Version{22, 1, 23}, false},
		{"higher patch", Version{23, 4, 12}, Version{23, 4, 9}, false},
		{"lower year", Version{22, 1, 1}, Version{23, 1, 1}, true},
		{"lower feature", Version{22, 1, 23}, Version{22, 2, 23}, true},
		{"lower patch", Version{23, 4, 12}, Version{23, 4, 22}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := test.a.Less(test.b); got != test.exp {
				t.Errorf("IsHigherOrEqual() = %v, want %v", got, test.exp)
			}
		})
	}
}
