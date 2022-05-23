package config

import (
	"testing"

	"gopkg.in/yaml.v3"
)

func TestWeakBool(t *testing.T) {
	type testWeakBool struct {
		Wb weakBool `yaml:"wb"`
	}

	tests := []struct {
		name    string
		data    string
		expBool bool
		expErr  bool
	}{
		{
			name:    "should unmarshal boolean:true",
			data:    "wb: true",
			expBool: true,
			expErr:  false,
		},
		{
			name:    "should unmarshal boolean:false",
			data:    "wb: false",
			expBool: false,
			expErr:  false,
		},
		{
			name:    "should unmarshal int:0",
			data:    "wb: 0",
			expBool: false,
			expErr:  false,
		},
		{
			name:    "should unmarshal int:different from zero",
			data:    "wb: 12",
			expBool: true,
			expErr:  false,
		},
		{
			name:    "should unmarshal string:true",
			data:    `wb: "true"`,
			expBool: true,
			expErr:  false,
		},
		{
			name:    "should unmarshal string:false",
			data:    `wb: "false"`,
			expBool: false,
			expErr:  false,
		},
		{
			name:   "should error with unsupported string",
			data:   `wb: "falsity"`,
			expErr: true,
		},
		{
			name:   "should error with float type",
			data:   `wb: 123.123`,
			expErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ts := testWeakBool{}
			err := yaml.Unmarshal([]byte(test.data), &ts)

			gotErr := err != nil
			if gotErr != test.expErr {
				t.Errorf("input %q: got err? %v, exp err? %v; error: %v",
					test.data, gotErr, test.expErr, err)
				return
			}
			if test.expErr {
				return
			}

			if bool(ts.Wb) != test.expBool {
				t.Errorf("input %q: got %v, expected %v",
					test.data, ts.Wb, test.expBool)
				return
			}
		})
	}
}

func TestWeakInt(t *testing.T) {
	type testWeakInt struct {
		Wi weakInt `yaml:"wi"`
	}
	tests := []struct {
		name   string
		data   string
		expInt int
		expErr bool
	}{
		{
			name:   "should unmarshal normal int types",
			data:   "wi: 1231",
			expInt: 1231,
			expErr: false,
		},
		{
			name:   "should unmarshal empty string as 0",
			data:   `wi: ""`,
			expInt: 0,
			expErr: false,
		},
		{
			name:   "should unmarshal string:-23414",
			data:   `wi: "-23414"`,
			expInt: -23414,
			expErr: false,
		},
		{
			name:   "should unmarshal string:231231",
			data:   `wi: "231231"`,
			expInt: 231231,
			expErr: false,
		},
		{
			name:   "should unmarshal bool:true",
			data:   `wi: true`,
			expInt: 1,
			expErr: false,
		},
		{
			name:   "should unmarshal bool:false",
			data:   `wi: false`,
			expInt: 0,
			expErr: false,
		},
		{
			name:   "should error with not-numeric strings",
			data:   `wi: "123foo234"`,
			expErr: true,
		},
		{
			name:   "should error with float numbers",
			data:   `wi: 123.234`,
			expErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ts := testWeakInt{}
			err := yaml.Unmarshal([]byte(test.data), &ts)

			gotErr := err != nil
			if gotErr != test.expErr {
				t.Errorf("input %q: got err? %v, exp err? %v; error: %v",
					test.data, gotErr, test.expErr, err)
				return
			}
			if test.expErr {
				return
			}

			if int(ts.Wi) != test.expInt {
				t.Errorf("input %q: got %v, expected %v",
					test.data, ts.Wi, test.expInt)
				return
			}
		})
	}
}

func TestWeakString(t *testing.T) {
	type testWeakString struct {
		Ws weakString `yaml:"ws"`
	}
	tests := []struct {
		name   string
		data   string
		expStr string
		expErr bool
	}{
		{
			name:   "should unmarshal normal string types",
			data:   `ws: "hello world"`,
			expStr: "hello world",
			expErr: false,
		},
		{
			name:   "should unmarshal bool:true",
			data:   "ws: true",
			expStr: "1",
			expErr: false,
		},
		{
			name:   "should unmarshal bool:false",
			data:   "ws: false",
			expStr: "0",
			expErr: false,
		},
		{
			name:   "should unmarshal base10 number:231231",
			data:   "ws: 231231",
			expStr: "231231",
			expErr: false,
		},
		{
			name:   "should unmarshal float number:231.231",
			data:   "ws: 231.231",
			expStr: "231.231",
			expErr: false,
		},
		{
			name:   "should error with unsupported types",
			data:   "ws: \n  - 231.231",
			expErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ts := testWeakString{}
			err := yaml.Unmarshal([]byte(test.data), &ts)

			gotErr := err != nil
			if gotErr != test.expErr {
				t.Errorf("input %q: got err? %v, exp err? %v; error: %v",
					test.data, gotErr, test.expErr, err)
				return
			}
			if test.expErr {
				return
			}

			if string(ts.Ws) != test.expStr {
				t.Errorf("input %q: got %v, expected %v",
					test.data, ts.Ws, test.expStr)
				return
			}
		})
	}
}
