package config

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestWeakBool(t *testing.T) {
	type testWeakBool struct {
		Wb weakBool `yaml:"wb"`
	}
	for _, test := range []struct {
		name   string
		data   string
		exp    bool
		expErr bool
	}{
		{
			name: "boolean:true",
			data: "wb: true",
			exp:  true,
		},
		{
			name: "boolean:false",
			data: "wb: false",
			exp:  false,
		},
		{
			name: "int:0",
			data: "wb: 0",
			exp:  false,
		},
		{
			name: "non-zero int",
			data: "wb: 12",
			exp:  true,
		},
		{
			name: "string:true",
			data: `wb: "true"`,
			exp:  true,
		},
		{
			name: "string:false",
			data: `wb: "false"`,
			exp:  false,
		},
		{
			name:   "error with unsupported string",
			data:   `wb: "falsity"`,
			expErr: true,
		},
		{
			name:   "error with float type",
			data:   `wb: 123.123`,
			expErr: true,
		},
	} {
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

			if bool(ts.Wb) != test.exp {
				t.Errorf("input %q: got %v, expected %v",
					test.data, ts.Wb, test.exp)
				return
			}
		})
	}
}

func TestWeakInt(t *testing.T) {
	type testWeakInt struct {
		Wi weakInt `yaml:"wi"`
	}
	for _, test := range []struct {
		name   string
		data   string
		expInt int
		expErr bool
	}{
		{
			name:   "normal int types",
			data:   "wi: 1231",
			expInt: 1231,
		},
		{
			name:   "empty string as 0",
			data:   `wi: ""`,
			expInt: 0,
		},
		{
			name:   "string:-23414",
			data:   `wi: "-23414"`,
			expInt: -23414,
		},
		{
			name:   "string:231231",
			data:   `wi: "231231"`,
			expInt: 231231,
		},
		{
			name:   "bool:true",
			data:   `wi: true`,
			expInt: 1,
		},
		{
			name:   "bool:false",
			data:   `wi: false`,
			expInt: 0,
		},
		{
			name:   "error with non-numeric strings",
			data:   `wi: "123foo234"`,
			expErr: true,
		},
		{
			name:   "error with float numbers",
			data:   `wi: 123.234`,
			expErr: true,
		},
	} {
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
	for _, test := range []struct {
		name   string
		data   string
		expStr string
		expErr bool
	}{
		{
			name:   "normal string",
			data:   `ws: "hello world"`,
			expStr: "hello world",
		},
		{
			name:   "bool:true",
			data:   "ws: true",
			expStr: "1",
		},
		{
			name:   "bool:false",
			data:   "ws: false",
			expStr: "0",
		},
		{
			name:   "base10 number",
			data:   "ws: 231231",
			expStr: "231231",
		},
		{
			name:   "float number",
			data:   "ws: 231.231",
			expStr: "231.231",
		},
		{
			name:   "should error with unsupported types",
			data:   "ws: \n  - 231.231",
			expErr: true,
		},
	} {
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

func TestNamedSocketAddressArray(t *testing.T) {
	type testNamedSocketAddressArray struct {
		Sockets NamedSocketAddresses `yaml:"test_api"`
	}
	for _, test := range []struct {
		name   string
		data   string
		expArr []NamedSocketAddress
		expErr bool
	}{
		{
			name: "single namedSocketAddress",
			data: "test_api:\n  address: 0.0.0.0\n  port: 80\n  name: socket\n",
			expArr: []NamedSocketAddress{
				{
					Name:          "socket",
					SocketAddress: SocketAddress{Address: "0.0.0.0", Port: 80},
				},
			},
		},
		{
			name: "list of 1 namedSocketAddress",
			data: "test_api:\n  - name: socket\n    address: 0.0.0.0\n    port: 80\n",
			expArr: []NamedSocketAddress{
				{
					Name:          "socket",
					SocketAddress: SocketAddress{Address: "0.0.0.0", Port: 80},
				},
			},
		},
		{
			name: "list of namedSocketAddress",
			data: `test_api:
  - name: socket
    address: 0.0.0.0
    port: 80
  - name: socket2
    address: 0.0.0.1
    port: 81`,
			expArr: []NamedSocketAddress{
				{
					Name:          "socket",
					SocketAddress: SocketAddress{Address: "0.0.0.0", Port: 80},
				},
				{
					Name:          "socket2",
					SocketAddress: SocketAddress{Address: "0.0.0.1", Port: 81},
				},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ts := testNamedSocketAddressArray{}
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
			require.Equal(t, NamedSocketAddresses(test.expArr), ts.Sockets)
		})
	}
}

func TestServerTLSArray(t *testing.T) {
	type testServerTLSArray struct {
		Servers ServerTLSArray `yaml:"test_api"`
	}
	for _, test := range []struct {
		name   string
		data   string
		expArr []ServerTLS
		expErr bool
	}{
		{
			name: "single serverTLS",
			data: `test_api:
  name: server
  key_file: "/etc/certs/cert.key"
  truststore_file: "/etc/certs/ca.crt"
  cert_file: "/etc/certs/cert.crt"
  enabled: true
  require_client_auth: true
`,
			expArr: []ServerTLS{
				{
					Name:              "server",
					KeyFile:           "/etc/certs/cert.key",
					TruststoreFile:    "/etc/certs/ca.crt",
					CertFile:          "/etc/certs/cert.crt",
					Enabled:           true,
					RequireClientAuth: true,
				},
			},
		},
		{
			name: "list of 1 serverTLS",
			data: `test_api:
  - name: server
    key_file: "/etc/certs/cert.key"
    truststore_file: "/etc/certs/ca.crt"
    cert_file: "/etc/certs/cert.crt"
    enabled: true
    require_client_auth: true
`,
			expArr: []ServerTLS{
				{
					Name:              "server",
					KeyFile:           "/etc/certs/cert.key",
					TruststoreFile:    "/etc/certs/ca.crt",
					CertFile:          "/etc/certs/cert.crt",
					Enabled:           true,
					RequireClientAuth: true,
				},
			},
		},
		{
			name: "list of serverTLS",
			data: `test_api:
  - name: server
    key_file: "/etc/certs/cert.key"
    truststore_file: "/etc/certs/ca.crt"
    cert_file: "/etc/certs/cert.crt"
    enabled: true
    require_client_auth: true
  - name: server2
    key_file: "/etc/certs/cert2.key"
    truststore_file: "/etc/certs/ca2.crt"
    cert_file: "/etc/certs/cert2.crt"
    enabled: false
    require_client_auth: true
`,
			expArr: []ServerTLS{
				{
					Name:              "server",
					KeyFile:           "/etc/certs/cert.key",
					TruststoreFile:    "/etc/certs/ca.crt",
					CertFile:          "/etc/certs/cert.crt",
					Enabled:           true,
					RequireClientAuth: true,
				},
				{
					Name:              "server2",
					KeyFile:           "/etc/certs/cert2.key",
					TruststoreFile:    "/etc/certs/ca2.crt",
					CertFile:          "/etc/certs/cert2.crt",
					Enabled:           false,
					RequireClientAuth: true,
				},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ts := testServerTLSArray{}
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
			require.Equal(t, ServerTLSArray(test.expArr), ts.Servers)
		})
	}
}
