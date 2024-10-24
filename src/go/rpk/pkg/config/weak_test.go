package config

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestWeakBool(t *testing.T) {
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
			name: "empty string",
			data: `wb: ""`,
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
			var ts struct {
				Wb weakBool `yaml:"wb"`
			}
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
	for _, test := range []struct {
		name   string
		data   string
		exp    int
		expErr bool
	}{
		{
			name: "normal int types",
			data: "wi: 1231",
			exp:  1231,
		},
		{
			name: "empty string as 0",
			data: `wi: ""`,
			exp:  0,
		},
		{
			name: "string:-23414",
			data: `wi: "-23414"`,
			exp:  -23414,
		},
		{
			name: "string:231231",
			data: `wi: "231231"`,
			exp:  231231,
		},
		{
			name: "bool:true",
			data: `wi: true`,
			exp:  1,
		},
		{
			name: "bool:false",
			data: `wi: false`,
			exp:  0,
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
			var ts struct {
				Wi weakInt `yaml:"wi"`
			}
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

			if int(ts.Wi) != test.exp {
				t.Errorf("input %q: got %v, expected %v",
					test.data, ts.Wi, test.exp)
				return
			}
		})
	}
}

func TestWeakString(t *testing.T) {
	for _, test := range []struct {
		name   string
		data   string
		exp    string
		expErr bool
	}{
		{
			name: "normal string",
			data: `ws: "hello world"`,
			exp:  "hello world",
		},
		{
			name: "bool:true",
			data: "ws: true",
			exp:  "1",
		},
		{
			name: "bool:false",
			data: "ws: false",
			exp:  "0",
		},
		{
			name: "base10 number",
			data: "ws: 231231",
			exp:  "231231",
		},
		{
			name: "float number",
			data: "ws: 231.231",
			exp:  "231.231",
		},
		{
			name:   "should error with unsupported types",
			data:   "ws: \n  - 231.231",
			expErr: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			var ts struct {
				Ws weakString `yaml:"ws"`
			}
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

			if string(ts.Ws) != test.exp {
				t.Errorf("input %q: got %v, expected %v",
					test.data, ts.Ws, test.exp)
				return
			}
		})
	}
}

func TestWeakStringArray(t *testing.T) {
	for _, test := range []struct {
		name   string
		data   string
		exp    []string
		expErr bool
	}{
		{
			name: "single weak string",
			data: `test_array:
  12`,
			exp: []string{"12"},
		},
		{
			name: "list of weak string",
			data: `test_array:
  - 12
  - 12.3
  - "hello"
  - true
`,
			exp: []string{"12", "12.3", "hello", "1"},
		},
		{
			name: "inline weak string",
			data: `test_array: 12`,
			exp:  []string{"12"},
		},
		{
			name: "array of weak strings",
			data: `test_array: [12, true]`,
			exp:  []string{"12", "1"},
		},
		{
			name:   "array with unsupported weak string",
			data:   `test_array: [12, {true}]`,
			expErr: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			var ts struct {
				WsArr weakStringArray `yaml:"test_array"`
			}
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

			require.Equal(t, weakStringArray(test.exp), ts.WsArr)
		})
	}
}

func TestNamedSocketAddresses(t *testing.T) {
	for _, test := range []struct {
		name   string
		data   string
		exp    []SocketAddress
		expErr bool
	}{
		{
			name: "single namedSocketAddress",
			data: "test_api:\n  address: 0.0.0.0\n  port: 80\n",
			exp:  []SocketAddress{{Address: "0.0.0.0", Port: 80}},
		},
		{
			name: "list of 1 namedSocketAddress",
			data: "test_api:\n  - address: 0.0.0.0\n    port: 80\n",
			exp:  []SocketAddress{{Address: "0.0.0.0", Port: 80}},
		},
		{
			name: "list of namedSocketAddress",
			data: `test_api:
  - address: 0.0.0.0
    port: 80
  - address: 0.0.0.1
    port: 81`,
			exp: []SocketAddress{
				{
					Address: "0.0.0.0", Port: 80,
				},
				{
					Address: "0.0.0.1", Port: 81,
				},
			},
		},
		{
			name:   "unsupported types",
			data:   "test_api:\n  - address: 0.0.0.0\n    port: [80]\n",
			expErr: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			var ts struct {
				Sockets socketAddresses `yaml:"test_api"`
			}
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
			require.Equal(t, socketAddresses(test.exp), ts.Sockets)
		})
	}
}

func TestNamedSocketAddressArray(t *testing.T) {
	for _, test := range []struct {
		name   string
		data   string
		exp    []NamedSocketAddress
		expErr bool
	}{
		{
			name: "single namedSocketAddress",
			data: "test_api:\n  address: 0.0.0.0\n  port: 80\n  name: socket\n",
			exp: []NamedSocketAddress{
				{
					Name:    "socket",
					Address: "0.0.0.0",
					Port:    80,
				},
			},
		},
		{
			name: "list of 1 namedSocketAddress",
			data: "test_api:\n  - name: socket\n    address: 0.0.0.0\n    port: 80\n",
			exp: []NamedSocketAddress{
				{
					Name:    "socket",
					Address: "0.0.0.0",
					Port:    80,
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
			exp: []NamedSocketAddress{
				{
					Name:    "socket",
					Address: "0.0.0.0",
					Port:    80,
				},
				{
					Name:    "socket2",
					Address: "0.0.0.1",
					Port:    81,
				},
			},
		},
		{
			name:   "unsupported types",
			data:   "test_api:\n  address: [0.0.0.0]\n  port: 80\n  name: socket\n",
			expErr: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			var ts struct {
				Sockets namedSocketAddresses `yaml:"test_api"`
			}
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
			require.Equal(t, namedSocketAddresses(test.exp), ts.Sockets)
		})
	}
}

func TestNamedAuthNSocketAddressArray(t *testing.T) {
	authNMtlsIdentity := "mtls_identity"
	authNSasl := "sasl"
	authNNOne := "none"
	for _, test := range []struct {
		name   string
		data   string
		exp    []NamedAuthNSocketAddress
		expErr bool
	}{
		{
			name: "single namedAuthNSocketAddress",
			data: "test_api:\n  address: 0.0.0.0\n  port: 80\n  name: socket\n  authentication_method: mtls_identity\n",
			exp: []NamedAuthNSocketAddress{
				{
					Name:    "socket",
					Address: "0.0.0.0",
					Port:    80,
					AuthN:   &authNMtlsIdentity,
				},
			},
		},
		{
			name: "list of 1 namedSocketAddress",
			data: "test_api:\n  - name: socket\n    address: 0.0.0.0\n    port: 80\n    authentication_method: sasl\n",
			exp: []NamedAuthNSocketAddress{
				{
					Name:    "socket",
					Address: "0.0.0.0",
					Port:    80,
					AuthN:   &authNSasl,
				},
			},
		},
		{
			name: "list of namedSocketAddress",
			data: `test_api:
  - name: socket
    address: 0.0.0.0
    port: 80
    authentication_method: mtls_identity
  - name: socket2
    address: 0.0.0.1
    port: 81
    authentication_method: sasl
  - name: socket3
    address: 0.0.0.2
    port: 81
    authentication_method: none
  - name: socket4
    address: 0.0.0.3
    port: 81`,
			exp: []NamedAuthNSocketAddress{
				{
					Name:    "socket",
					Address: "0.0.0.0",
					Port:    80,
					AuthN:   &authNMtlsIdentity,
				},
				{
					Name:    "socket2",
					Address: "0.0.0.1",
					Port:    81,
					AuthN:   &authNSasl,
				},
				{
					Name:    "socket3",
					Address: "0.0.0.2",
					Port:    81,
					AuthN:   &authNNOne,
				},
				{
					Name:    "socket4",
					Address: "0.0.0.3",
					Port:    81,
				},
			},
		},
		{
			name:   "unsupported types",
			data:   "test_api:\n  address: [0.0.0.0]\n  port: 80\n  name: socket\n",
			expErr: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			var ts struct {
				Sockets namedAuthNSocketAddresses `yaml:"test_api"`
			}
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
			require.Equal(t, namedAuthNSocketAddresses(test.exp), ts.Sockets)
		})
	}
}

func TestServerTLSArray(t *testing.T) {
	for _, test := range []struct {
		name   string
		data   string
		exp    []ServerTLS
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
			exp: []ServerTLS{
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
			exp: []ServerTLS{
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
			exp: []ServerTLS{
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
		{
			name: "unsupported types",
			data: `test_api:
  name: server
  enabled: [true]
`,
			expErr: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			var ts struct {
				Servers serverTLSArray `yaml:"test_api"`
			}
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
			require.Equal(t, serverTLSArray(test.exp), ts.Servers)
		})
	}
}

func TestSeedServers(t *testing.T) {
	for _, test := range []struct {
		name   string
		data   string
		exp    []SeedServer
		expErr bool
	}{
		{
			name: "single seed server",
			data: `test_server:
  host:
    address: "0.0.0.1"
    port: 80
`,
			exp: []SeedServer{
				{Host: SocketAddress{"0.0.0.1", 80}},
			},
		},
		{
			name: "list of single seed server with old tabbing",
			data: `test_server:
  - host:
    address: "0.0.0.1"
    port: 80
`,
			exp: []SeedServer{
				{Host: SocketAddress{"0.0.0.1", 80}, untabbed: true},
			},
		},
		{
			name: "list of seed server",
			data: `test_server:
  - host:
      address: "0.0.0.1"
      port: 80
  - host:
      address: "0.0.0.2"
      port: 90
`,
			exp: []SeedServer{
				{Host: SocketAddress{"0.0.0.1", 80}},
				{Host: SocketAddress{"0.0.0.2", 90}},
			},
		},
		{
			name: "unsupported types",
			data: `test_server:
  host:
    address: "0.0.0.1"
    port: [80]
`,
			expErr: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			var ts struct {
				Ss seedServers `yaml:"test_server"`
			}
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
			require.Equal(t, seedServers(test.exp), ts.Ss)
		})
	}
}

func TestSeedServer(t *testing.T) {
	for _, test := range []struct {
		name   string
		data   string
		exp    SeedServer
		expErr bool
	}{
		{
			name: "with node_id",
			data: `test_server:
  node_id: 1
  host:
    address: 192.168.10.1
    port: 33145
`,
			exp: SeedServer{
				Host: SocketAddress{"192.168.10.1", 33145},
			},
		},
		{
			name: "with host",
			data: `test_server:
    host:
        address: "0.0.0.1"
        port: 80
`,
			exp: SeedServer{
				Host: SocketAddress{"0.0.0.1", 80},
			},
		},
		{
			name: "address and port",
			data: `test_server:
    address: "1.0.0.1"
    port: 80
`,
			exp: SeedServer{
				Host:     SocketAddress{"1.0.0.1", 80},
				untabbed: true,
			},
		},
		{
			name: "equal host and address & port",
			data: `test_server:
  host:
    address: "0.0.0.1"
    port: 80
  address: "0.0.0.1"
  port: 80
`,
			exp: SeedServer{
				Host:     SocketAddress{"0.0.0.1", 80},
				untabbed: true,
			},
		},
		{
			name: "host.address and port",
			data: `test_server:
  host:
    address: "0.0.0.1"
  port: 80
`,
			expErr: true,
		},
		{
			name: "address and host.port",
			data: `test_server:
  host:
    port: 80
  address: "0.0.0.1"
`,
			expErr: true,
		},
		{
			name: "address different from host.address",
			data: `test_server:
  host:
    address: "0.0.0.1"
    port: 80
  address: "0.2.0.1"
  port: 80
`,
			expErr: true,
		},
		{
			name: "port different from host.port",
			data: `test_server:
  host:
    address: "0.0.0.1"
    port: 82
  address: "0.0.0.1"
  port: 80
`,
			expErr: true,
		},
		{
			name: "equal host.address and address",
			data: `test_server:
  host:
    address: "0.0.0.1"
  address: "0.0.0.1"
`,
			exp: SeedServer{
				Host:     SocketAddress{Address: "0.0.0.1"},
				untabbed: true,
			},
		},
		{
			name: "equal host.port and port",
			data: `test_server:
  host:
    port: 82
  port: 82
`,
			exp: SeedServer{
				Host:     SocketAddress{Port: 82},
				untabbed: true,
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			var ts struct {
				Ss SeedServer `yaml:"test_server"`
			}
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
			require.Equal(t, test.exp, ts.Ss)
		})
	}
}

func TestConfig_UnmarshalYAML(t *testing.T) {
	intPtr := func(i int) *int { return &i }
	for _, test := range []struct {
		name   string
		data   string
		exp    *RedpandaYaml
		expErr bool
	}{
		{
			name: "Config file with normal types",
			data: `
redpanda:
  data_directory: "var/lib/redpanda/data"
  node_id: 1
  enable_admin_api: true
  admin_api_doc_dir: "/usr/share/redpanda/admin-api-doc"
  admin:
  - address: "0.0.0.0"
    port: 9644
    name: admin
  admin_api_tls:
  - enabled: false
    cert_file: "certs/tls-cert.pem"
  rpc_server:
    address: "0.0.0.0"
    port: 33145
  rpc_server_tls:
  - require_client_auth: false
    truststore_file: "certs/tls-ca.pem"
  advertised_rpc_api:
    address: "0.0.0.0"
    port: 33145
  kafka_api:
  - address: "0.0.0.0"
    name: internal
    port: 9092
  - address: "0.0.0.0"
    name: external
    port: 9093
  kafka_api_tls:
  - name: "external"
    key_file: "certs/tls-key.pem"
  - name: "internal"
    enabled: false
  advertised_kafka_api:
  - address: 0.0.0.0
    name: internal
    port: 9092
  - address: redpanda-0.my.domain.com.
    name: external
    port: 9093
  seed_servers:
  - address: 192.168.0.1
    port: 33145
  rack: "rack-id"
pandaproxy:
  pandaproxy_api:
  - address: "0.0.0.0"
    name: internal
    port: 8082
  - address: "0.0.0.0"
    name: external
    port: 8083
  pandaproxy_api_tls:
  - name: external
    enabled: false
    truststore_file: "truststore_file"
  - name: internal
    enabled: false
  advertised_pandaproxy_api:
  - address: 0.0.0.0
    name: internal
    port: 8082
  - address: "redpanda-rest-0.my.domain.com."
    name: external
    port: 8083
  consumer_instance_timeout_ms: 60000
pandaproxy_client:
  brokers:
  - address: "127.0.0.1"
    port: 9092
  broker_tls:
    require_client_auth: false
    cert_file: "certfile"
  retries: 5
  retry_base_backoff_ms: 100
  sasl_mechanism: "mechanism"
schema_registry:
  schema_registry_api:
  - address: "0.0.0.0"
    name: internal
    port: 8081
  - address: "0.0.0.0"
    name: external
    port: 18081
  schema_registry_replication_factor: 3
  schema_registry_api_tls:
  - name: external
    enabled: false
  - name: internal
    enabled: false
rpk:
  tls:
    key_file: ~/certs/key.pem
  sasl:
    user: user
    password: pass
  additional_start_flags:
    - "--overprovisioned"
  kafka_api:
    brokers:
    - 192.168.72.34:9092
    - 192.168.72.35:9092
    tls:
      key_file: ~/certs/key.pem
    sasl:
      user: user
      password: pass
  admin_api:
    addresses:
    - 192.168.72.34:9644
    - 192.168.72.35:9644
    tls:
      cert_file: ~/certs/admin-cert.pem
      truststore_file: ~/certs/admin-ca.pem
  tune_network: false
  tune_disk_scheduler: false
  tune_cpu: true
  tune_aio_events: false
  tune_clocksource: true
`,
			exp: &RedpandaYaml{
				Redpanda: RedpandaNodeConfig{
					Directory:      "var/lib/redpanda/data",
					ID:             intPtr(1),
					AdminAPIDocDir: "/usr/share/redpanda/admin-api-doc",
					Rack:           "rack-id",
					AdminAPI: []NamedSocketAddress{
						{"0.0.0.0", 9644, "admin"},
					},
					AdminAPITLS: []ServerTLS{
						{Enabled: false, CertFile: "certs/tls-cert.pem"},
					},
					RPCServer:        SocketAddress{"0.0.0.0", 33145},
					AdvertisedRPCAPI: &SocketAddress{"0.0.0.0", 33145},
					KafkaAPI: []NamedAuthNSocketAddress{
						{"0.0.0.0", 9092, "internal", nil},
						{"0.0.0.0", 9093, "external", nil},
					},
					KafkaAPITLS: []ServerTLS{
						{Name: "external", KeyFile: "certs/tls-key.pem"},
						{Name: "internal", Enabled: false},
					},
					AdvertisedKafkaAPI: []NamedSocketAddress{
						{"0.0.0.0", 9092, "internal"},
						{"redpanda-0.my.domain.com.", 9093, "external"},
					},
					SeedServers: []SeedServer{
						{Host: SocketAddress{"192.168.0.1", 33145}, untabbed: true},
					},
					Other: map[string]interface{}{
						"enable_admin_api": true,
						// This one is a slice
						"rpc_server_tls": []interface{}{
							map[string]interface{}{
								"require_client_auth": false,
								"truststore_file":     "certs/tls-ca.pem",
							},
						},
					},
				},
				Pandaproxy: &Pandaproxy{
					PandaproxyAPI: []NamedAuthNSocketAddress{
						{"0.0.0.0", 8082, "internal", nil},
						{"0.0.0.0", 8083, "external", nil},
					},
					PandaproxyAPITLS: []ServerTLS{
						{Name: "external", Enabled: false, TruststoreFile: "truststore_file"},
						{Name: "internal", Enabled: false},
					},
					AdvertisedPandaproxyAPI: []NamedSocketAddress{
						{"0.0.0.0", 8082, "internal"},
						{"redpanda-rest-0.my.domain.com.", 8083, "external"},
					},
					Other: map[string]interface{}{
						"consumer_instance_timeout_ms": 60000,
					},
				},
				PandaproxyClient: &KafkaClient{
					Brokers: []SocketAddress{
						{"127.0.0.1", 9092},
					},
					BrokerTLS: ServerTLS{
						RequireClientAuth: false, CertFile: "certfile",
					},
					SASLMechanism: func() *string { s := "mechanism"; return &s }(),
					Other: map[string]interface{}{
						"retries":               5,
						"retry_base_backoff_ms": 100,
					},
				},
				SchemaRegistry: &SchemaRegistry{
					SchemaRegistryAPI: []NamedAuthNSocketAddress{
						{"0.0.0.0", 8081, "internal", nil},
						{"0.0.0.0", 18081, "external", nil},
					},
					SchemaRegistryAPITLS: []ServerTLS{
						{Name: "external", Enabled: false},
						{Name: "internal", Enabled: false},
					},
					SchemaRegistryReplicationFactor: func() *int { i := 3; return &i }(),
				},
				Rpk: RpkNodeConfig{
					AdditionalStartFlags: []string{"--overprovisioned"},
					KafkaAPI: RpkKafkaAPI{
						Brokers: []string{"192.168.72.34:9092", "192.168.72.35:9092"},
						TLS:     &TLS{KeyFile: "~/certs/key.pem"},
						SASL:    &SASL{User: "user", Password: "pass"},
					},
					AdminAPI: RpkAdminAPI{
						Addresses: []string{"192.168.72.34:9644", "192.168.72.35:9644"},
						TLS:       &TLS{CertFile: "~/certs/admin-cert.pem", TruststoreFile: "~/certs/admin-ca.pem"},
					},
					Tuners: RpkNodeTuners{
						TuneNetwork:       false,
						TuneDiskScheduler: false,
						TuneCPU:           true,
						TuneAioEvents:     false,
						TuneClocksource:   true,
					},
				},
			},
		},
		{
			name: "Config file with omitted node ID",
			data: `
redpanda:
  data_directory: "var/lib/redpanda/data"
  enable_admin_api: true
  admin_api_doc_dir: "/usr/share/redpanda/admin-api-doc"
  admin:
  - address: "0.0.0.0"
    port: 9644
    name: admin
  admin_api_tls:
  - enabled: false
    cert_file: "certs/tls-cert.pem"
  rpc_server:
    address: "0.0.0.0"
    port: 33145
  rpc_server_tls:
  - require_client_auth: false
    truststore_file: "certs/tls-ca.pem"
  advertised_rpc_api:
    address: "0.0.0.0"
    port: 33145
  kafka_api:
  - address: "0.0.0.0"
    name: internal
    port: 9092
  - address: "0.0.0.0"
    name: external
    port: 9093
  kafka_api_tls:
  - name: "external"
    key_file: "certs/tls-key.pem"
  - name: "internal"
    enabled: false
  advertised_kafka_api:
  - address: 0.0.0.0
    name: internal
    port: 9092
  - address: redpanda-0.my.domain.com.
    name: external
    port: 9093
  seed_servers:
  - address: 192.168.0.1
    port: 33145
  rack: "rack-id"
`,
			exp: &RedpandaYaml{
				Redpanda: RedpandaNodeConfig{
					Directory:      "var/lib/redpanda/data",
					ID:             nil,
					AdminAPIDocDir: "/usr/share/redpanda/admin-api-doc",
					Rack:           "rack-id",
					AdminAPI: []NamedSocketAddress{
						{"0.0.0.0", 9644, "admin"},
					},
					AdminAPITLS: []ServerTLS{
						{Enabled: false, CertFile: "certs/tls-cert.pem"},
					},
					RPCServer:        SocketAddress{"0.0.0.0", 33145},
					AdvertisedRPCAPI: &SocketAddress{"0.0.0.0", 33145},
					KafkaAPI: []NamedAuthNSocketAddress{
						{"0.0.0.0", 9092, "internal", nil},
						{"0.0.0.0", 9093, "external", nil},
					},
					KafkaAPITLS: []ServerTLS{
						{Name: "external", KeyFile: "certs/tls-key.pem"},
						{Name: "internal", Enabled: false},
					},
					AdvertisedKafkaAPI: []NamedSocketAddress{
						{"0.0.0.0", 9092, "internal"},
						{"redpanda-0.my.domain.com.", 9093, "external"},
					},
					SeedServers: []SeedServer{
						{Host: SocketAddress{"192.168.0.1", 33145}, untabbed: true},
					},
					Other: map[string]interface{}{
						"enable_admin_api": true,
						// This one is a slice
						"rpc_server_tls": []interface{}{
							map[string]interface{}{
								"require_client_auth": false,
								"truststore_file":     "certs/tls-ca.pem",
							},
						},
					},
				},
			},
		},
		{
			name: "Config file with weak types",
			data: `
redpanda:
  data_directory: "var/lib/redpanda/data"
  node_id: 1
  enable_admin_api: true
  admin_api_doc_dir: "/usr/share/redpanda/admin-api-doc"
  admin:
    address: "0.0.0.0"
    port: 9644
    name: admin
  admin_api_tls:
    enabled: false
    cert_file: "certs/tls-cert.pem"
  rpc_server:
    address: "0.0.0.0"
    port: 33145
  rpc_server_tls:
    require_client_auth: false
    truststore_file: "certs/tls-ca.pem"
  advertised_rpc_api:
    address: "0.0.0.0"
    port: 33145
  kafka_api:
  - address: "0.0.0.0"
    name: internal
    port: "9092"
  - address: "0.0.0.0"
    name: external
    port: "9093"
  kafka_api_tls:
  - name: "external"
    key_file: "certs/tls-key.pem"
  - name: "internal"
    enabled: false
  advertised_kafka_api:
  - address: 0.0.0.0
    name: internal
    port: 9092
  - address: redpanda-0.my.domain.com.
    name: external
    port: 9093
  seed_servers:
  - host:
      address: 192.168.0.1
      port: 33145
  - node_id: "0"
    host:
      address: 192.168.0.1
      port: 33145
  - address: 192.168.0.1
    port: 33145
  rack: "rack-id"
pandaproxy:
  pandaproxy_api:
  - address: "0.0.0.0"
    name: internal
    port: 8082
  - address: "0.0.0.0"
    name: external
    port: 8083
  pandaproxy_api_tls:
  - name: external
    enabled: 0
    truststore_file: "truststore_file"
  - name: internal
    enabled: 0
  advertised_pandaproxy_api:
  - address: 0.0.0.0
    name: internal
    port: 8082
  - address: "redpanda-rest-0.my.domain.com."
    name: external
    port: 8083
  consumer_instance_timeout_ms: 60000
pandaproxy_client:
  brokers:
  - address: "127.0.0.1"
    port: 9092
  broker_tls:
    require_client_auth: false
    cert_file: "certfile"
  retries: 5
  retry_base_backoff_ms: 100
  sasl_mechanism: "mechanism"
schema_registry:
  schema_registry_api:
  - address: "0.0.0.0"
    name: internal
    port: 8081
  - address: "0.0.0.0"
    name: external
    port: 18081
  schema_registry_replication_factor: 3
  schema_registry_api_tls:
  - name: external
    enabled: false
  - name: internal
    enabled: false
rpk:
  tls:
    key_file: ~/certs/key.pem
  sasl:
    user: user
    password: pass
  additional_start_flags: "--overprovisioned"
  kafka_api:
    brokers:
    - 192.168.72.34:9092
    - 192.168.72.35:9092
    tls:
      key_file: ~/certs/key.pem
    sasl:
      user: user
      password: pass
  admin_api:
    addresses:
    - 192.168.72.34:9644
    - 192.168.72.35:9644
    tls:
      cert_file: ~/certs/admin-cert.pem
      truststore_file: ~/certs/admin-ca.pem
  tune_network: false
  tune_disk_scheduler: false
  tune_cpu: 1
  tune_aio_events: false
  tune_clocksource: 1
`,
			exp: &RedpandaYaml{
				Redpanda: RedpandaNodeConfig{
					Directory:      "var/lib/redpanda/data",
					ID:             intPtr(1),
					AdminAPIDocDir: "/usr/share/redpanda/admin-api-doc",
					Rack:           "rack-id",
					AdminAPI: []NamedSocketAddress{
						{"0.0.0.0", 9644, "admin"},
					},
					AdminAPITLS: []ServerTLS{
						{Enabled: false, CertFile: "certs/tls-cert.pem"},
					},
					RPCServer:        SocketAddress{"0.0.0.0", 33145},
					AdvertisedRPCAPI: &SocketAddress{"0.0.0.0", 33145},
					KafkaAPI: []NamedAuthNSocketAddress{
						{"0.0.0.0", 9092, "internal", nil},
						{"0.0.0.0", 9093, "external", nil},
					},
					KafkaAPITLS: []ServerTLS{
						{Name: "external", KeyFile: "certs/tls-key.pem"},
						{Name: "internal", Enabled: false},
					},
					AdvertisedKafkaAPI: []NamedSocketAddress{
						{"0.0.0.0", 9092, "internal"},
						{"redpanda-0.my.domain.com.", 9093, "external"},
					},
					SeedServers: []SeedServer{
						{Host: SocketAddress{"192.168.0.1", 33145}},
						{Host: SocketAddress{"192.168.0.1", 33145}},
						{Host: SocketAddress{"192.168.0.1", 33145}, untabbed: true},
					},
					Other: map[string]interface{}{
						"enable_admin_api": true,
						"rpc_server_tls": map[string]interface{}{
							"require_client_auth": false,
							"truststore_file":     "certs/tls-ca.pem",
						},
					},
				},
				Pandaproxy: &Pandaproxy{
					PandaproxyAPI: []NamedAuthNSocketAddress{
						{"0.0.0.0", 8082, "internal", nil},
						{"0.0.0.0", 8083, "external", nil},
					},
					PandaproxyAPITLS: []ServerTLS{
						{Name: "external", Enabled: false, TruststoreFile: "truststore_file"},
						{Name: "internal", Enabled: false},
					},
					AdvertisedPandaproxyAPI: []NamedSocketAddress{
						{"0.0.0.0", 8082, "internal"},
						{"redpanda-rest-0.my.domain.com.", 8083, "external"},
					},
					Other: map[string]interface{}{
						"consumer_instance_timeout_ms": 60000,
					},
				},
				PandaproxyClient: &KafkaClient{
					Brokers: []SocketAddress{
						{"127.0.0.1", 9092},
					},
					BrokerTLS: ServerTLS{
						RequireClientAuth: false, CertFile: "certfile",
					},
					SASLMechanism: func() *string { s := "mechanism"; return &s }(),
					Other: map[string]interface{}{
						"retries":               5,
						"retry_base_backoff_ms": 100,
					},
				},
				SchemaRegistry: &SchemaRegistry{
					SchemaRegistryAPI: []NamedAuthNSocketAddress{
						{"0.0.0.0", 8081, "internal", nil},
						{"0.0.0.0", 18081, "external", nil},
					},
					SchemaRegistryAPITLS: []ServerTLS{
						{Name: "external", Enabled: false},
						{Name: "internal", Enabled: false},
					},
					SchemaRegistryReplicationFactor: func() *int { i := 3; return &i }(),
				},
				Rpk: RpkNodeConfig{
					AdditionalStartFlags: []string{"--overprovisioned"},
					KafkaAPI: RpkKafkaAPI{
						Brokers: []string{"192.168.72.34:9092", "192.168.72.35:9092"},
						TLS:     &TLS{KeyFile: "~/certs/key.pem"},
						SASL:    &SASL{User: "user", Password: "pass"},
					},
					AdminAPI: RpkAdminAPI{
						Addresses: []string{"192.168.72.34:9644", "192.168.72.35:9644"},
						TLS:       &TLS{CertFile: "~/certs/admin-cert.pem", TruststoreFile: "~/certs/admin-ca.pem"},
					},
					Tuners: RpkNodeTuners{
						TuneNetwork:       false,
						TuneDiskScheduler: false,
						TuneCPU:           true,
						TuneAioEvents:     false,
						TuneClocksource:   true,
					},
				},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			var ts struct {
				RedpandaYaml *RedpandaYaml `yaml:",inline"`
			}
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
			require.Equal(t, test.exp, ts.RedpandaYaml)
		})
	}
}
