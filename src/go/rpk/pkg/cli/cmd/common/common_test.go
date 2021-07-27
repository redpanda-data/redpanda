// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package common

import (
	"context"
	"crypto/tls"
	"errors"
	"os"
	"testing"

	"github.com/docker/docker/api/types"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	ccommon "github.com/vectorizedio/redpanda/src/go/rpk/pkg/cli/cmd/container/common"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/config"
)

func TestDeduceBrokers(t *testing.T) {
	tests := []struct {
		name     string
		client   func() (ccommon.Client, error)
		config   func() (*config.Config, error)
		brokers  []string
		before   func()
		cleanup  func()
		expected []string
	}{{
		name: "it should prioritize the flag over the config & containers",
		client: func() (ccommon.Client, error) {
			return &ccommon.MockClient{
				MockContainerInspect: ccommon.MockContainerInspect,
				MockContainerList: func(
					_ context.Context,
					_ types.ContainerListOptions,
				) ([]types.Container, error) {
					return []types.Container{{
						ID: "a",
						Labels: map[string]string{
							"node-id": "0",
						},
					}}, nil
				},
			}, nil
		},
		brokers:  []string{"192.168.34.12:9093"},
		expected: []string{"192.168.34.12:9093"},
	}, {
		name: "it should take the value from the env vars if the flag wasn't passed",
		client: func() (ccommon.Client, error) {
			return &ccommon.MockClient{
				MockContainerInspect: ccommon.MockContainerInspect,
				MockContainerList: func(
					_ context.Context,
					_ types.ContainerListOptions,
				) ([]types.Container, error) {
					return []types.Container{{
						ID: "a",
						Labels: map[string]string{
							"node-id": "0",
						},
					}}, nil
				},
			}, nil
		},
		before: func() {
			os.Setenv("REDPANDA_BROKERS", "192.168.34.12:9093,123.4.5.78:9092")
		},
		cleanup: func() {
			os.Unsetenv("REDPANDA_BROKERS")
		},
		expected: []string{"192.168.34.12:9093", "123.4.5.78:9092"},
	}, {
		name: "it should prioritize the local containers over the config",
		client: func() (ccommon.Client, error) {
			return &ccommon.MockClient{
				MockContainerInspect: ccommon.MockContainerInspect,
				MockContainerList: func(
					_ context.Context,
					_ types.ContainerListOptions,
				) ([]types.Container, error) {
					return []types.Container{{
						ID: "a",
						Labels: map[string]string{
							"node-id": "0",
						},
					}}, nil
				},
			}, nil
		},
		expected: []string{"127.0.0.1:89080"},
	}, {
		name: "it should fall back to the config if the docker client" +
			" can't be init'd",
		client: func() (ccommon.Client, error) {
			return nil, errors.New("The docker client can't be initialized")
		},
		config: func() (*config.Config, error) {
			conf := config.Default()
			conf.Rpk.KafkaApi.Brokers = []string{"192.168.25.88:1235"}
			return conf, nil
		},
		expected: []string{"192.168.25.88:1235"},
	}, {
		name: "it should fall back to the default addr if there's an" +
			" error reading the config",
		config: func() (*config.Config, error) {
			return nil, errors.New("The config file couldn't be read")
		},
		expected: []string{"127.0.0.1:9092"},
	}, {
		name: "it should prioritize rpk.kafka_api.brokers over the redpanda.kafka_api",
		config: func() (*config.Config, error) {
			conf := config.Default()

			conf.Rpk.KafkaApi.Brokers = []string{"192.168.25.87:1234", "192.168.26.98:9092"}

			conf.Redpanda.KafkaApi = []config.NamedSocketAddress{{
				SocketAddress: config.SocketAddress{"somedomain.co", 1234},
			}, {
				SocketAddress: config.SocketAddress{"anotherthing", 4321},
			}}
			return conf, nil
		},
		expected: []string{"192.168.25.87:1234", "192.168.26.98:9092"},
	}, {
		name: "it should take the local broker's cluster addresses if rpk.kafka_api.brokers is empty",
		config: func() (*config.Config, error) {
			conf := config.Default()
			conf.Redpanda.SeedServers = []config.SeedServer{{
				Host: config.SocketAddress{"somedomain.co", 1234},
			}}
			conf.Redpanda.KafkaApi = []config.NamedSocketAddress{{
				SocketAddress: config.SocketAddress{"anotherhost", 4321},
			}}
			return conf, nil
		},
		expected: []string{"somedomain.co:1234", "anotherhost:4321"},
	}, {
		name: "it should return 127.0.0.1:9092 if no config sources yield a brokers list",
		config: func() (*config.Config, error) {
			conf := config.Default()
			conf.Redpanda.SeedServers = nil
			conf.Redpanda.KafkaApi = nil
			return conf, nil
		},
		expected: []string{"127.0.0.1:9092"},
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(st *testing.T) {
			if tt.before != nil {
				tt.before()
			}
			if tt.cleanup != nil {
				defer tt.cleanup()
			}
			client := func() (ccommon.Client, error) {
				return &ccommon.MockClient{}, nil
			}
			config := func() (*config.Config, error) {
				return config.Default(), nil
			}
			brokersList := []string{}
			brokers := &brokersList

			if tt.client != nil {
				client = tt.client
			}
			if tt.config != nil {
				config = tt.config
			}
			if tt.brokers != nil {
				brokers = &tt.brokers
			}
			bs := DeduceBrokers(client, config, brokers)()
			require.Exactly(st, tt.expected, bs)
		})
	}
}

func TestAddKafkaFlags(t *testing.T) {
	var (
		brokers        []string
		configFile     string
		user           string
		password       string
		mechanism      string
		certFile       string
		keyFile        string
		truststoreFile string
	)
	command := func() *cobra.Command {
		parent := &cobra.Command{
			Use: "parent",
			RunE: func(_ *cobra.Command, _ []string) error {
				return nil
			},
		}
		child := &cobra.Command{
			Use: "child",
			RunE: func(_ *cobra.Command, _ []string) error {
				return nil
			},
		}
		parent.AddCommand(child)

		enableTLS := false

		AddKafkaFlags(
			parent,
			&configFile,
			&user,
			&password,
			&mechanism,
			&enableTLS,
			&certFile,
			&keyFile,
			&truststoreFile,
			&brokers,
		)
		return parent
	}

	cmd := command()
	cmd.SetArgs([]string{
		"--config", "arbitraryconfig.yaml",
		"--brokers", "192.168.72.22:9092,localhost:9092",
		"--user", "david",
		"--password", "verysecrethaha",
		"--sasl-mechanism", "some-mechanism",
		"--tls-cert", "cert.pem",
		"--tls-key", "key.pem",
		"--tls-truststore", "truststore.pem",
	})

	err := cmd.Execute()
	require.NoError(t, err)

	require.Exactly(t, "arbitraryconfig.yaml", configFile)
	require.Exactly(t, []string{"192.168.72.22:9092", "localhost:9092"}, brokers)
	require.Exactly(t, "david", user)
	require.Exactly(t, "verysecrethaha", password)
	require.Exactly(t, "some-mechanism", mechanism)
	require.Exactly(t, "cert.pem", certFile)
	require.Exactly(t, "key.pem", keyFile)
	require.Exactly(t, "truststore.pem", truststoreFile)

	// The flags should be available for the children commands too
	cmd = command() // reset it.
	cmd.SetArgs([]string{
		"child", // so that it executes the child command
		"--config", "justaconfig.yaml",
		"--brokers", "192.168.72.23:9092",
		"--brokers", "mykafkahost:9093",
		"--user", "juan",
		"--password", "sosecure",
		"--sasl-mechanism", "whatevs",
		"--tls-cert", "cert1.pem",
		"--tls-key", "key1.pem",
		"--tls-truststore", "truststore1.pem",
	})

	err = cmd.Execute()
	require.NoError(t, err)

	require.Exactly(t, "justaconfig.yaml", configFile)
	require.Exactly(t, []string{"192.168.72.23:9092", "mykafkahost:9093"}, brokers)
	require.Exactly(t, "juan", user)
	require.Exactly(t, "sosecure", password)
	require.Exactly(t, "whatevs", mechanism)
	require.Exactly(t, "cert1.pem", certFile)
	require.Exactly(t, "key1.pem", keyFile)
	require.Exactly(t, "truststore1.pem", truststoreFile)
}

func TestAddAdminAPITLSFlags(t *testing.T) {
	var (
		enableTLS      bool
		certFile       string
		keyFile        string
		truststoreFile string
	)
	command := func() *cobra.Command {
		parent := &cobra.Command{
			Use: "parent",
			RunE: func(_ *cobra.Command, _ []string) error {
				return nil
			},
		}
		child := &cobra.Command{
			Use: "child",
			RunE: func(_ *cobra.Command, _ []string) error {
				return nil
			},
		}
		parent.AddCommand(child)

		AddAdminAPITLSFlags(
			parent,
			&enableTLS,
			&certFile,
			&keyFile,
			&truststoreFile,
		)
		return parent
	}

	cmd := command()
	cmd.SetArgs([]string{
		"--admin-api-tls-cert", "admin-cert.pem",
		"--admin-api-tls-key", "admin-key.pem",
		"--admin-api-tls-truststore", "admin-truststore.pem",
		"--admin-api-tls-enabled",
	})

	err := cmd.Execute()
	require.NoError(t, err)

	require.True(t, enableTLS)
	require.Exactly(t, "admin-cert.pem", certFile)
	require.Exactly(t, "admin-key.pem", keyFile)
	require.Exactly(t, "admin-truststore.pem", truststoreFile)

	// The flags should be available for the children commands too
	cmd = command() // reset it.
	cmd.SetArgs([]string{
		"child", // so that it executes the child command
		"--admin-api-tls-cert", "admin-cert1.pem",
		"--admin-api-tls-key", "admin-key1.pem",
		"--admin-api-tls-truststore", "admin-truststore1.pem",
	})

	err = cmd.Execute()
	require.NoError(t, err)

	require.False(t, enableTLS)
	require.Exactly(t, "admin-cert1.pem", certFile)
	require.Exactly(t, "admin-key1.pem", keyFile)
	require.Exactly(t, "admin-truststore1.pem", truststoreFile)
}

func TestKafkaAuthConfig(t *testing.T) {
	tests := []struct {
		name           string
		user           string
		password       string
		mechanism      string
		config         func() (*config.Config, error)
		before         func()
		cleanup        func()
		expected       *config.SASL
		expectedErrMsg string
	}{{
		name:           "it should fail if user is empty",
		password:       "somethingsecure",
		expectedErrMsg: "empty user. Pass --user or set rpk.kafka_api.sasl.user.",
	}, {
		name:           "it should fail if password is empty",
		user:           "usuario",
		expectedErrMsg: "empty password. Pass --password or set rpk.kafka_api.sasl.password.",
	}, {
		name:           "it should fail if both user and password are empty",
		expectedErrMsg: ErrNoCredentials.Error(),
	}, {
		name:      "it should fail if the mechanism isn't supported",
		user:      "usuario",
		password:  "contraseño",
		mechanism: "super-crypto-3000",
		expectedErrMsg: "unsupported mechanism 'super-crypto-3000'. Pass --sasl-mechanism or set rpk.kafka_api.sasl.mechanism." +
			" Supported: SCRAM-SHA-256, SCRAM-SHA-512.",
	}, {
		name:      "it should support SCRAM-SHA-256",
		user:      "usuario",
		password:  "contraseño",
		mechanism: "SCRAM-SHA-256",
		expected:  &config.SASL{User: "usuario", Password: "contraseño", Mechanism: "SCRAM-SHA-256"},
	}, {
		name:      "it should support SCRAM-SHA-512",
		user:      "usuario",
		password:  "contraseño",
		mechanism: "SCRAM-SHA-512",
		expected:  &config.SASL{User: "usuario", Password: "contraseño", Mechanism: "SCRAM-SHA-512"},
	}, {
		name: "it should pick up the values from env vars if the vars' values is empty",
		before: func() {
			os.Setenv("REDPANDA_SASL_USERNAME", "ringo")
			os.Setenv("REDPANDA_SASL_PASSWORD", "octopussgarden66")
			os.Setenv("REDPANDA_SASL_MECHANISM", "SCRAM-SHA-512")
		},
		cleanup: func() {
			os.Unsetenv("REDPANDA_SASL_USERNAME")
			os.Unsetenv("REDPANDA_SASL_PASSWORD")
			os.Unsetenv("REDPANDA_SASL_MECHANISM")
		},
		expected: &config.SASL{User: "ringo", Password: "octopussgarden66", Mechanism: "SCRAM-SHA-512"},
	}, {
		name:      "it should give priority to values set through the flags",
		user:      "usuario",
		password:  "contraseño",
		mechanism: "SCRAM-SHA-512",
		before: func() {
			os.Setenv("REDPANDA_SASL_USERNAME", "ringo")
			os.Setenv("REDPANDA_SASL_PASSWORD", "octopussgarden66")
			os.Setenv("REDPANDA_SASL_MECHANISM", "SCRAM-SHA-512")
		},
		cleanup: func() {
			os.Unsetenv("REDPANDA_SASL_USERNAME")
			os.Unsetenv("REDPANDA_SASL_PASSWORD")
			os.Unsetenv("REDPANDA_SASL_MECHANISM")
		},
		// Disregards the env vars' values
		expected: &config.SASL{User: "usuario", Password: "contraseño", Mechanism: "SCRAM-SHA-512"},
	}, {
		name: "it should fall back to the config if no values were set through flags or env vars",
		config: func() (*config.Config, error) {
			conf := config.Default()
			conf.Rpk.KafkaApi.SASL = &config.SASL{
				User:      "config-user",
				Password:  "config-password",
				Mechanism: "SCRAM-SHA-512",
			}
			return conf, nil
		},
		expected: &config.SASL{User: "config-user", Password: "config-password", Mechanism: "SCRAM-SHA-512"},
	}, {
		name: "it should fall back to the deprecated config fields if rpk.kafka_api.sasl is missing",
		config: func() (*config.Config, error) {
			conf := config.Default()
			conf.Rpk.SASL = &config.SASL{
				User:      "depr-config-user",
				Password:  "depr-config-password",
				Mechanism: "SCRAM-SHA-256",
			}
			return conf, nil
		},
		expected: &config.SASL{User: "depr-config-user", Password: "depr-config-password", Mechanism: "SCRAM-SHA-256"},
	}, {
		name: "it should build a complete config from values set through different sources",
		user: "flag-user",
		before: func() {
			os.Setenv("REDPANDA_SASL_PASSWORD", "verysecurenoonewillknow")
			// REDPANDA_SASL_USERNAME is also set, but since user is passed
			// directly, that one will be picked up.
			os.Setenv("REDPANDA_SASL_USERNAME", "dang,Iwillbeignored")
		},
		cleanup: func() {
			os.Unsetenv("REDPANDA_SASL_PASSWORD")
			os.Unsetenv("REDPANDA_SASL_USERNAME")
		},
		config: func() (*config.Config, error) {
			conf := config.Default()
			conf.Rpk.SASL = &config.SASL{
				Mechanism: "SCRAM-SHA-512",
			}
			return conf, nil
		},
		expected: &config.SASL{User: "flag-user", Password: "verysecurenoonewillknow", Mechanism: "SCRAM-SHA-512"},
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(st *testing.T) {
			if tt.before != nil {
				tt.before()
			}
			if tt.cleanup != nil {
				defer tt.cleanup()
			}
			confClosure := func() (*config.Config, error) {
				return config.Default(), nil
			}
			if tt.config != nil {
				confClosure = tt.config
			}
			closure := KafkaAuthConfig(&tt.user, &tt.password, &tt.mechanism, confClosure)
			res, err := closure()
			if tt.expectedErrMsg != "" {
				require.EqualError(st, err, tt.expectedErrMsg)
				return
			}
			require.Exactly(st, tt.expected, res)
		})
	}
}

func TestBuildTLSConfig(t *testing.T) {
	truststoreContents := `-----BEGIN CERTIFICATE-----
MIIDDjCCAfagAwIBAgIUZHm6D1aJYtmVEV1X23m+oK9EJgcwDQYJKoZIhvcNAQEL
BQAwLTETMBEGA1UECgwKVmVjdG9yaXplZDEWMBQGA1UEAwwNVmVjdG9yaXplZCBD
QTAeFw0yMTA0MDYwMjMzMjNaFw0yMjA0MDYwMjMzMjNaMC0xEzARBgNVBAoMClZl
Y3Rvcml6ZWQxFjAUBgNVBAMMDVZlY3Rvcml6ZWQgQ0EwggEiMA0GCSqGSIb3DQEB
AQUAA4IBDwAwggEKAoIBAQDZN7ytDxWGvxprVvydv0hwuD3hGcZFbJaeDXrIInGd
RGK8zr/zoq3oAgw1ZW0OabID3OKs9tezKGM5wPUvzGfU94qhi1ot040Utw+Jf2tQ
GA3T1X+VHRTUlqDVVnIvhcAiy21bMUMVuFl4QtJnWx9ZljkCFo8IIh/3Sq88ORDl
gzfM3cK1kk+60uKzXNvgK8ShrZ0GYsbmxncxhbdr2O7mSdVcO6x0tbmNDLc5Hx+w
34z2hRHReavw3KDFfaIdHZE2tSQ4xh25F9aZQiPsTaGzPtGozLl7Ck1p9Ew4d+MO
EXd51gGeIdP6EUD8exOMfRq2B6/p18HyjMsuKQKhjx2NAgMBAAGjJjAkMA4GA1Ud
DwEB/wQEAwIC5DASBgNVHRMBAf8ECDAGAQH/AgEBMA0GCSqGSIb3DQEBCwUAA4IB
AQCse+GLdlBi77fOgDgX8dby2PVZiSoW8cE1uRJzaXvw9mrz83RW2SxnW3och6Mm
cixyv+taomZfYNM2wbOzEpkI0QEcV9CF/9Sx5RQVKsoAQkjkmzpHUeRp5ha/VAYq
KWVCj9Ej+1y8dE0+AvltyymRbcMKUSk3vXK6HmzCGn2XAVz1WBQHQHXe0vwQGAXu
Tq7VoNh+LNEud7ro5Hwi5aiDA1B7HZum6u2MvU5KwGY3txDve1Jn0bWOa8J3HjIN
wHAv/4PAPweXflPAVHkUR4VOslBdZo/tAcSbG/Zr3cneBt7VYnPyO+IAe7S54u9g
eKlljKHOgHw7gXOzsvWTVQbm
-----END CERTIFICATE-----`

	certContents := `-----BEGIN CERTIFICATE-----
MIIDDTCCAfWgAwIBAgIBATANBgkqhkiG9w0BAQsFADAtMRMwEQYDVQQKDApWZWN0
b3JpemVkMRYwFAYDVQQDDA1WZWN0b3JpemVkIENBMB4XDTIxMDQwNjAyMzMyM1oX
DTIyMDQwNjAyMzMyM1owFTETMBEGA1UECgwKVmVjdG9yaXplZDCCASIwDQYJKoZI
hvcNAQEBBQADggEPADCCAQoCggEBANU4vIalSYOb0Oui2ZaDjDxW075L2erSvpZg
7QWxp5WTmDWglsy/sXxj0aub1HDu6dyZPFSIOc4TunjM+0aU7aLR7aK7k9sCxqs8
FdRx4VwAQ/ERuJzzrOtw5qb/Wr345K7VmQqO3nfIixc4mipgssWWAFUOCvz3hAod
qCDKwrsQS3innG6LUUEDgyeSpjopSue7luHCaHL0KwNP/AlzLNhxTJP8MqKDgFX8
w8oXwlIcmzOUO2RPq08Odspe5g9sfhiOo0uzRvPix7nrg4e6p+zSAnUp6BTGxSL4
eHRQDTbtXGsAv1ZonbDpAQwhSgszypyG2YeGkamdH8cS6hOy6ycCAwEAAaNQME4w
DgYDVR0PAQH/BAQDAgWgMB0GA1UdJQQWMBQGCCsGAQUFBwMBBggrBgEFBQcDAjAd
BgNVHREBAf8EEzARgglsb2NhbGhvc3SHBH8AAAEwDQYJKoZIhvcNAQELBQADggEB
ABVaMl14UyUPUw8hYtRusEE8Q/jt2LVJyX/0OQQkTJpiGaBfbsT7XHIuIgE7EJgk
zIBT9FigLvnsyndjC+tR5xXjNaMzK5kzpT29BN+v2wpRUhvldJ4nrVxWlMIS8AKj
TIUnkfUSHIDn+h2WCF3Kwr6zalDkG1tTLxDtJoZzgqTBORozkGmj5O31QSIKy9Y1
2DMqvkvvUUrTA4V8aNI6pkUwhR56ln2ilYmoccdyO/Xxwe2Vq4lBlXehhGT7tMd0
5FWiLjwxu1UzElnCTpay+y4snQpRDf46JMZ3UkW66GNCZulImIU9kalWOuPE3bh4
bsyMIZfcs4exHOV0g2EenLk=
-----END CERTIFICATE-----`

	keyContents := `-----BEGIN RSA PRIVATE KEY-----
MIIEpQIBAAKCAQEA1Ti8hqVJg5vQ66LZloOMPFbTvkvZ6tK+lmDtBbGnlZOYNaCW
zL+xfGPRq5vUcO7p3Jk8VIg5zhO6eMz7RpTtotHtoruT2wLGqzwV1HHhXABD8RG4
nPOs63Dmpv9avfjkrtWZCo7ed8iLFziaKmCyxZYAVQ4K/PeECh2oIMrCuxBLeKec
botRQQODJ5KmOilK57uW4cJocvQrA0/8CXMs2HFMk/wyooOAVfzDyhfCUhybM5Q7
ZE+rTw52yl7mD2x+GI6jS7NG8+LHueuDh7qn7NICdSnoFMbFIvh4dFANNu1cawC/
VmidsOkBDCFKCzPKnIbZh4aRqZ0fxxLqE7LrJwIDAQABAoIBAQDQ3yuPmwtQ6arX
qkgMsgEGeugiWpu29YvONFT8ZvQMCvHoVtBi8sYjXIVg3t5VYzWk7Fe1V12JCrp4
7BSbJ/lCrvNjnu1Qdn+37rxTyNtDDN+BoCKBXhPe8FKC9VMnFlKvEn9BYIN+Q+49
aS1cpi16cV8R8xfAh5fJcRPqS7ZHF/1rhUqkcoV956h5RtQy3k/BIVpJydbrb17U
xEhiCFyRbCIEEnX2hTRFrRt4hEIwa2YceWMsY8ddCngtS/i4UGLsaYLCMrTgtvwS
H9uI1q0gL5tcp7qo4MQo9BRtbiazV70MofabF19tapDlbPoqcggN/sTi4VQzFWsx
YKShZhIBAoGBAPA5ZaH6pg76O+E47kPbugx2V6y8YongvBOe+MXkwXG3samLUhN9
63th3qhDOqqwUN8FgJYqATbn6lsvVnIhoJ1EK126grVeZme7FCTswjHKd2gaVAaz
GBJ7ejMMoHzw1IGSXxnZ69XFN4xUSAiCeq58LWl3PJmtpt+vZ2ZZrA+HAoGBAOM5
YJP7YzGPG/OX4EVttGzcMCigTUTKZwH4aNTzJm2KDHiS3uiW6XMgMsY8H057XZbT
QjB7QnokRUuVqD/Zugh2bHZcHiJNT9re/TZwqXPXh2B4fY0ZOSAImMpsXWdfJSKK
anIroNF4v5gw+4zqtEBE8Zc7Ct+gbvfPp9qdbu9hAoGAXkAWyQufdYbmUYJVsVgX
UeZolcQ/4RrEj+oybupGn4hT81JPPIiOCJWol1nxPaD5ydbN0ZzfZxxszaPwBc19
x9ZEMX0I5YIJKa+zwp0FwCVQ3g5eY1aHHlFF65uLqBmRNtkn6OugZPoAxlUXAge3
fJgJ9TQsGZuROngGWJjcMicCgYEAxPd12pFt2QX+6tfalxST9FGihXT/xgPV6wVU
ilQEGawzR0m5ZNF8qEle+iwfzz5tUFLs623NoGdUkkK2yDKKas+NEcSkcoOmF0p5
IPnkSgCo311TKD6XIEeTetUY2oTFgf2ObE2ZaDtNijXbuLmzaorZCYkq0dMWnkYp
cP5LrcECgYEA4+Sk/uND5dDc1TXRKGaiu9t2DscUpQCwk5PVOMMA0iwBJBodt85o
nDpxxecx1Mh1+0V46bnrYrIrAGurqCbQN8B5EsB9dcLZ3bg86KRwP3ZbQNxKPIqo
T16cNmHSk5jIiR6odFmV6KPjvXhjFTUYxOIjFIWNItOhXBrBxG3NyVE=
-----END RSA PRIVATE KEY-----`

	certVarName := "RP_TEST_CERT"
	keyVarName := "RP_TEST_KEY"
	truststoreVarName := "RP_TEST_TRUSTSTORE"

	tests := []struct {
		name           string
		keyFile        string
		certFile       string
		truststoreFile string
		before         func(afero.Fs)
		cleanup        func()
		defaultVal     func() (*config.TLS, error)
		expectedErrMsg string
	}{{
		name: "it should return the default value provided if none are set",
		defaultVal: func() (*config.TLS, error) {
			return &config.TLS{
				CertFile:       "default-cert.pem",
				KeyFile:        "default-key.pem",
				TruststoreFile: "default-trust.pem",
			}, nil
		},
		before: func(fs afero.Fs) {
			afero.WriteFile(fs, "default-cert.pem", []byte(certContents), 0755)
			afero.WriteFile(fs, "default-key.pem", []byte(keyContents), 0755)
			afero.WriteFile(fs, "default-trust.pem", []byte(truststoreContents), 0755)
		},
	}, {
		name:           "it should fail if certFile is present but keyFile is empty",
		certFile:       "cert.pem",
		truststoreFile: "trust.pem",
		expectedErrMsg: "if a TLS client certificate is set, then its key must be passed to enable TLS authentication",
	}, {
		name:           "it should fail if keyFile is present but certFile is empty",
		keyFile:        "key.pem",
		truststoreFile: "trust.pem",
		expectedErrMsg: "if a TLS client certificate key is set, then its certificate must be passed to enable TLS authentication",
	}, {
		name:           "it should build the config with only a truststore",
		truststoreFile: "trust.pem",
		before: func(fs afero.Fs) {
			afero.WriteFile(fs, "trust.pem", []byte(truststoreContents), 0755)
		},
	}, {
		name:           "it should build the config with all fields",
		certFile:       "cert.pem",
		keyFile:        "key.pem",
		truststoreFile: "trust.pem",
		before: func(fs afero.Fs) {
			afero.WriteFile(fs, "cert.pem", []byte(certContents), 0755)
			afero.WriteFile(fs, "key.pem", []byte(keyContents), 0755)
			afero.WriteFile(fs, "trust.pem", []byte(truststoreContents), 0755)
		},
	}, {
		name: "it should pick up the values from env vars if the vars' values is empty",
		before: func(fs afero.Fs) {
			cert, key, truststore := "./node.crt", "./node.key", "./ca.crt"
			os.Setenv(certVarName, cert)
			os.Setenv(keyVarName, key)
			os.Setenv(truststoreVarName, truststore)

			afero.WriteFile(fs, cert, []byte(certContents), 0755)
			afero.WriteFile(fs, key, []byte(keyContents), 0755)
			afero.WriteFile(fs, truststore, []byte(truststoreContents), 0755)
		},
		cleanup: func() {
			os.Unsetenv(certVarName)
			os.Unsetenv(keyVarName)
			os.Unsetenv(truststoreVarName)
		},
	}, {
		name:           "it should give priority to values set through the flags",
		certFile:       "cert.pem",
		keyFile:        "key.pem",
		truststoreFile: "trust.pem",
		before: func(fs afero.Fs) {
			afero.WriteFile(fs, "cert.pem", []byte(certContents), 0755)
			afero.WriteFile(fs, "key.pem", []byte(keyContents), 0755)
			afero.WriteFile(fs, "trust.pem", []byte(truststoreContents), 0755)
		},
	}, {
		name: "it should return the given default value if no values are set",
		defaultVal: func() (*config.TLS, error) {
			return &config.TLS{
				CertFile:       "certificate.pem",
				KeyFile:        "cert.key",
				TruststoreFile: "ca.pem",
			}, nil
		},
		before: func(fs afero.Fs) {
			afero.WriteFile(fs, "certificate.pem", []byte(certContents), 0755)
			afero.WriteFile(fs, "cert.key", []byte(keyContents), 0755)
			afero.WriteFile(fs, "ca.pem", []byte(truststoreContents), 0755)
		},
	},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(st *testing.T) {
			fs := afero.NewMemMapFs()
			if tt.before != nil {
				tt.before(fs)
			}
			if tt.cleanup != nil {
				defer tt.cleanup()
			}
			defaultVal := tt.defaultVal
			if tt.defaultVal == nil {
				defaultVal = func() (*config.TLS, error) {
					return nil, nil
				}
			}

			enableTLS := false

			_, err := buildTLS(
				fs,
				&enableTLS,
				&tt.certFile,
				&tt.keyFile,
				&tt.truststoreFile,
				certVarName,
				keyVarName,
				truststoreVarName,
				defaultVal,
			)
			if tt.expectedErrMsg != "" {
				require.EqualError(st, err, tt.expectedErrMsg)
				return
			}
			require.NoError(st, err)
		})
	}
}

func TestCreateAdmin(t *testing.T) {
	tests := []struct {
		name           string
		brokers        func() []string
		configuration  func() (*config.Config, error)
		tlsConfig      func() (*tls.Config, error)
		authConfig     func() (*config.SASL, error)
		expectedErrMsg string
	}{{
		name: "the returned closure should fail if configuration fails",
		configuration: func() (*config.Config, error) {
			return nil, errors.New("No config found here")
		},
		expectedErrMsg: "No config found here",
	}, {
		name: "the returned closure should fail if tlsConfig fails",
		tlsConfig: func() (*tls.Config, error) {
			return nil, errors.New("bad tls conf")
		},
		expectedErrMsg: "bad tls conf",
	}, {
		name: "the returned closure should fail if authConfig returns an error other than ErrNoCredentials",
		authConfig: func() (*config.SASL, error) {
			return nil, errors.New("Some bad error")
		},
		expectedErrMsg: "Some bad error",
	}, {
		name: "the returned closure shouldn't fail due to authConfig returning ErrNoCredentials",
		authConfig: func() (*config.SASL, error) {
			return nil, ErrNoCredentials
		},
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(st *testing.T) {
			brokers := func() []string {
				return []string{}
			}
			if tt.brokers != nil {
				brokers = tt.brokers
			}

			configuration := func() (*config.Config, error) {
				return config.Default(), nil
			}
			if tt.configuration != nil {
				configuration = tt.configuration
			}

			tlsConfig := func() (*tls.Config, error) {
				return nil, nil
			}
			if tt.tlsConfig != nil {
				tlsConfig = tt.tlsConfig
			}

			authConfig := func() (*config.SASL, error) {
				return nil, nil
			}
			if tt.authConfig != nil {
				authConfig = tt.authConfig
			}

			fn := CreateAdmin(brokers, configuration, tlsConfig, authConfig)
			_, err := fn()
			if tt.expectedErrMsg != "" {
				require.EqualError(st, err, tt.expectedErrMsg)
				return
			}
			// The admin always fails to initialize because the brokers closure
			// always returns an empty slice.
			require.EqualError(st, err, "couldn't connect to redpanda at . Try using --brokers to specify other brokers to connect to.")
		})
	}
}
func TestCreateClient(t *testing.T) {
	tests := []struct {
		name           string
		brokers        func() []string
		configuration  func() (*config.Config, error)
		tlsConfig      func() (*tls.Config, error)
		authConfig     func() (*config.SASL, error)
		expectedErrMsg string
	}{{
		name: "the returned closure should fail if configuration fails",
		configuration: func() (*config.Config, error) {
			return nil, errors.New("No config found here")
		},
		expectedErrMsg: "No config found here",
	}, {
		name: "the returned closure should fail if tlsConfig fails",
		tlsConfig: func() (*tls.Config, error) {
			return nil, errors.New("bad tls conf")
		},
		expectedErrMsg: "bad tls conf",
	}, {
		name: "the returned closure should fail if authConfig returns an error other than ErrNoCredentials",
		authConfig: func() (*config.SASL, error) {
			return nil, errors.New("Some bad error")
		},
		expectedErrMsg: "Some bad error",
	}, {
		name: "the returned closure shouldn't fail due to authConfig returning ErrNoCredentials",
		authConfig: func() (*config.SASL, error) {
			return nil, ErrNoCredentials
		},
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(st *testing.T) {
			brokers := func() []string {
				return []string{}
			}
			if tt.brokers != nil {
				brokers = tt.brokers
			}

			configuration := func() (*config.Config, error) {
				return config.Default(), nil
			}
			if tt.configuration != nil {
				configuration = tt.configuration
			}

			tlsConfig := func() (*tls.Config, error) {
				return nil, nil
			}
			if tt.tlsConfig != nil {
				tlsConfig = tt.tlsConfig
			}

			authConfig := func() (*config.SASL, error) {
				return nil, nil
			}
			if tt.authConfig != nil {
				authConfig = tt.authConfig
			}

			fn := CreateClient(brokers, configuration, tlsConfig, authConfig)
			_, err := fn()
			if tt.expectedErrMsg != "" {
				require.EqualError(st, err, tt.expectedErrMsg)
				return
			}
			// The client always fails to initialize because the brokers closure
			// always returns an empty slice.
			require.EqualError(st, err, "couldn't connect to redpanda at . Try using --brokers to specify other brokers to connect to.")
		})
	}
}
