// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package config

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/exp/maps"
	"golang.org/x/term"

	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"

	rpknet "github.com/redpanda-data/redpanda/src/go/rpk/pkg/net"
)

const (
	// The following flags are currently used in some areas of rpk
	// (and ideally will be deprecated / removed in the future).
	FlagEnableTLS = "tls-enabled"
	FlagTLSCA     = "tls-truststore"
	FlagTLSCert   = "tls-cert"
	FlagTLSKey    = "tls-key"
	FlagSASLUser  = "user"

	// The following flags and env vars are used in `rpk cloud`. We will
	// always support them, but they are also duplicated by -X auth.*.
	FlagClientID     = "client-id"
	FlagClientSecret = "client-secret"

	// This block contains names X flags that are used for backcompat.
	// All new X flags are defined directly into the xflags slice.
	xKafkaBrokers       = "brokers"
	xKafkaTLSEnabled    = "tls.enabled"
	xKafkaCACert        = "tls.ca"
	xKafkaClientCert    = "tls.cert"
	xKafkaClientKey     = "tls.key"
	xKafkaSASLMechanism = "sasl.mechanism"
	xKafkaSASLUser      = "user"
	xKafkaSASLPass      = "pass"
	xAdminHosts         = "admin.hosts"
	xAdminTLSEnabled    = "admin.tls.enabled"
	xAdminCACert        = "admin.tls.ca"
	xAdminClientCert    = "admin.tls.cert"
	xAdminClientKey     = "admin.tls.key"
	xCloudClientID      = "cloud.client_id"
	xCloudClientSecret  = "cloud.client_secret"
)

const (
	xkindProfile   = iota // configuration for the current profile
	xkindCloudAuth        // configuration for the current cloud_auth
	xkindDefault          // configuration for rpk.yaml defaults
)

type xflag struct {
	path        string
	testExample string
	kind        uint8
	parse       func(string, *RpkYaml) error
}

func splitCommaIntoStrings(in string, dst *[]string) error {
	*dst = nil
	split := strings.Split(in, ",")
	for _, on := range split {
		on = strings.TrimSpace(on)
		if len(on) == 0 {
			return fmt.Errorf("invalid empty value in %q", in)
		}
		*dst = append(*dst, on)
	}
	return nil
}

func mkKafkaTLS(k *RpkKafkaAPI) *TLS {
	if k.TLS == nil {
		k.TLS = new(TLS)
	}
	return k.TLS
}

func mkSASL(k *RpkKafkaAPI) *SASL {
	if k.SASL == nil {
		k.SASL = new(SASL)
	}
	return k.SASL
}

func mkAdminTLS(a *RpkAdminAPI) *TLS {
	if a.TLS == nil {
		a.TLS = new(TLS)
	}
	return a.TLS
}

var xflags = map[string]xflag{
	xKafkaBrokers: {
		"kafka_api.brokers",
		"127.8.8.4,126.1.3.4:9093,localhost",
		xkindProfile,
		func(v string, y *RpkYaml) error {
			p := y.Profile(y.CurrentProfile)
			return splitCommaIntoStrings(v, &p.KafkaAPI.Brokers)
		},
	},
	xKafkaTLSEnabled: {
		"kafka_api.tls.enabled",
		"true",
		xkindProfile,
		func(v string, y *RpkYaml) error {
			p := y.Profile(y.CurrentProfile)
			mkKafkaTLS(&p.KafkaAPI)
			return nil
		},
	},
	xKafkaCACert: {
		"kafka_api.tls.ca_file",
		"/path.pem",
		xkindProfile,
		func(v string, y *RpkYaml) error {
			p := y.Profile(y.CurrentProfile)
			mkKafkaTLS(&p.KafkaAPI).TruststoreFile = v
			return nil
		},
	},
	xKafkaClientCert: {
		"kafka_api.tls.cert_file",
		"unrooted/path.pem",
		xkindProfile,
		func(v string, y *RpkYaml) error {
			p := y.Profile(y.CurrentProfile)
			mkKafkaTLS(&p.KafkaAPI).CertFile = v
			return nil
		},
	},
	xKafkaClientKey: {
		"kafka_api.tls.key_file",
		"fileonly.pem",
		xkindProfile,
		func(v string, y *RpkYaml) error {
			p := y.Profile(y.CurrentProfile)
			mkKafkaTLS(&p.KafkaAPI).KeyFile = v
			return nil
		},
	},

	xKafkaSASLMechanism: {
		"kafka_api.sasl.mechanism",
		"scram-sha-256",
		xkindProfile,
		func(v string, y *RpkYaml) error {
			p := y.Profile(y.CurrentProfile)
			mkSASL(&p.KafkaAPI).Mechanism = v
			return nil
		},
	},
	xKafkaSASLUser: {
		"kafka_api.sasl.user",
		"username",
		xkindProfile,
		func(v string, y *RpkYaml) error {
			p := y.Profile(y.CurrentProfile)
			mkSASL(&p.KafkaAPI).User = v
			return nil
		},
	},
	xKafkaSASLPass: {
		"kafka_api.sasl.password",
		"23oi4jdkslfnoi23j",
		xkindProfile,
		func(v string, y *RpkYaml) error {
			p := y.Profile(y.CurrentProfile)
			mkSASL(&p.KafkaAPI).Password = v
			return nil
		},
	},

	xAdminHosts: {
		"admin_api.addresses",
		"example.com",
		xkindProfile,
		func(v string, y *RpkYaml) error {
			p := y.Profile(y.CurrentProfile)
			return splitCommaIntoStrings(v, &p.AdminAPI.Addresses)
		},
	},
	xAdminTLSEnabled: {
		"admin_api.tls.enabled",
		"false",
		xkindProfile,
		func(v string, y *RpkYaml) error {
			p := y.Profile(y.CurrentProfile)
			mkAdminTLS(&p.AdminAPI)
			return nil
		},
	},
	xAdminCACert: {
		"admin_api.tls.ca_file",
		"noextension",
		xkindProfile,
		func(v string, y *RpkYaml) error {
			p := y.Profile(y.CurrentProfile)
			mkAdminTLS(&p.AdminAPI).TruststoreFile = v
			return nil
		},
	},
	xAdminClientCert: {
		"admin_api.tls.cert_file",
		"cert.pem",
		xkindProfile,
		func(v string, y *RpkYaml) error {
			p := y.Profile(y.CurrentProfile)
			mkAdminTLS(&p.AdminAPI).CertFile = v
			return nil
		},
	},
	xAdminClientKey: {
		"admin_api.tls.key_file",
		"key.pem",
		xkindProfile,
		func(v string, y *RpkYaml) error {
			p := y.Profile(y.CurrentProfile)
			mkAdminTLS(&p.AdminAPI).KeyFile = v
			return nil
		},
	},

	xCloudClientID: {
		"client_id",
		"anystring",
		xkindCloudAuth,
		func(v string, y *RpkYaml) error {
			auth := y.Auth(y.CurrentCloudAuth)
			auth.ClientID = v
			return nil
		},
	},
	xCloudClientSecret: {
		"client_secret",
		"anysecret",
		xkindCloudAuth,
		func(v string, y *RpkYaml) error {
			auth := y.Auth(y.CurrentCloudAuth)
			auth.ClientSecret = v
			return nil
		},
	},

	"defaults.prompt": {
		"defaults.prompt",
		"bg-red \"%n\"",
		xkindDefault,
		func(v string, y *RpkYaml) error {
			y.Defaults.Prompt = v
			return nil
		},
	},

	"defaults.no_default_cluster": {
		"defaults.no_default_cluster",
		"false",
		xkindDefault,
		func(v string, y *RpkYaml) error {
			b, err := strconv.ParseBool(v)
			y.Defaults.NoDefaultCluster = b
			return err
		},
	},

	"defaults.dial_timeout": {
		"defaults.dial_timeout",
		"3s",
		xkindDefault,
		func(v string, y *RpkYaml) error {
			return y.Defaults.DialTimeout.UnmarshalText([]byte(v))
		},
	},

	"defaults.request_timeout_overhead": {
		"defaults.request_timeout_overhead",
		"10s",
		xkindDefault,
		func(v string, y *RpkYaml) error {
			return y.Defaults.RequestTimeoutOverhead.UnmarshalText([]byte(v))
		},
	},

	"defaults.retry_timeout": {
		"defaults.retry_timeout",
		"30s",
		xkindDefault,
		func(v string, y *RpkYaml) error {
			return y.Defaults.RetryTimeout.UnmarshalText([]byte(v))
		},
	},

	"defaults.fetch_max_wait": {
		"defaults.fetch_max_wait",
		"5s",
		xkindDefault,
		func(v string, y *RpkYaml) error {
			return y.Defaults.FetchMaxWait.UnmarshalText([]byte(v))
		},
	},

	"defaults.redpanda_client_id": {
		"defaults.redpanda_client_id",
		"rpk",
		xkindDefault,
		func(v string, y *RpkYaml) error {
			y.Defaults.RedpandaClientID = v
			return nil
		},
	},
}

// XFlags returns the list of -X flags that are supported by rpk.
func XFlags() []string {
	keys := maps.Keys(xflags)
	sort.Strings(keys)
	return keys
}

// XProfileFlags returns all X flags that modify rpk profile settings, and
// their corresponding yaml paths.
func XProfileFlags() (xs, yamlPaths []string) {
	for k, v := range xflags {
		if v.kind == xkindProfile {
			xs = append(xs, k)
			yamlPaths = append(yamlPaths, v.path)
		}
	}
	return
}

// XCloudAuthFlags returns all X flags that modify rpk cloud auth settings, and
// their corresponding yaml paths.
func XCloudAuthFlags() (xs, yamlPaths []string) {
	for k, v := range xflags {
		if v.kind == xkindCloudAuth {
			xs = append(xs, k)
			yamlPaths = append(yamlPaths, v.path)
		}
	}
	return
}

// XRpkDefaultsFlags returns all X flags that modify rpk defaults, and their
// corresponding yaml paths. Note that for rpk defaults, the X flags always
// have the same name as the yaml path and always begin with "defaults.".
func XRpkDefaultsFlags() (xs, yamlPaths []string) {
	for k, v := range xflags {
		if v.kind == xkindDefault {
			xs = append(xs, k)
			yamlPaths = append(yamlPaths, v.path)
		}
	}
	return
}

// XFlagYamlPath returns the yaml path for the given x flag, if the
// flag exists.
func XFlagYamlPath(x string) (string, bool) {
	v, ok := xflags[x]
	if !ok {
		return "", false
	}
	return v.path, true
}

// Params contains rpk-wide configuration parameters.
type Params struct {
	// ConfigFlag is any flag-specified config path.
	ConfigFlag string

	// Profile is any flag-specified profile name.
	Profile string

	// DebugLogs opts into debug logging.
	//
	// This field only for setting, to actually get a logger after the
	// field is set, use Logger().
	DebugLogs bool

	// FlagOverrides are any flag-specified config overrides.
	FlagOverrides []string

	loggerOnce sync.Once
	logger     *zap.Logger

	// BACKCOMPAT FLAGS
	brokers           []string
	user              string
	password          string
	saslMechanism     string
	enableKafkaTLS    bool
	kafkaCAFile       string
	kafkaCertFile     string
	kafkaKeyFile      string
	adminURLs         []string
	enableAdminTLS    bool
	adminCAFile       string
	adminCertFile     string
	adminKeyFile      string
	cloudClientID     string
	cloudClientSecret string
}

// ParamsHelp returns the long help text for -X help.
func ParamsHelp() string {
	return `The -X flag can be used to override any rpk specific configuration option.
As an example, -X brokers.tls.enabled=true enables TLS for the Kafka API.

The following options are available, with an example value for each option:

brokers=127.0.0.1:9092,localhost:9094
  A comma separated list of host:ports that rpk talks to for the Kafka API.
  By default, this is 127.0.0.1:9092.

tls.enabled=true
  A boolean that enableenables rpk to speak TLS to your broker's Kafka API listeners.
  You can use this if you have well known certificates setup on your Kafka API.
  If you use mTLS, specifying mTLS certificate filepaths automatically opts
  into TLS enabled.

tls.ca=/path/to/ca.pem
  A filepath to a PEM encoded CA certificate file to talk to your broker's
  Kafka API listeners with mTLS. You may also need this if your listeners are
  using a certificate by a well known authority that is not yet bundled on your
  operating system.

tls.cert=/path/to/cert.pem
  A filepath to a PEM encoded client certificate file to talk to your broker's
  Kafka API listeners with mTLS.

tls.key=/path/to/key.pem
  A filepath to a PEM encoded client key file to talk to your broker's Kafka
  API listeners with mTLS.

sasl.mechanism=SCRAM-SHA-256
  The SASL mechanism to use for authentication. This can be either SCRAM-SHA-256
  or SCRAM-SHA-512. Note that with Redpanda, the Admin API can be configured to
  require basic authentication with your Kafka API SASL credentials. This
  defaults to SCRAM-SHA-256 if no mechanism is specified.

user=username
  The SASL username to use for authentication. This is also used for the admin
  API if you have configured it to require basic authentication.

pass=password
  The SASL password to use for authentication. This is also used for the admin
  API if you have configured it to require basic authentication.

admin.hosts=localhost:9644,rp.example.com:9644
  A comma separated list of host:ports that rpk talks to for the Admin API.
  By default, this is 127.0.0.1:9644.

admin.tls.enabled=false
  A boolean that enables rpk to speak TLS to your broker's Admin API listeners.
  You can use this if you have well known certificates setup on your admin API.
  If you use mTLS, specifying mTLS certificate filepaths automatically opts
  into TLS enabled.

admin.tls.ca=/path/to/ca.pem
  A filepath to a PEM encoded CA certificate file to talk to your broker's
  Admin API listeners with mTLS. You may also need this if your listeners are
  using a certificate by a well known authority that is not yet bundled on your
  operating system.

admin.tls.cert=/path/to/cert.pem
  A filepath to a PEM encoded client certificate file to talk to your broker's
  Admin API listeners with mTLS.

admin.tls.key=/path/to/key.pem
  A filepath to a PEM encoded client key file to talk to your broker's Admin
  API listeners with mTLS.

cloud.client_id=somestring
  An oauth client ID to use for authenticating with the Redpanda Cloud API.

cloud.client_secret=somelongerstring
  An oauth client secret to use for authenticating with the Redpanda Cloud API.

defaults.prompt="%n"
  A format string to use for the default prompt; see 'rpk profile prompt' for
  more information.

defaults.no_default_cluster=false
  A boolean that disables rpk from talking to localhost:9092 if no other
  cluster is specified.

defaults.dial_timeout=3s
  A duration that rpk will wait for a connection to be established before
  timing out.

defaults.request_timeout_overhead=10s
  A duration that limits how long rpk waits for responses, *on top* of any
  request-internal timeout. For example, ListOffsets has no Timeout field so
  if request_timeout_overhead is 10s, rpk will wait for 10s for a response.
  However, JoinGroup has a RebalanceTimeoutMillis field, so the 10s is applied
  on top of the rebalance timeout.

defaults.retry_timeout=30s
  This timeout specifies how long rpk will retry Kafka API requests. This
  timeout is evaluated before any backoff -- if a request fails, we first check
  if the retry timeout has elapsed and if so, we stop retrying. If not, we wait
  for the backoff and then retry.

defaults.fetch_max_wait=5s
  This timeout specifies the maximum time that brokers will wait before
  replying to a fetch request with whatever data is available.

defaults.redpanda_client_id=rpk
  This string value is the client ID that rpk uses when issuing Kafka protocol
  requests to Redpanda. This client ID shows up in Redpanda logs and metrics,
  changing it can be useful if you want to have your own rpk client stand out
  from others that may be hitting the cluster.
`
}

// ParamsList returns the short help text for -X list.
func ParamsList() string {
	return `brokers=comma,delimited,host:ports
tls.enabled=boolean
tls.ca=/path/to/ca.pem
tls.cert=/path/to/cert.pem
tls.key=/path/to/key.pem
sasl.mechanism=SCRAM-SHA-256 or SCRAM-SHA-512
user=username
pass=password
admin.hosts=comma,delimited,host:ports
admin.tls.enabled=boolean
admin.tls.ca=/path/to/ca.pem
admin.tls.cert=/path/to/cert.pem
admin.tls.key=/path/to/key.pem
cloud.client_id=somestring
cloud.client_secret=somelongerstring
defaults.prompt="%n"
defaults.no_default_cluster=boolean
defaults.dial_timeout=duration(3s,1m,2h)
defaults.request_timeout_overhead=duration(10s,1m,2h)
defaults.retry_timeout=duration(30s,1m,2h)
defaults.fetch_max_wait=duration(5s,1m,2h)
defaults.redpanda_client_id=rpk
`
}

//////////////////////
// BACKCOMPAT FLAGS //
//////////////////////

// InstallKafkaFlags adds the original rpk Kafka API set of flags to this
// command and all subcommands.
func (p *Params) InstallKafkaFlags(cmd *cobra.Command) {
	pf := cmd.PersistentFlags()

	pf.StringSliceVar(&p.brokers, "brokers", nil, "Comma separated list of broker host:ports")
	pf.MarkHidden("brokers")

	p.InstallSASLFlags(cmd)
	p.InstallTLSFlags(cmd)

	pf.MarkHidden(FlagEnableTLS)
	pf.MarkHidden(FlagTLSCA)
	pf.MarkHidden(FlagTLSCert)
	pf.MarkHidden(FlagTLSKey)
}

// InstallSASLFlags adds the original rpk Kafka SASL flags that are also used
// by the admin API for authentication.
func (p *Params) InstallSASLFlags(cmd *cobra.Command) {
	pf := cmd.PersistentFlags()

	pf.StringVar(&p.user, FlagSASLUser, "", "SASL user to be used for authentication")
	pf.StringVar(&p.password, "password", "", "SASL password to be used for authentication")
	pf.StringVar(&p.saslMechanism, "sasl-mechanism", "", "The authentication mechanism to use (SCRAM-SHA-256, SCRAM-SHA-512)")

	pf.MarkHidden(FlagSASLUser)
	pf.MarkHidden("password")
	pf.MarkHidden("sasl-mechanism")
}

// InstallTLSFlags adds the original rpk Kafka API TLS set of flags to this
// command and all subcommands. This is only used by the prometheus dashboard
// generation; all other Kafka API flag backcompat commands use
// InstallKafkaFlags. This command does not mark the added flags as deprecated.
func (p *Params) InstallTLSFlags(cmd *cobra.Command) {
	pf := cmd.PersistentFlags()

	pf.BoolVar(&p.enableKafkaTLS, FlagEnableTLS, false, "Enable TLS for the Kafka API (not necessary if specifying custom certs)")
	pf.StringVar(&p.kafkaCAFile, FlagTLSCA, "", "The CA certificate to be used for TLS communication with the broker")
	pf.StringVar(&p.kafkaCertFile, FlagTLSCert, "", "The certificate to be used for TLS authentication with the broker")
	pf.StringVar(&p.kafkaKeyFile, FlagTLSKey, "", "The certificate key to be used for TLS authentication with the broker")
}

// InstallAdminFlags adds the original rpk Admin API set of flags to this
// command and all subcommands.
func (p *Params) InstallAdminFlags(cmd *cobra.Command) {
	pf := cmd.PersistentFlags()

	pf.StringSliceVar(&p.adminURLs, "api-urls", nil, "Comma separated list of admin API host:ports")
	pf.StringSliceVar(&p.adminURLs, "hosts", nil, "")
	pf.StringSliceVar(&p.adminURLs, "admin-url", nil, "")

	pf.BoolVar(&p.enableAdminTLS, "admin-api-tls-enabled", false, "Enable TLS for the Admin API (not necessary if specifying custom certs)")
	pf.StringVar(&p.adminCAFile, "admin-api-tls-truststore", "", "The CA certificate  to be used for TLS communication with the admin API")
	pf.StringVar(&p.adminCertFile, "admin-api-tls-cert", "", "The certificate to be used for TLS authentication with the admin API")
	pf.StringVar(&p.adminKeyFile, "admin-api-tls-key", "", "The certificate key to be used for TLS authentication with the admin API")

	pf.MarkHidden("api-urls")
	pf.MarkHidden("hosts")
	pf.MarkHidden("admin-url")
	pf.MarkHidden("admin-api-tls-enabled")
	pf.MarkHidden("admin-api-tls-truststore")
	pf.MarkHidden("admin-api-tls-cert")
	pf.MarkHidden("admin-api-tls-key")
}

// InstallCloudFlags adds the --client-id and --client-secret flags that
// existed in the `rpk cloud` subcommands.
func (p *Params) InstallCloudFlags(cmd *cobra.Command) {
	cmd.Flags().StringVar(&p.cloudClientID, FlagClientID, "", "The client ID of the organization in Redpanda Cloud")
	cmd.Flags().StringVar(&p.cloudClientSecret, FlagClientSecret, "", "The client secret of the organization in Redpanda Cloud")
	cmd.MarkFlagsRequiredTogether(FlagClientID, FlagClientSecret)
}

func (p *Params) backcompatFlagsToOverrides() {
	if len(p.brokers) > 0 {
		p.FlagOverrides = append(p.FlagOverrides, fmt.Sprintf("%s=%s", xKafkaBrokers, strings.Join(p.brokers, ",")))
	}
	if p.user != "" {
		p.FlagOverrides = append(p.FlagOverrides, fmt.Sprintf("%s=%s", xKafkaSASLUser, p.user))
	}
	if p.password != "" {
		p.FlagOverrides = append(p.FlagOverrides, fmt.Sprintf("%s=%s", xKafkaSASLPass, p.password))
	}
	if p.saslMechanism != "" {
		p.FlagOverrides = append(p.FlagOverrides, fmt.Sprintf("%s=%s", xKafkaSASLMechanism, p.saslMechanism))
	}

	if p.enableKafkaTLS {
		p.FlagOverrides = append(p.FlagOverrides, fmt.Sprintf("%s=%t", xKafkaTLSEnabled, p.enableKafkaTLS))
	}
	if p.kafkaCAFile != "" {
		p.FlagOverrides = append(p.FlagOverrides, fmt.Sprintf("%s=%s", xKafkaCACert, p.kafkaCAFile))
	}
	if p.kafkaCertFile != "" {
		p.FlagOverrides = append(p.FlagOverrides, fmt.Sprintf("%s=%s", xKafkaClientCert, p.kafkaCertFile))
	}
	if p.kafkaKeyFile != "" {
		p.FlagOverrides = append(p.FlagOverrides, fmt.Sprintf("%s=%s", xKafkaClientKey, p.kafkaKeyFile))
	}

	if len(p.adminURLs) > 0 {
		p.FlagOverrides = append(p.FlagOverrides, fmt.Sprintf("%s=%s", xAdminHosts, strings.Join(p.adminURLs, ",")))
	}
	if p.enableAdminTLS {
		p.FlagOverrides = append(p.FlagOverrides, fmt.Sprintf("%s=%t", xAdminTLSEnabled, p.enableAdminTLS))
	}
	if p.adminCAFile != "" {
		p.FlagOverrides = append(p.FlagOverrides, fmt.Sprintf("%s=%s", xAdminCACert, p.adminCAFile))
	}
	if p.adminCertFile != "" {
		p.FlagOverrides = append(p.FlagOverrides, fmt.Sprintf("%s=%s", xAdminClientCert, p.adminCertFile))
	}
	if p.adminKeyFile != "" {
		p.FlagOverrides = append(p.FlagOverrides, fmt.Sprintf("%s=%s", xAdminClientKey, p.adminKeyFile))
	}

	if p.cloudClientID != "" {
		p.FlagOverrides = append(p.FlagOverrides, fmt.Sprintf("%s=%s", xCloudClientID, p.cloudClientID))
	}
	if p.cloudClientSecret != "" {
		p.FlagOverrides = append(p.FlagOverrides, fmt.Sprintf("%s=%s", xCloudClientSecret, p.cloudClientSecret))
	}
}

///////////////////////
// LOADING & WRITING //
///////////////////////

// Load returns the param's config file. In order, this
//
//   - Finds the config file, per the --config flag or the default search set.
//   - Decodes the config over the default configuration.
//   - Back-compats any old format into any new format.
//   - Processes env and flag overrides.
//   - Sets unset default values.
func (p *Params) Load(fs afero.Fs) (*Config, error) {
	defRpk, err := defaultVirtualRpkYaml()
	if err != nil {
		return nil, err
	}
	c := &Config{
		p:            p,
		redpandaYaml: *DevDefault(),
		rpkYaml:      defRpk,
	}

	// For Params, we clear the KafkaAPI and AdminAPI -- they are used to
	// fill in rpk sections by default. These sections are *also* filled in
	// in ensureBrokerAddrs, but only if we want a default cluster. At the
	// end, if these two values are still nil AND there is no actual
	// redpanda.yaml file with empty sections, we fill back in our old
	// defaults.
	{
		oldKafka := c.redpandaYaml.Redpanda.KafkaAPI
		oldAdmin := c.redpandaYaml.Redpanda.AdminAPI
		c.redpandaYaml.Redpanda.KafkaAPI = nil
		c.redpandaYaml.Redpanda.AdminAPI = nil
		defer func() {
			if c.redpandaYamlExists {
				return
			}
			if c.redpandaYaml.Redpanda.KafkaAPI == nil {
				c.redpandaYaml.Redpanda.KafkaAPI = oldKafka
			}
			if c.redpandaYaml.Redpanda.AdminAPI == nil {
				c.redpandaYaml.Redpanda.AdminAPI = oldAdmin
			}
		}()
	}

	if err := p.backcompatOldCloudYaml(fs); err != nil {
		return nil, err
	}
	if err := p.readRpkConfig(fs, c); err != nil {
		return nil, err
	}
	if err := p.readRedpandaConfig(fs, c); err != nil {
		return nil, err
	}

	c.mergeRpkIntoRedpanda(true)     // merge actual rpk.yaml KafkaAPI,AdminAPI,Tuners into redpanda.yaml rpk section
	c.addUnsetRedpandaDefaults(true) // merge from actual redpanda.yaml redpanda section to rpk section
	c.ensureRpkProfile()             // ensure Virtual rpk.yaml has a loaded profile
	c.ensureRpkCloudAuth()           // ensure Virtual rpk.yaml has a current auth
	c.mergeRedpandaIntoRpk()         // merge redpanda.yaml rpk section back into rpk.yaml KafkaAPI,AdminAPI,Tuners (picks up redpanda.yaml extras sections were empty)
	p.backcompatFlagsToOverrides()
	if err := p.processOverrides(c); err != nil { // override rpk.yaml profile from env&flags
		return nil, err
	}
	c.mergeRpkIntoRedpanda(false)     // merge Virtual rpk.yaml into redpanda.yaml rpk section (picks up env&flags)
	c.addUnsetRedpandaDefaults(false) // merge from Virtual redpanda.yaml redpanda section to rpk section (picks up original redpanda.yaml defaults)
	c.mergeRedpandaIntoRpk()          // merge from redpanda.yaml rpk section back to rpk.yaml, picks up final redpanda.yaml defaults
	c.fixSchemePorts()                // strip any scheme, default any missing ports
	c.addConfigToProfiles()
	c.parseDevOverrides()

	if !c.rpkYaml.Defaults.NoDefaultCluster {
		c.ensureBrokerAddrs()
	}

	return c, nil
}

// SugarLogger returns Logger().Sugar().
func (p *Params) SugarLogger() *zap.SugaredLogger {
	return p.Logger().Sugar()
}

// Logger parses returns the corresponding zap logger or a NopLogger.
func (p *Params) Logger() *zap.Logger {
	p.loggerOnce.Do(func() {
		if !p.DebugLogs {
			p.logger = zap.NewNop()
			return
		}

		// Now the zap config. We want to to the console and make the logs
		// somewhat nice. The log time is effectively time.TimeMillisOnly.
		// We disable logging the callsite and sampling, we shorten the log
		// level to three letters, and we only add color if this is a
		// terminal.
		zcfg := zap.NewProductionConfig()
		zcfg.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
		zcfg.DisableCaller = true
		zcfg.DisableStacktrace = true
		zcfg.Sampling = nil
		zcfg.Encoding = "console"
		zcfg.EncoderConfig.EncodeTime = zapcore.TimeEncoder(func(t time.Time, pae zapcore.PrimitiveArrayEncoder) {
			pae.AppendString(t.Format("15:04:05.000"))
		})
		zcfg.EncoderConfig.EncodeDuration = zapcore.StringDurationEncoder
		zcfg.EncoderConfig.ConsoleSeparator = "  "

		// https://en.wikipedia.org/wiki/ANSI_escape_code#Colors
		const (
			red     = 31
			yellow  = 33
			blue    = 34
			magenta = 35
		)

		// Zap's OutputPaths bydefault is []string{"stderr"}, so we
		// only need to check os.Stderr.
		tty := term.IsTerminal(int(os.Stderr.Fd()))
		color := func(n int, s string) string {
			if !tty {
				return s
			}
			return fmt.Sprintf("\x1b[%dm%s\x1b[0m", n, s)
		}
		colors := map[zapcore.Level]string{
			zapcore.ErrorLevel: color(red, "ERROR"),
			zapcore.WarnLevel:  color(yellow, "WARN"),
			zapcore.InfoLevel:  color(blue, "INFO"),
			zapcore.DebugLevel: color(magenta, "DEBUG"),
		}
		zcfg.EncoderConfig.EncodeLevel = func(l zapcore.Level, enc zapcore.PrimitiveArrayEncoder) {
			switch l {
			case zapcore.ErrorLevel,
				zapcore.WarnLevel,
				zapcore.InfoLevel,
				zapcore.DebugLevel:
			default:
				l = zapcore.ErrorLevel
			}
			enc.AppendString(colors[l])
		}

		p.logger, _ = zcfg.Build() // this configuration does not error
	})
	return p.logger
}

func readFile(fs afero.Fs, path string) (string, []byte, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return abs, nil, err
	}
	file, err := afero.ReadFile(fs, abs)
	if err != nil {
		return abs, nil, err
	}
	return abs, file, err
}

func (*Params) backcompatOldCloudYaml(fs afero.Fs) error {
	def, err := DefaultRpkYamlPath()
	if err != nil {
		//nolint:nilerr // This error only happens if the user unset $HOME, and
		// if they do that, we will avoid failing / avoid backcompat here.
		return nil
	}

	// Read and parse the old file. If it does not exist, that's great.
	oldPath := filepath.Join(filepath.Dir(def), "__cloud.yaml")
	_, raw, err := readFile(fs, oldPath)
	if err != nil {
		if errors.Is(err, afero.ErrFileNotFound) {
			return nil
		}
		return fmt.Errorf("unable to backcompat __cloud.yaml file: %v", err)
	}
	var old struct {
		ClientID     string `yaml:"client_id"`
		ClientSecret string `yaml:"client_secret"`
		AuthToken    string `yaml:"auth_token"`
	}
	if err := yaml.Unmarshal(raw, &old); err != nil {
		return fmt.Errorf("unable to yaml decode %s: %v", oldPath, err)
	}

	// For the rpk.yaml, if it does not exist, we will create it.
	// We only support the default path, not any --config override.
	// We do not want to migrate into some custom path.
	_, rawRpkYaml, err := readFile(fs, def)
	if err != nil && !errors.Is(err, afero.ErrFileNotFound) {
		return fmt.Errorf("unable to read %s: %v", def, err)
	}
	var rpkYaml RpkYaml
	if errors.Is(err, afero.ErrFileNotFound) {
		rpkYaml = emptyVirtualRpkYaml()
	} else {
		if err := yaml.Unmarshal(rawRpkYaml, &rpkYaml); err != nil {
			return fmt.Errorf("unable to yaml decode %s: %v", def, err)
		}
		if rpkYaml.Version < 1 {
			return fmt.Errorf("%s is not in the expected rpk.yaml format", def)
		} else if rpkYaml.Version > 1 {
			return fmt.Errorf("%s is using a newer rpk.yaml format than we understand, please upgrade rpk", def)
		}
	}
	rpkYaml.fileLocation = def

	var exists bool
	for _, a := range rpkYaml.CloudAuths {
		if a.ClientID == old.ClientID && a.ClientSecret == old.ClientSecret {
			exists = true
			break
		}
	}
	if !exists {
		a := RpkCloudAuth{
			Name:         "for_byoc",
			Description:  "Client ID and Secret for BYOC",
			ClientID:     old.ClientID,
			ClientSecret: old.ClientSecret,
			AuthToken:    old.AuthToken,
		}
		rpkYaml.PushAuth(a)
		if rpkYaml.CurrentCloudAuth == "" {
			rpkYaml.CurrentCloudAuth = a.Name
		}
		if err := rpkYaml.Write(fs); err != nil {
			return fmt.Errorf("unable to migrate %s to %s: %v", oldPath, def, err)
		}
	}
	// If we fail at removing the old file, that's ok. We will try again
	// the next time this command runs.
	fs.Remove(oldPath)

	return nil
}

func (p *Params) readRpkConfig(fs afero.Fs, c *Config) error {
	def, err := DefaultRpkYamlPath()
	path := def
	if p.ConfigFlag != "" {
		path = p.ConfigFlag
	} else if err != nil {
		//nolint:nilerr // If $HOME is unset, we do not read any file. If the user
		// eventually tries to write, we fail in Write. Allowing
		// $HOME to not exists allows rpk to work in CI settings
		// where all config flags are being specified.
		return nil
	}
	abs, file, err := readFile(fs, path)
	if err != nil {
		if !errors.Is(err, afero.ErrFileNotFound) {
			return err
		}
		// The file does not exist. We might create it. The user could
		// be trying to create either an rpk.yaml or a redpanda.yaml.
		// All rpk.yaml creation commands are under rpk {auth,profile},
		// whereas there as only three redpanda.yaml creation commands.
		// Since they do not overlap, it is ok to save this config flag
		// as the file location for both of these.
		c.rpkYaml.fileLocation = abs
		c.rpkYamlActual.fileLocation = abs
		return nil
	}
	before := c.rpkYaml
	if err := yaml.Unmarshal(file, &c.rpkYaml); err != nil {
		return fmt.Errorf("unable to yaml decode %s: %v", path, err)
	}
	if c.rpkYaml.Version < 1 {
		if p.ConfigFlag == "" {
			return fmt.Errorf("%s is not in the expected rpk.yaml format", def)
		}
		c.rpkYaml = before // this config is not an rpk.yaml; preserve our defaults
		return nil
	} else if c.rpkYaml.Version > 1 {
		return fmt.Errorf("%s is using a newer rpk.yaml format than we understand, please upgrade rpk", def)
	}
	yaml.Unmarshal(file, &c.rpkYamlActual)

	if p.Profile != "" {
		if c.rpkYaml.Profile(p.Profile) == nil {
			return fmt.Errorf("selected profile %q does not exist", p.Profile)
		}
		c.rpkYaml.CurrentProfile = p.Profile
		c.rpkYamlActual.CurrentProfile = p.Profile
	}
	c.rpkYamlExists = true
	c.rpkYaml.fileLocation = abs
	c.rpkYamlActual.fileLocation = abs
	c.rpkYaml.fileRaw = file
	c.rpkYamlActual.fileRaw = file
	return nil
}

func (p *Params) readRedpandaConfig(fs afero.Fs, c *Config) error {
	paths := []string{p.ConfigFlag}
	if p.ConfigFlag == "" {
		paths = paths[:0]
		if cd, _ := os.Getwd(); cd != "" {
			paths = append(paths, filepath.Join(cd, "redpanda.yaml"))
		}
		paths = append(paths, filepath.FromSlash(DefaultRedpandaYamlPath))
	}
	for _, path := range paths {
		abs, file, err := readFile(fs, path)
		if err != nil {
			if errors.Is(err, afero.ErrFileNotFound) {
				continue
			}
			return fmt.Errorf("unable to read file in %v: %v", path, err)
		}

		if err := yaml.Unmarshal(file, &c.redpandaYaml); err != nil {
			return fmt.Errorf("unable to yaml decode %s: %v", path, err)
		}
		yaml.Unmarshal(file, &c.redpandaYamlActual)

		c.redpandaYamlExists = true
		c.redpandaYaml.fileLocation = abs
		c.redpandaYamlActual.fileLocation = abs
		c.redpandaYaml.fileRaw = file
		c.redpandaYamlActual.fileRaw = file
		return nil
	}
	location := paths[len(paths)-1]
	c.redpandaYaml.fileLocation = location
	c.redpandaYamlActual.fileLocation = location
	return nil
}

// We merge rpk.yaml files into our Virtual redpanda.yaml rpk section,
// only if the rpk section contains relevant bits of information.
//
// We start with the actual file itself: if the file is populated, we use it.
// Later, after doing a bunch of default setting to the Virtual rpk.yaml,
// we call this again to migrate any final new additions.
func (c *Config) mergeRpkIntoRedpanda(actual bool) {
	src := &c.rpkYaml
	if actual {
		src = &c.rpkYamlActual
	}
	dst := &c.redpandaYaml.Rpk

	p := src.Profile(src.CurrentProfile)
	if p == nil {
		return
	}
	if !reflect.DeepEqual(p.KafkaAPI, RpkKafkaAPI{}) {
		dst.KafkaAPI = p.KafkaAPI
	}
	if !reflect.DeepEqual(p.AdminAPI, RpkAdminAPI{}) {
		dst.AdminAPI = p.AdminAPI
	}
}

// This function ensures a current profile exists in the Virtual rpk.yaml.
func (c *Config) ensureRpkProfile() {
	dst := &c.rpkYaml
	p := dst.Profile(dst.CurrentProfile)
	if p != nil {
		return
	}

	def := DefaultRpkProfile()
	dst.CurrentProfile = def.Name
	p = dst.Profile(dst.CurrentProfile)
	if p != nil {
		return
	}
	dst.PushProfile(def)
}

// This function ensures a current auth exists in the Virtual rpk.yaml.
func (c *Config) ensureRpkCloudAuth() {
	dst := &c.rpkYaml
	auth := dst.Auth(dst.CurrentCloudAuth)
	if auth != nil {
		return
	}

	def := DefaultRpkCloudAuth()
	dst.CurrentCloudAuth = def.Name
	auth = dst.Auth(dst.CurrentCloudAuth)
	if auth != nil {
		return
	}
	dst.PushAuth(def)
}

func (c *Config) ensureBrokerAddrs() {
	{
		dst := &c.redpandaYaml
		if len(dst.Rpk.KafkaAPI.Brokers) == 0 {
			dst.Rpk.KafkaAPI.Brokers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(DefaultKafkaPort))}
		}
		if len(dst.Rpk.AdminAPI.Addresses) == 0 {
			dst.Rpk.AdminAPI.Addresses = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(DefaultAdminPort))}
		}
	}
	{
		dst := c.rpkYaml.Profile(c.rpkYaml.CurrentProfile) // must exist by this function
		if len(dst.KafkaAPI.Brokers) == 0 {
			dst.KafkaAPI.Brokers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(DefaultKafkaPort))}
		}
		if len(dst.AdminAPI.Addresses) == 0 {
			dst.AdminAPI.Addresses = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(DefaultAdminPort))}
		}
	}
}

// We merge redpanda.yaml's rpk section back into rpk.yaml's profile.  This
// picks up any extras from addUnsetRedpandaDefaults that were not set in the
// rpk file. We call this after ensureRpkProfile, so we do not need to
// nil-check the profile.
func (c *Config) mergeRedpandaIntoRpk() {
	src := &c.redpandaYaml.Rpk
	dst := &c.rpkYaml

	p := dst.Profile(dst.CurrentProfile)
	if reflect.DeepEqual(p.KafkaAPI, RpkKafkaAPI{}) {
		p.KafkaAPI = src.KafkaAPI
	}
	if reflect.DeepEqual(p.AdminAPI, RpkAdminAPI{}) {
		p.AdminAPI = src.AdminAPI
	}
}

// Similar to backcompat flags, we first capture old env vars and then new
// ones. New env vars are the same as -X, uppercased, s/./_/.
func envOverrides() []string {
	var envOverrides []string
	for _, envMapping := range []struct {
		old       string
		targetKey string
	}{
		{"REDPANDA_BROKERS", xKafkaBrokers},
		{"REDPANDA_TLS_TRUSTSTORE", xKafkaCACert},
		{"REDPANDA_TLS_CA", xKafkaCACert},
		{"REDPANDA_TLS_CERT", xKafkaClientCert},
		{"REDPANDA_TLS_KEY", xKafkaClientKey},
		{"REDPANDA_SASL_MECHANISM", xKafkaSASLMechanism},
		{"REDPANDA_SASL_USERNAME", xKafkaSASLUser},
		{"REDPANDA_SASL_PASSWORD", xKafkaSASLPass},
		{"REDPANDA_API_ADMIN_ADDRS", xAdminHosts},
		{"REDPANDA_ADMIN_TLS_TRUSTSTORE", xAdminCACert},
		{"REDPANDA_ADMIN_TLS_CA", xAdminCACert},
		{"REDPANDA_ADMIN_TLS_CERT", xAdminClientCert},
		{"REDPANDA_ADMIN_TLS_KEY", xAdminClientKey},
		{"RPK_CLOUD_CLIENT_ID", xCloudClientID},
		{"RPK_CLOUD_CLIENT_SECRET", xCloudClientSecret},
	} {
		if v, exists := os.LookupEnv(envMapping.old); exists {
			envOverrides = append(envOverrides, envMapping.targetKey+"="+v)
		}
	}
	for _, k := range XFlags() {
		targetKey := k
		k = strings.ReplaceAll(k, ".", "_")
		k = strings.ToUpper(k)
		if v, exists := os.LookupEnv("RPK_" + k); exists {
			envOverrides = append(envOverrides, targetKey+"="+v)
		}
	}
	return envOverrides
}

// processes first env and then flag overrides into our virtual rpk yaml.
func (p *Params) processOverrides(c *Config) error {
	parse := func(isEnv bool, kvs []string) error {
		from := "flag"
		if isEnv {
			from = "env"
		}
		for _, opt := range kvs {
			kv := strings.SplitN(opt, "=", 2)
			if len(kv) != 2 {
				return fmt.Errorf("%s config: %q is not a key=value", from, opt)
			}
			k, v := kv[0], kv[1]

			xf, exists := xflags[strings.ToLower(k)]
			if !exists {
				return fmt.Errorf("%s config: unknown key %q", from, k)
			}
			if err := xf.parse(v, &c.rpkYaml); err != nil {
				return fmt.Errorf("%s config key %q: %s", from, k, err)
			}
		}
		return nil
	}
	if err := parse(true, envOverrides()); err != nil {
		return err
	}
	return parse(false, p.FlagOverrides)
}

// As a final step in initializing a config, we add a few defaults to some
// specific unset values.
func (c *Config) addUnsetRedpandaDefaults(actual bool) {
	src := c.redpandaYaml
	if actual {
		src = c.redpandaYamlActual
	}
	dst := &c.redpandaYaml
	defaultFromRedpanda(
		namedAuthnToNamed(src.Redpanda.KafkaAPI),
		src.Redpanda.KafkaAPITLS,
		&dst.Rpk.KafkaAPI.Brokers,
	)
	defaultFromRedpanda(
		src.Redpanda.AdminAPI,
		src.Redpanda.AdminAPITLS,
		&dst.Rpk.AdminAPI.Addresses,
	)

	if len(dst.Rpk.KafkaAPI.Brokers) == 0 && len(dst.Rpk.AdminAPI.Addresses) > 0 {
		_, host, _, err := rpknet.SplitSchemeHostPort(dst.Rpk.AdminAPI.Addresses[0])
		if err == nil {
			host = net.JoinHostPort(host, strconv.Itoa(DefaultKafkaPort))
			dst.Rpk.KafkaAPI.Brokers = []string{host}
			dst.Rpk.KafkaAPI.TLS = dst.Rpk.AdminAPI.TLS
		}
	}

	if len(dst.Rpk.AdminAPI.Addresses) == 0 && len(dst.Rpk.KafkaAPI.Brokers) > 0 {
		_, host, _, err := rpknet.SplitSchemeHostPort(dst.Rpk.KafkaAPI.Brokers[0])
		if err == nil {
			host = net.JoinHostPort(host, strconv.Itoa(DefaultAdminPort))
			dst.Rpk.AdminAPI.Addresses = []string{host}
			dst.Rpk.AdminAPI.TLS = dst.Rpk.KafkaAPI.TLS
		}
	}
}

func (c *Config) fixSchemePorts() error {
	for i, k := range c.redpandaYaml.Rpk.KafkaAPI.Brokers {
		_, host, port, err := rpknet.SplitSchemeHostPort(k)
		if err != nil {
			return fmt.Errorf("unable to fix broker address %v: %w", k, err)
		}
		if port == "" {
			port = strconv.Itoa(DefaultKafkaPort)
		}
		c.redpandaYaml.Rpk.KafkaAPI.Brokers[i] = net.JoinHostPort(host, port)
	}
	p := c.rpkYaml.Profile(c.rpkYaml.CurrentProfile)
	for i, k := range p.KafkaAPI.Brokers {
		_, host, port, err := rpknet.SplitSchemeHostPort(k)
		if err != nil {
			return fmt.Errorf("unable to fix broker address %v: %w", k, err)
		}
		if port == "" {
			port = strconv.Itoa(DefaultKafkaPort)
		}
		p.KafkaAPI.Brokers[i] = net.JoinHostPort(host, port)
	}
	// if it's OIDC we don't need to add the default ports to the admin API URL.
	if isOIDC := p.KafkaAPI.SASL != nil && p.KafkaAPI.SASL.Mechanism == "oidc_from_cloud_auth"; !isOIDC {
		for i, a := range c.redpandaYaml.Rpk.AdminAPI.Addresses {
			_, host, port, err := rpknet.SplitSchemeHostPort(a)
			if err != nil {
				return fmt.Errorf("unable to fix admin address %v: %w", a, err)
			}
			if port == "" {
				port = strconv.Itoa(DefaultAdminPort)
			}
			c.redpandaYaml.Rpk.AdminAPI.Addresses[i] = net.JoinHostPort(host, port)
		}
		for i, a := range p.AdminAPI.Addresses {
			_, host, port, err := rpknet.SplitSchemeHostPort(a)
			if err != nil {
				return fmt.Errorf("unable to fix admin address %v: %w", a, err)
			}
			if port == "" {
				port = strconv.Itoa(DefaultAdminPort)
			}
			p.AdminAPI.Addresses[i] = net.JoinHostPort(host, port)
		}
	}
	return nil
}

func (c *Config) addConfigToProfiles() {
	for i := range c.rpkYaml.Profiles {
		c.rpkYaml.Profiles[i].c = c
	}
	for i := range c.rpkYamlActual.Profiles {
		c.rpkYamlActual.Profiles[i].c = c
	}
}

func (c *Config) parseDevOverrides() {
	v := reflect.ValueOf(&c.devOverrides)
	v = reflect.Indirect(v)
	t := v.Type()
	for i := 0; i < v.NumField(); i++ {
		envKey, ok := t.Field(i).Tag.Lookup("env")
		if !ok {
			panic(fmt.Sprintf("missing env tag on DevOverride.%s", t.Field(i).Name))
		}
		v.Field(i).SetString(os.Getenv(envKey))
	}
}

// defaultFromRedpanda sets fields in our `rpk` config section if those fields
// are left unspecified. Primarily, this benefits the workflow where we ssh
// into hosts and then run rpk against a localhost broker. To that end, we have
// the following preference:
//
//	localhost -> loopback -> private -> public -> (same order, but TLS)
//
// We favor no TLS. The broker likely does not have client certs, so we cannot
// set client TLS settings. If we have any non-TLS host, we do not use TLS
// hosts.
func defaultFromRedpanda(src []NamedSocketAddress, srcTLS []ServerTLS, dst *[]string) {
	if len(*dst) != 0 {
		return
	}

	tlsNames := make(map[string]bool)
	mtlsNames := make(map[string]bool)
	for _, t := range srcTLS {
		if t.Enabled {
			// redpanda uses RequireClientAuth to opt into mtls: if
			// RequireClientAuth is true, redpanda requires a CA
			// cert. Conversely, if RequireClientAuth is false, the
			// broker's CA is meaningless. This is a little bit
			// backwards, a CA should always vet against client
			// certs, but we use the bool field to determine mTLS.
			if t.RequireClientAuth {
				mtlsNames[t.Name] = true
			} else {
				tlsNames[t.Name] = true
			}
		}
	}
	add := func(noTLS, yesTLS, yesMTLS *[]string, hostport string, a NamedSocketAddress) {
		if mtlsNames[a.Name] {
			*yesMTLS = append(*yesTLS, hostport)
		} else if tlsNames[a.Name] {
			*yesTLS = append(*yesTLS, hostport)
		} else {
			*noTLS = append(*noTLS, hostport)
		}
	}

	var localhost, loopback, private, public,
		tlsLocalhost, tlsLoopback, tlsPrivate, tlsPublic,
		mtlsLocalhost, mtlsLoopback, mtlsPrivate, mtlsPublic []string
	for _, a := range src {
		s := net.JoinHostPort(a.Address, strconv.Itoa(a.Port))
		ip := net.ParseIP(a.Address)
		switch {
		case a.Address == "localhost":
			add(&localhost, &tlsLocalhost, &mtlsLocalhost, s, a)
		case ip.IsLoopback():
			add(&loopback, &tlsLoopback, &mtlsLoopback, s, a)
		case ip.IsUnspecified():
			// An unspecified address ("0.0.0.0") tells the server
			// to listen on all available interfaces. We cannot
			// dial 0.0.0.0, but we can dial 127.0.0.1 which is an
			// available interface. Also see:
			//
			// 	https://stackoverflow.com/a/20778887
			//
			// So, we add a loopback hostport.
			s = net.JoinHostPort("127.0.0.1", strconv.Itoa(a.Port))
			add(&loopback, &tlsLoopback, &mtlsLoopback, s, a)
		case ip.IsPrivate():
			add(&private, &tlsPrivate, &mtlsPrivate, s, a)
		default:
			add(&public, &tlsPublic, &mtlsPublic, s, a)
		}
	}
	*dst = append(*dst, localhost...)
	*dst = append(*dst, loopback...)
	*dst = append(*dst, private...)
	*dst = append(*dst, public...)

	if len(*dst) > 0 {
		return
	}

	*dst = append(*dst, tlsLocalhost...)
	*dst = append(*dst, tlsLoopback...)
	*dst = append(*dst, tlsPrivate...)
	*dst = append(*dst, tlsPublic...)

	if len(*dst) > 0 {
		return
	}

	*dst = append(*dst, mtlsLocalhost...)
	*dst = append(*dst, mtlsLoopback...)
	*dst = append(*dst, mtlsPrivate...)
	*dst = append(*dst, mtlsPublic...)
}

///////////////////
// FIELD SETTING //
///////////////////

// Set sets a field in pointer-to-struct p to a value, following yaml tags.
//
//	Key:    string containing the yaml field tags, e.g: 'rpk.admin_api'.
//	Value:  string representation of the value
func Set[T any](p *T, key, value string) error {
	if key == "" {
		return fmt.Errorf("key field must not be empty")
	}
	tags := strings.Split(key, ".")
	for _, tag := range tags {
		if _, _, err := splitTagIndex(tag); err != nil {
			return err
		}
	}
	finalTag := tags[len(tags)-1]
	if len(tags) > 1 && (finalTag == "enabled" && tags[len(tags)-2] == "tls" || finalTag == "tls") {
		switch value {
		case "{}":
		case "null":
		case "true":
			value = "{}"
		case "false":
			value = "null"
		default:
			return fmt.Errorf("%s must be true or {}", key)
		}
		if finalTag == "enabled" {
			tags = tags[:len(tags)-1]
			finalTag = tags[len(tags)-1]
		}
	}

	field, other, err := getField(tags, "", reflect.ValueOf(p).Elem())
	if err != nil {
		return err
	}
	isOther := other != reflect.Value{}

	// For Other fields, we need to wrap the value in key:value format when
	// unmarshaling, and we forbid indexing.
	if isOther {
		if _, index, _ := splitTagIndex(finalTag); index >= 0 {
			return fmt.Errorf("cannot index into unknown field %q", finalTag)
		}
		field = other
	}

	if !field.CanAddr() {
		return errors.New("rpk bug, please describe how you encountered this at https://github.com/redpanda-data/redpanda/issues/new?assignees=&labels=kind%2Fbug&template=01_bug_report.md")
	}

	if isOther {
		value = fmt.Sprintf("%s: %s", finalTag, value)
	}

	// If we cannot unmarshal, but our error looks like we are trying to
	// unmarshal a single element into a slice, we index[0] into the slice
	// and try unmarshaling again.
	rawv := []byte(value)
	if err := yaml.Unmarshal(rawv, field.Addr().Interface()); err != nil {
		// First we try wrapped with [ and ].
		if wrapped, ok := tryValueAsUnwrappedArray(field, value, err); ok {
			if err := yaml.Unmarshal([]byte(wrapped), field.Addr().Interface()); err == nil {
				return nil
			}
		}
		// If that still fails, we try setting a slice value if the
		// target is a slice.
		if elem0, ok := tryValueAsSlice0(field, err); ok {
			return yaml.Unmarshal(rawv, elem0.Addr().Interface())
		}
		return err
	}
	return nil
}

// getField deeply search in v for the value that reflect field tags.
//
// The parentRawTag is the previous tag, and includes an index if there is one.
func getField(tags []string, parentRawTag string, v reflect.Value) (reflect.Value, reflect.Value, error) {
	// *At* the last element, we check if it is a slice. The final tag can
	// still index into the slice and if that happens, we want to return
	// the index:
	//
	//     rpk.kafka_api.brokers[0] => return first broker.
	//
	if v.Kind() == reflect.Slice {
		index := -1
		if parentRawTag != "" {
			_, index, _ = splitTagIndex(parentRawTag)
		}
		if index < 0 {
			// If there is no index and if there are no additional
			// tags, we return the field itself (the slice). If
			// there are more tags or there is an index, we index.
			if len(tags) == 0 {
				return v, reflect.Value{}, nil
			}
			index = 0
		}
		if index > v.Len() {
			return reflect.Value{}, reflect.Value{}, fmt.Errorf("field %q: unable to modify index %d of %d elements", parentRawTag, index, v.Len())
		} else if index == v.Len() {
			v.Set(reflect.Append(v, reflect.Indirect(reflect.New(v.Type().Elem()))))
		}
		v = v.Index(index)
	}

	if len(tags) == 0 {
		// Now, either this is not a slice and we return the field, or
		// we indexed into the slice and we return the indexed value.
		return v, reflect.Value{}, nil
	}

	tag, _, _ := splitTagIndex(tags[0]) // err already checked at the start in Set

	// If is a nil pointer we assign the zero value, and we reassign v to the
	// value that v points to
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			v.Set(reflect.New(v.Type().Elem()))
		}
		v = reflect.Indirect(v)
	}
	if v.Kind() == reflect.Struct {
		newP, other, err := getFieldByTag(tag, v)
		if err != nil {
			return reflect.Value{}, reflect.Value{}, err
		}
		// if is "Other" map field, we stop the recursion and return
		if (other != reflect.Value{}) {
			// user may try to set deep unmanaged field:
			// rpk.unmanaged.name = "name"
			if len(tags) > 1 {
				return reflect.Value{}, reflect.Value{}, fmt.Errorf("unable to set field %q using rpk", strings.Join(tags, "."))
			}
			return reflect.Value{}, other, nil
		}
		return getField(tags[1:], tags[0], newP)
	}
	return reflect.Value{}, reflect.Value{}, fmt.Errorf("unable to set field of type %v", v.Type())
}

// getFieldByTag finds a field with a given yaml tag and returns 3 parameters:
//
//  1. if tag is found within the struct, return the field.
//  2. if tag is not found _but_ the struct has "Other" field, return Other.
//  3. Error if it can't find the given tag and "Other" field is unavailable.
func getFieldByTag(tag string, v reflect.Value) (reflect.Value, reflect.Value, error) {
	var (
		t       = v.Type()
		other   bool
		inlines []int
	)

	// Loop struct to get the field that match tag.
	for i := 0; i < v.NumField(); i++ {
		// rpk allows blindly setting unknown configuration parameters in
		// Other map[string]interface{} fields
		if t.Field(i).Name == "Other" {
			other = true
			continue
		}
		yt := t.Field(i).Tag.Get("yaml")

		// yaml struct tags can contain flags such as omitempty,
		// when tag.Get("yaml") is called it will return
		//   "my_tag,omitempty"
		// so we only need first parameter of the string slice.
		pieces := strings.Split(yt, ",")
		ft := pieces[0]

		if ft == tag {
			return v.Field(i), reflect.Value{}, nil
		}
		for _, p := range pieces {
			if p == "inline" {
				inlines = append(inlines, i)
				break
			}
		}
	}

	for _, i := range inlines {
		if v, _, err := getFieldByTag(tag, v.Field(i)); err == nil {
			return v, reflect.Value{}, nil
		}
	}

	// If we can't find the tag but the struct has an 'Other' map field:
	if other {
		return reflect.Value{}, v.FieldByName("Other"), nil
	}

	return reflect.Value{}, reflect.Value{}, fmt.Errorf("unable to find field %q", tag)
}

// All valid tags in redpanda.yaml are alphabetic_with_underscores. The k8s
// tests use dashes in and numbers in places for AdditionalConfiguration, and
// theoretically, a key may be added to redpanda in the future with a dash or a
// number. We will accept alphanumeric with any case, as well as dashes or
// underscores. That is plenty generous.
//
// 0: entire match
// 1: tag name
// 2: index, if present
var tagIndexRe = regexp.MustCompile(`^([_a-zA-Z0-9-]+)(?:\[(\d+)\])?$`)

// We accept tags with indices such as foo[1]. This splits the index and
// returns it if present, or -1 if not present.
func splitTagIndex(tag string) (string, int, error) {
	m := tagIndexRe.FindStringSubmatch(tag)
	if len(m) == 0 {
		return "", 0, fmt.Errorf("invalid field %q", tag)
	}

	field := m[1]

	if m[2] != "" {
		index, err := strconv.Atoi(m[2])
		if err != nil {
			return "", 0, fmt.Errorf("invalid field %q index: %v", field, err)
		}
		return field, index, nil
	}

	return field, -1, nil
}

// If a person tries to set an array field with a comma-separated string that
// is not wrapped in [], then we try wrapping. This makes setting brokers
// easier. We keep our parsing a bit simple; if a person is trying to set
// {"foo":"bar"},{"biz":"baz"}, we will not try to wrap. This supports the most
// common / only expected use case.
func tryValueAsUnwrappedArray(v reflect.Value, setValue string, err error) (string, bool) {
	if v.Kind() != reflect.Slice || !strings.Contains(err.Error(), "cannot unmarshal !!") {
		return "", false // if our destination is not a slice, or the error is not a destination-type-mismatch error, we do not wrap
	}
	if setValue == "" {
		return "", false // we do not try wrapping empty strings in brackets
	}
	if setValue[0] == '[' || setValue[len(setValue)-1] == ']' {
		return "", false // if this is already array-ish, we do not wrap
	}
	if setValue[0] == '{' || setValue[len(setValue)-1] == '}' {
		return "", false // if this is a yaml object, we do not wrap
	}
	if comma := strings.IndexByte(setValue, ','); comma < 1 || comma > len(setValue)-1 {
		return "", false // if there is no comma in the middle, we do not assume this is a comma-separated list
	}
	return "[" + setValue + "]", true
}

// If a value is a slice and our error indicates we are decoding a single
// element into the slice, we create index 0 and return that to be unmarshaled
// into.
//
// For json this is nice, the error is explicit. For yaml, we have to string
// match and it is a bit rough.
func tryValueAsSlice0(v reflect.Value, err error) (reflect.Value, bool) {
	if v.Kind() != reflect.Slice || !strings.Contains(err.Error(), "cannot unmarshal !!") {
		return v, false
	}
	if v.Len() == 0 {
		v.Set(reflect.Append(v, reflect.Indirect(reflect.New(v.Type().Elem()))))
	}
	// We are setting an entire array with one item; we always clear what
	// existed previously.
	v.Set(v.Slice(0, 1))
	return v.Index(0), true
}
