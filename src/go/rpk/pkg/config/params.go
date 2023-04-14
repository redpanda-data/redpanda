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
	"strconv"
	"strings"
	"sync"
	"time"

	rpkos "github.com/redpanda-data/redpanda/src/go/rpk/pkg/os"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/term"

	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

const (
	// The following flags exist for backcompat purposes and should not be
	// used elsewhere within rpk.
	flagBrokers        = "brokers"
	flagSASLMechanism  = "sasl-mechanism"
	flagSASLPass       = "password"
	flagAdminHosts1    = "hosts"
	flagAdminHosts2    = "api-urls"
	flagEnableAdminTLS = "admin-api-tls-enabled"
	flagAdminTLSCA     = "admin-api-tls-truststore"
	flagAdminTLSCert   = "admin-api-tls-cert"
	flagAdminTLSKey    = "admin-api-tls-key"

	// The following flags are currently used in some areas of rpk
	// (and ideally will be deprecated / removed in the future).
	FlagEnableTLS = "tls-enabled"
	FlagTLSCA     = "tls-truststore"
	FlagTLSCert   = "tls-cert"
	FlagTLSKey    = "tls-key"
	FlagSASLUser  = "user"

	envBrokers       = "REDPANDA_BROKERS"
	envTLSCA         = "REDPANDA_TLS_TRUSTSTORE"
	envTLSCert       = "REDPANDA_TLS_CERT"
	envTLSKey        = "REDPANDA_TLS_KEY"
	envSASLMechanism = "REDPANDA_SASL_MECHANISM"
	envSASLUser      = "REDPANDA_SASL_USERNAME"
	envSASLPass      = "REDPANDA_SASL_PASSWORD"
	envAdminHosts    = "REDPANDA_API_ADMIN_ADDRS"
	envAdminTLSCA    = "REDPANDA_ADMIN_TLS_TRUSTSTORE"
	envAdminTLSCert  = "REDPANDA_ADMIN_TLS_CERT"
	envAdminTLSKey   = "REDPANDA_ADMIN_TLS_KEY"
)

// This block contains what will eventually be used as keys in the global
// config-setting -X flag, as well as upper-cased, dot-to-underscore replaced
// env variables.
const (
	xKafkaBrokers = "brokers"

	xKafkaTLSEnabled = "brokers.tls.enabled"
	xKafkaCACert     = "brokers.tls.ca_cert_path"
	xKafkaClientCert = "brokers.tls.client_cert_path"
	xKafkaClientKey  = "brokers.tls.client_key_path"

	xKafkaSASLMechanism = "brokers.sasl.mechanism"
	xKafkaSASLUser      = "brokers.sasl.user"
	xKafkaSASLPass      = "brokers.sasl.pass"

	xAdminHosts      = "admin.hosts"
	xAdminTLSEnabled = "admin.tls.enabled"
	xAdminCACert     = "admin.tls.ca_cert_path"
	xAdminClientCert = "admin.tls.client_cert_path"
	xAdminClientKey  = "admin.tls.client_key_path"
)

// DefaultPath is where redpanda's configuration is located by default.
const DefaultPath = "/etc/redpanda/redpanda.yaml"

// Params contains rpk-wide configuration parameters.
type Params struct {
	// ConfigPath is any flag-specified config path.
	//
	// This is unused until step (2) in the refactoring process.
	ConfigPath string

	// LogLevel can be either none (default), error, warn, info, or debug,
	// or any prefix of those strings, upper or lower case.
	//
	// This field is meant to be set, to actually get a logger after the
	// field is set, use Logger().
	LogLevel string

	// FlagOverrides are any flag-specified config overrides.
	//
	// This is unused until step (2) in the refactoring process.
	FlagOverrides []string

	loggerOnce sync.Once
	logger     *zap.Logger

	// BACKCOMPAT FLAGS
	//
	// Note that some of these will move to standard persistent flags,
	// but are backcompat flags for the -X transition.

	brokers       []string
	user          string
	password      string
	saslMechanism string

	enableKafkaTLS bool
	kafkaCAFile    string
	kafkaCertFile  string
	kafkaKeyFile   string

	adminURLs      []string
	enableAdminTLS bool
	adminCAFile    string
	adminCertFile  string
	adminKeyFile   string
}

// ParamsHelp returns the long help text for -X help.
func ParamsHelp() string {
	return `The -X flag can be used to override any rpk specific configuration option.
As an example, -X brokers.tls.enabled=true enables TLS for the Kafka API.

The following options are available, with an example value for each option:

brokers=127.0.0.1:9092,localhost:9094
  A comma separated list of host:ports that rpk talks to for the Kafka API.
  By default, this is 127.0.0.1:9092.

brokers.tls.enabled=true
  A boolean that enableenables rpk to speak TLS to your broker's Kafka API listeners.
  You can use this if you have well known certificates setup on your Kafka API.
  If you use mTLS, specifying mTLS certificate filepaths automatically opts
  into TLS enabled.

brokers.tls.ca_cert_path=/path/to/ca.pem
  A filepath to a PEM encoded CA certificate file to talk to your broker's
  Kafka API listeners with mTLS. You may also need this if your listeners are
  using a certificate by a well known authority that is not yet bundled on your
  operating system.

brokers.tls.client_cert_path=/path/to/cert.pem
  A filepath to a PEM encoded client certificate file to talk to your broker's
  Kafka API listeners with mTLS.

brokers.tls.client_key_path=/path/to/key.pem
  A filepath to a PEM encoded client key file to talk to your broker's Kafka
  API listeners with mTLS.

brokers.sasl.mechanism=SCRAM-SHA-256
  The SASL mechanism to use for authentication. This can be either SCRAM-SHA-256
  or SCRAM-SHA-512. Note that with Redpanda, the Admin API can be configured to
  require basic authentication with your Kafka API SASL credentials.

brokers.sasl.user=username
  The SASL username to use for authentication.

brokers.sasl.pass=password
  The SASL password to use for authentication.

admin.hosts=localhost:9644,rp.example.com:9644
  A comma separated list of host:ports that rpk talks to for the Admin API.
  By default, this is 127.0.0.1:9644.

admin.tls.enabled=false
  A boolean that enables rpk to speak TLS to your broker's Admin API listeners.
  You can use this if you have well known certificates setup on your admin API.
  If you use mTLS, specifying mTLS certificate filepaths automatically opts
  into TLS enabled.

admin.tls.ca_cert_path=/path/to/ca.pem
  A filepath to a PEM encoded CA certificate file to talk to your broker's
  Admin API listeners with mTLS. You may also need this if your listeners are
  using a certificate by a well known authority that is not yet bundled on your
  operating system.

admin.tls.client_cert_path=/path/to/cert.pem
  A filepath to a PEM encoded client certificate file to talk to your broker's
  Admin API listeners with mTLS.

admin.tls.client_key_path=/path/to/key.pem
  A filepath to a PEM encoded client key file to talk to your broker's Admin
  API listeners with mTLS.
`
}

// ParamsList returns the short help text for -X list.
func ParamsList() string {
	return `brokers=comma,delimited,host:ports
brokers.tls.enabled=boolean
brokers.tls.ca_cert_path=/path/to/ca.pem
brokers.tls.client_cert_path=/path/to/cert.pem
brokers.tls.client_key_path=/path/to/key.pem
brokers.sasl.mechanism=SCRAM-SHA-256 or SCRAM-SHA-512
brokers.sasl.user=username
brokers.sasl.pass=password
admin.hosts=comma,delimited,host:ports
admin.tls.enabled=boolean
admin.tls.ca_cert_path=/path/to/ca.pem
admin.tls.client_cert_path=/path/to/cert.pem
admin.tls.client_key_path=/path/to/key.pem
`
}

//////////////////////
// BACKCOMPAT FLAGS //
//////////////////////

// InstallKafkaFlags adds the original rpk Kafka API set of flags to this
// command and all subcommands.
func (p *Params) InstallKafkaFlags(cmd *cobra.Command) {
	pf := cmd.PersistentFlags()

	pf.StringSliceVar(&p.brokers, flagBrokers, nil, "Comma separated list of broker host:ports")
	pf.StringVar(&p.user, FlagSASLUser, "", "SASL user to be used for authentication")
	pf.StringVar(&p.password, flagSASLPass, "", "SASL password to be used for authentication")
	pf.StringVar(&p.saslMechanism, flagSASLMechanism, "", "The authentication mechanism to use (SCRAM-SHA-256, SCRAM-SHA-512)")

	p.InstallTLSFlags(cmd)
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

	pf.StringSliceVar(&p.adminURLs, flagAdminHosts2, nil, "Comma separated list of admin API host:ports")
	pf.StringSliceVar(&p.adminURLs, flagAdminHosts1, nil, "")
	pf.StringSliceVar(&p.adminURLs, "admin-url", nil, "")

	pf.BoolVar(&p.enableAdminTLS, flagEnableAdminTLS, false, "Enable TLS for the Admin API (not necessary if specifying custom certs)")
	pf.StringVar(&p.adminCAFile, flagAdminTLSCA, "", "The CA certificate  to be used for TLS communication with the admin API")
	pf.StringVar(&p.adminCertFile, flagAdminTLSCert, "", "The certificate to be used for TLS authentication with the admin API")
	pf.StringVar(&p.adminKeyFile, flagAdminTLSKey, "", "The certificate key to be used for TLS authentication with the admin API")
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
	p.backcompatFlagsToOverrides()

	// If we have a config path loaded (through --config flag) the user
	// expect to load or create the file from this directory.
	if p.ConfigPath != "" {
		if exist, _ := afero.Exists(fs, p.ConfigPath); !exist {
			err := fs.MkdirAll(filepath.Dir(p.ConfigPath), 0o755)
			if err != nil {
				return nil, err
			}
		}
	}
	c := DevDefault()

	if err := p.readConfig(fs, c); err != nil {
		// Sometimes a config file will not exist (e.g. rpk running on MacOS),
		// which is OK. In those cases, just return the default config.
		if !errors.Is(err, afero.ErrFileNotFound) {
			return nil, err
		}
		// If there is no file, we set the file location to the passed
		// --config value, otherwise we use the default.
		if p.ConfigPath != "" {
			c.fileLocation = p.ConfigPath
		} else {
			c.fileLocation = DefaultPath
		}
	}
	c.backcompat()
	if err := p.processOverrides(c); err != nil {
		return nil, err
	}
	c.addUnsetDefaults()

	return c, nil
}

// SugarLogger returns Logger().Sugar().
func (p *Params) SugarLogger() *zap.SugaredLogger {
	return p.Logger().Sugar()
}

// Logger parses p.LogLevel and returns the corresponding zap logger or
// a NopLogger if the log level is invalid.
func (p *Params) Logger() *zap.Logger {
	p.loggerOnce.Do(func() {
		// First we normalize the level. We support prefixes such
		// that "w" means warn.
		p.LogLevel = strings.TrimSpace(strings.ToLower(p.LogLevel))
		if p.LogLevel == "" {
			p.LogLevel = "none"
		}
		var ok bool
		for _, level := range []string{"none", "error", "warn", "info", "debug"} {
			if strings.HasPrefix(level, p.LogLevel) {
				p.LogLevel, ok = level, true
				break
			}
		}
		if !ok {
			p.logger = zap.NewNop()
			return
		}
		var level zapcore.Level
		switch p.LogLevel {
		case "none":
			p.logger = zap.NewNop()
			return
		case "error":
			level = zap.ErrorLevel
		case "warn":
			level = zap.WarnLevel
		case "info":
			level = zap.InfoLevel
		case "debug":
			level = zap.DebugLevel
		}

		// Now the zap config. We want to to the console and make the logs
		// somewhat nice. The log time is effectively time.TimeMillisOnly.
		// We disable logging the callsite and sampling, we shorten the log
		// level to three letters, and we only add color if this is a
		// terminal.
		zcfg := zap.NewProductionConfig()
		zcfg.Level = zap.NewAtomicLevelAt(level)
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
			zapcore.ErrorLevel: color(red, "ERR"),
			zapcore.WarnLevel:  color(yellow, "WRN"),
			zapcore.InfoLevel:  color(blue, "INF"),
			zapcore.DebugLevel: color(magenta, "DBG"),
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

// isSameLoaded checks if the config object content is the same as the one
// loaded. The function returns a boolean indicating if the two config objects
// are equal.
func (c *Config) isSameLoaded() bool {
	var init, final *Config
	if err := yaml.Unmarshal(c.RawFile(), &init); err != nil {
		return false
	}

	// We marshal to later unmarshal the passed cfg to avoid comparing a
	// configuration with loaded unexported values such as
	// (file, fileLocation or rawFile)
	finalRaw, err := yaml.Marshal(c)
	if err != nil {
		return false
	}

	if err := yaml.Unmarshal(finalRaw, &final); err != nil {
		return false
	}

	// If we have a file with an older version of the SeedServer, we should
	// write the file to disk even if the contents are the same. This is
	// necessary because Redpanda no longer parses older SeedServer versions.
	//
	// For more information, see github.com/redpanda-data/redpanda/issues/8915.
	if init != nil {
		for _, s := range init.Redpanda.SeedServers {
			if s.untabbed {
				return false
			}
		}
	}

	return reflect.DeepEqual(init, final)
}

// Write writes loaded configuration parameters to redpanda.yaml.
func (c *Config) Write(fs afero.Fs) (rerr error) {
	// We return early if the config is the same as the one loaded in the first
	// place and avoid writing the file.
	if c.isSameLoaded() {
		return nil
	}
	location := c.fileLocation
	if location == "" {
		location = DefaultPath
	}
	b, err := yaml.Marshal(c)
	if err != nil {
		return fmt.Errorf("marshal error in loaded config, err: %s", err)
	}

	return rpkos.ReplaceFile(fs, location, b, 0o644)
}

func (p *Params) LocateConfig(fs afero.Fs) (string, error) {
	paths := []string{p.ConfigPath}
	if p.ConfigPath == "" {
		paths = nil
		if configDir, _ := os.UserConfigDir(); configDir != "" {
			paths = append(paths, filepath.Join(configDir, "rpk", "rpk.yaml"))
		}
		paths = append(paths, filepath.FromSlash(DefaultPath))
		if cd, _ := os.Getwd(); cd != "" {
			paths = append(paths, filepath.Join(cd, "redpanda.yaml"))
		}
		if home, _ := os.UserHomeDir(); home != "" {
			paths = append(paths, filepath.Join(home, "redpanda.yaml"))
		}
	}

	for _, path := range paths {
		// Ignore error: we only care whether it exists, other
		// stat() errors are not interesting.
		exists, _ := afero.Exists(fs, path)
		if exists {
			return path, nil
		}
	}

	return "", fmt.Errorf("%w: unable to find config in searched paths %v", afero.ErrFileNotFound, paths)
}

func (p *Params) readConfig(fs afero.Fs, c *Config) error {
	path, err := p.LocateConfig(fs)
	if err != nil {
		return err
	}

	file, err := afero.ReadFile(fs, path)
	if err != nil {
		return err
	}

	if err := yaml.Unmarshal(file, c); err != nil {
		return fmt.Errorf("unable to yaml decode %s: %v", path, err)
	}
	yaml.Unmarshal(file, &c.file) // cannot error since previous did not
	abs, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	c.fileLocation = abs
	c.file.fileLocation = abs
	c.rawFile = file
	c.file.rawFile = file
	return nil
}

// Before we process overrides, we process any backwards compatibility from the
// loaded file.
func (c *Config) backcompat() {
	r := &c.Rpk
	if r.KafkaAPI.TLS == nil {
		r.KafkaAPI.TLS = r.TLS
	}
	if r.KafkaAPI.SASL == nil {
		r.KafkaAPI.SASL = r.SASL
	}
	if r.AdminAPI.TLS == nil {
		r.AdminAPI.TLS = r.TLS
	}
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

// Process overrides processes env and flag overrides into a config file (so
// that we result in our priority order: flag, env, file).
func (p *Params) processOverrides(c *Config) error {
	r := &c.Rpk
	k := &r.KafkaAPI
	a := &r.AdminAPI

	// We have three "make" functions that initialize pointer values if
	// necessary.
	var (
		mkKafkaTLS = func() {
			if k.TLS == nil {
				k.TLS = new(TLS)
			}
		}
		mkSASL = func() {
			if k.SASL == nil {
				k.SASL = new(SASL)
			}
		}
		mkAdminTLS = func() {
			if a.TLS == nil {
				a.TLS = new(TLS)
			}
		}
	)

	// To override, we lookup any override key (e.g., brokers.tls.enabled
	// or admin_api.hosts) into this map. If the key exists, we processes
	// the value as appropriate (per the value function in the map).
	fns := map[string]func(string) error{
		xKafkaBrokers: func(v string) error { return splitCommaIntoStrings(v, &k.Brokers) },

		xKafkaTLSEnabled: func(string) error { mkKafkaTLS(); return nil },
		xKafkaCACert:     func(v string) error { mkKafkaTLS(); k.TLS.TruststoreFile = v; return nil },
		xKafkaClientCert: func(v string) error { mkKafkaTLS(); k.TLS.CertFile = v; return nil },
		xKafkaClientKey:  func(v string) error { mkKafkaTLS(); k.TLS.KeyFile = v; return nil },

		xKafkaSASLMechanism: func(v string) error { mkSASL(); k.SASL.Mechanism = v; return nil },
		xKafkaSASLUser:      func(v string) error { mkSASL(); k.SASL.User = v; return nil },
		xKafkaSASLPass:      func(v string) error { mkSASL(); k.SASL.Password = v; return nil },

		xAdminHosts:      func(v string) error { return splitCommaIntoStrings(v, &a.Addresses) },
		xAdminTLSEnabled: func(string) error { mkAdminTLS(); return nil },
		xAdminCACert:     func(v string) error { mkAdminTLS(); a.TLS.TruststoreFile = v; return nil },
		xAdminClientCert: func(v string) error { mkAdminTLS(); a.TLS.CertFile = v; return nil },
		xAdminClientKey:  func(v string) error { mkAdminTLS(); a.TLS.KeyFile = v; return nil },
	}

	// The parse function accepts the given overrides (key=value pairs) and
	// processes each. This is run first for env vars then for flags.
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

			fn, exists := fns[strings.ToLower(k)]
			if !exists {
				return fmt.Errorf("%s config: unknown key %q", from, k)
			}
			if err := fn(v); err != nil {
				return fmt.Errorf("%s config key %q: %s", from, k, err)
			}
		}
		return nil
	}

	var envOverrides []string

	// Similar to our flag mapping in ParamsFromCommand, we want to
	// continue supporting older environment variables. This section maps
	// old env vars to what key we should use in this new format.
	for _, envMapping := range []struct {
		old       string
		targetKey string
	}{
		{envBrokers, xKafkaBrokers},
		{envTLSCA, xKafkaCACert},
		{envTLSCert, xKafkaClientCert},
		{envTLSKey, xKafkaClientKey},
		{envSASLMechanism, xKafkaSASLMechanism},
		{envSASLUser, xKafkaSASLUser},
		{envSASLPass, xKafkaSASLPass},
		{envAdminHosts, xAdminHosts},
		{envAdminTLSCA, xAdminCACert},
		{envAdminTLSCert, xAdminClientCert},
		{envAdminTLSKey, xAdminClientKey},
	} {
		if v, exists := os.LookupEnv(envMapping.old); exists {
			envOverrides = append(envOverrides, envMapping.targetKey+"="+v)
		}
	}

	// Now we lookup any new format environment variables. These are named
	// exactly the same as our -X flag keys, but with dots replaced with
	// underscores, and the words uppercased. The new format takes
	// precedence over the old, and we ensure that by adding these
	// overrides last in the list of env overrides.
	for k := range fns {
		targetKey := k
		k = strings.ReplaceAll(k, ".", "_")
		k = strings.ToUpper(k)
		if v, exists := os.LookupEnv("RPK_" + k); exists {
			envOverrides = append(envOverrides, targetKey+"="+v)
		}
	}

	// Finally, we process overrides: first environment variables, and then
	// flags.
	if err := parse(true, envOverrides); err != nil {
		return err
	}
	return parse(false, p.FlagOverrides)
}

// As a final step in initializing a config, we add a few defaults to some
// specific unset values.
func (c *Config) addUnsetDefaults() {
	defaultFromRedpanda(
		namedAuthnToNamed(c.Redpanda.KafkaAPI),
		c.Redpanda.KafkaAPITLS,
		&c.Rpk.KafkaAPI.Brokers,
	)
	defaultFromRedpanda(
		c.Redpanda.AdminAPI,
		c.Redpanda.AdminAPITLS,
		&c.Rpk.AdminAPI.Addresses,
	)

	if len(c.Rpk.KafkaAPI.Brokers) == 0 && len(c.Rpk.AdminAPI.Addresses) > 0 {
		host, _, err := net.SplitHostPort(c.Rpk.AdminAPI.Addresses[0])
		if err == nil {
			host = net.JoinHostPort(host, strconv.Itoa(DefaultKafkaPort))
			c.Rpk.KafkaAPI.Brokers = []string{host}
			c.Rpk.KafkaAPI.TLS = c.Rpk.AdminAPI.TLS
		}
	}

	if len(c.Rpk.AdminAPI.Addresses) == 0 && len(c.Rpk.KafkaAPI.Brokers) > 0 {
		host, _, err := net.SplitHostPort(c.Rpk.KafkaAPI.Brokers[0])
		if err == nil {
			host = net.JoinHostPort(host, strconv.Itoa(DefaultAdminPort))
			c.Rpk.AdminAPI.Addresses = []string{host}
			c.Rpk.AdminAPI.TLS = c.Rpk.KafkaAPI.TLS
		}
	}

	if len(c.Rpk.KafkaAPI.Brokers) == 0 {
		c.Rpk.KafkaAPI.Brokers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(DefaultKafkaPort))}
	}

	if len(c.Rpk.AdminAPI.Addresses) == 0 {
		c.Rpk.AdminAPI.Addresses = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(DefaultAdminPort))}
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

// Set allow to set a single configuration field by passing a key value pair
//
//	Key:    string containing the yaml field tags, e.g: 'rpk.admin_api'.
//	Value:  string representation of the value, either single value or partial
//	        representation.
//	Format: either json or yaml (default: yaml).
func (c *Config) Set(key, value, format string) error {
	if key == "" {
		return fmt.Errorf("key field must not be empty")
	}
	tags := strings.Split(key, ".")
	for _, tag := range tags {
		if _, _, err := splitTagIndex(tag); err != nil {
			return err
		}
	}

	field, other, err := getField(tags, "", reflect.ValueOf(c).Elem())
	if err != nil {
		return err
	}
	isOther := other != reflect.Value{}

	// For Other fields, we need to wrap the value in key:value format when
	// unmarshaling, and we forbid indexing.
	var finalTag string
	if isOther {
		finalTag = tags[len(tags)-1]
		if _, index, _ := splitTagIndex(finalTag); index >= 0 {
			return fmt.Errorf("cannot index into unknown field %q", finalTag)
		}
		field = other
	}

	if !field.CanAddr() {
		return errors.New("rpk bug, please describe how you encountered this at https://github.com/redpanda-data/redpanda/issues/new?assignees=&labels=kind%2Fbug&template=01_bug_report.md")
	}

	var unmarshal func([]byte, interface{}) error
	switch strings.ToLower(format) {
	case "yaml", "single", "", "json": // single is deprecated and kept for backcompat; json is a subset of yaml
		if isOther {
			value = fmt.Sprintf("%s: %s", finalTag, value)
		}
		unmarshal = yaml.Unmarshal
	default:
		return fmt.Errorf("unsupported format %s", format)
	}

	// If we cannot unmarshal, but our error looks like we are trying to
	// unmarshal a single element into a slice, we index[0] into the slice
	// and try unmarshaling again.
	if err := unmarshal([]byte(value), field.Addr().Interface()); err != nil {
		if elem0, ok := tryValueAsSlice0(field, format, err); ok {
			return unmarshal([]byte(value), elem0.Addr().Interface())
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

// If a value is a slice and our error indicates we are decoding a single
// element into the slice, we create index 0 and return that to be unmarshaled
// into.
//
// For json this is nice, the error is explicit. For yaml, we have to string
// match and it is a bit rough.
func tryValueAsSlice0(v reflect.Value, format string, err error) (reflect.Value, bool) {
	if v.Kind() != reflect.Slice {
		return v, false
	}

	switch format {
	case "json", "yaml":
		if !strings.Contains(err.Error(), "cannot unmarshal !!") {
			return v, false
		}
	}
	if v.Len() == 0 {
		v.Set(reflect.Append(v, reflect.Indirect(reflect.New(v.Type().Elem()))))
	}
	// We are setting an entire array with one item; we always clear what
	// existed previously.
	v.Set(v.Slice(0, 1))
	return v.Index(0), true
}
