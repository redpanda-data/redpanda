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
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"gopkg.in/yaml.v3"
)

// This file contains the Params type, which will eventually be created in
// rpk's root command and passed to every command. This new params type is what
// will be used to load and parse configuration.
//
// The goal of the proposed refactoring is so that commands only have to take a
// Params variable, which will load a finalized configuration that needs no
// further setting. This will replace the current usages of deducing sections
// when some fields are missing, defaulting in separate areas, passing closures
// through many commands so that we have partial evaluation in one area, full
// evaluation in another, etc.
//
// The following are the steps to refactoring:
//
//  1) For every command, convert the command to using ParamsFromCommand.
//
// Once this step is complete, we can remove individual levels of flags and
// instead use flags from root itself.
//
//  2) In the rpk section of our config,
//      * drop _api from kafka_api and admin_api
//      * rename key_file to client_key_path
//      * rename cert_file to client_cert_path
//      * rename truststore_file to ca_cert_path
//      * rename password to pass
//      * rename admin's addresses to hosts
//
// We can do these renames in a backwards compatible way: rather than simple
// renames, we will add new fields and, when we load the config, if the new
// fields are empty, use the old. The purpose of these renames is to make the
// config map directly to our new configuration keys, to make the tls fields
// more explicit as to that they are, and to simplify some terminology.
//
//  3) Introduce `func (p *Params) InstallDeprecatedFlags(cmd *cobra.Command)`.
//     As well, introduce a global -X configuration flag.
//
// This function will entirely replace ParamsFromCommand, and will instead run
// on the root command only. This function will install all old flags, hide
// them, and mark them deprecated.
//
// The new -X flag will simply add to Params.FlagOverrides.
//
// Once step (3) is complete, we will officially deprecate anything old and
// remove the old at minimum 6 months later.

const (
	// FlagConfig is rpk config flag.
	FlagConfig = "config"

	// FlagVerbose opts in to verbose logging. This is to be replaced with
	// a log-level flag later, with `-v` meaning DEBUG for backcompat.
	FlagVerbose = "verbose"

	// This entire block is filled with our current flags and environment
	// variables. These will all eventually be hidden.

	FlagBrokers        = "brokers"
	FlagEnableTLS      = "tls-enabled"
	FlagTLSCA          = "tls-truststore"
	FlagTLSCert        = "tls-cert"
	FlagTLSKey         = "tls-key"
	FlagSASLMechanism  = "sasl-mechanism"
	FlagSASLUser       = "user"
	FlagSASLPass       = "password"
	FlagAdminHosts1    = "hosts"
	FlagAdminHosts2    = "api-urls"
	FlagEnableAdminTLS = "admin-api-tls-enabled"
	FlagAdminTLSCA     = "admin-api-tls-truststore"
	FlagAdminTLSCert   = "admin-api-tls-cert"
	FlagAdminTLSKey    = "admin-api-tls-key"

	EnvBrokers       = "REDPANDA_BROKERS"
	EnvTLSCA         = "REDPANDA_TLS_TRUSTSTORE"
	EnvTLSCert       = "REDPANDA_TLS_CERT"
	EnvTLSKey        = "REDPANDA_TLS_KEY"
	EnvSASLMechanism = "REDPANDA_SASL_MECHANISM"
	EnvSASLUser      = "REDPANDA_SASL_USERNAME"
	EnvSASLPass      = "REDPANDA_SASL_PASSWORD"
	EnvAdminHosts    = "REDPANDA_API_ADMIN_ADDRS"
	EnvAdminTLSCA    = "REDPANDA_ADMIN_TLS_TRUSTSTORE"
	EnvAdminTLSCert  = "REDPANDA_ADMIN_TLS_CERT"
	EnvAdminTLSKey   = "REDPANDA_ADMIN_TLS_KEY"
)

// This block contains what will eventually be used as keys in the global
// config-setting -X flag, as well as upper-cased, dot-to-underscore replaced
// env variables.
const (
	xKafkaBrokers = "kafka.brokers"

	xKafkaTLSEnabled = "kafka.tls.enabled"
	xKafkaCACert     = "kafka.tls.ca_cert_path"
	xKafkaClientCert = "kafka.tls.client_cert_path"
	xKafkaClientKey  = "kafka.tls.client_key_path"

	xKafkaSASLMechanism = "kafka.sasl.mechanism"
	xKafkaSASLUser      = "kafka.sasl.user"
	xKafkaSASLPass      = "kafka.sasl.pass"

	xAdminHosts      = "admin.hosts"
	xAdminTLSEnabled = "admin.tls.enabled"
	xAdminCACert     = "admin.tls.ca_cert_path"
	xAdminClientCert = "admin.tls.client_cert_path"
	xAdminClientKey  = "admin.tls.client_key_path"
)

// Params contains rpk-wide configuration parameters.
type Params struct {
	// ConfigPath is any flag-specified config path.
	//
	// This is unused until step (2) in the refactoring process.
	ConfigPath string

	// Verbose tracks the -v flag. This will be swapped with --log-level in
	// the future.
	Verbose bool

	// FlagOverrides are any flag-specified config overrides.
	//
	// This is unused until step (2) in the refactoring process.
	FlagOverrides []string
}

// ParamsFromCommand is an intermediate function to be used while refactoring
// rpk to have a top-down passed Params function. See the docs at the top of
// this file for the refactoring process.
func ParamsFromCommand(cmd *cobra.Command) *Params {
	var p Params

	for _, set := range []*pflag.FlagSet{
		cmd.PersistentFlags(),
		cmd.Flags(),
	} {
		set.Visit(func(f *pflag.Flag) {
			var key string
			var stripBrackets bool

			switch f.Name {
			default:
				return

			case FlagConfig:
				p.ConfigPath = f.Value.String()
				return

			case FlagVerbose:
				if b, err := strconv.ParseBool(f.Value.String()); err == nil {
					p.Verbose = b
				}
				return

			case FlagBrokers:
				key = xKafkaBrokers
				stripBrackets = true

			case FlagEnableTLS:
				key = xKafkaTLSEnabled
			case FlagTLSCA:
				key = xKafkaCACert
			case FlagTLSCert:
				key = xKafkaClientCert
			case FlagTLSKey:
				key = xKafkaClientKey

			case FlagSASLMechanism:
				key = xKafkaSASLMechanism
			case FlagSASLUser:
				key = xKafkaSASLUser
			case FlagSASLPass:
				key = xKafkaSASLPass

			case FlagAdminHosts1, FlagAdminHosts2:
				key = xAdminHosts
				stripBrackets = true
			case FlagEnableAdminTLS:
				key = xAdminTLSEnabled
			case FlagAdminTLSCA:
				key = xAdminCACert
			case FlagAdminTLSCert:
				key = xAdminClientCert
			case FlagAdminTLSKey:
				key = xAdminClientKey
			}

			val := f.Value.String()
			// Value.String() adds brackets to slice types, and we
			// need to strip that here.
			if stripBrackets {
				if len(val) > 0 && val[0] == '[' && val[len(val)-1] == ']' {
					val = val[1 : len(val)-1]
				}
			}

			p.FlagOverrides = append(p.FlagOverrides, key+"="+val)
		})
	}

	return &p
}

// Load returns the param's config file. In order, this
//
//  * Finds the config file, per the --config flag or the default search set.
//  * Decodes the config over the default configuration.
//  * Back-compats any old format into any new format.
//  * Processes env and flag overrides.
//  * Sets unset default values.
//
func (p *Params) Load(fs afero.Fs) (*Config, error) {
	cf := "/etc/redpanda/redpanda.yaml"
	// If we have a config path loaded (through --config flag) the user
	// expect to load or create the file from this directory.
	if p.ConfigPath != "" {
		fmt.Println("ConfigPath is not null: ", p.ConfigPath)
		if exist, _ := afero.Exists(fs, p.ConfigPath); !exist {
			err := fs.MkdirAll(filepath.Dir(p.ConfigPath), 0o755)
			if err != nil {
				return nil, err
			}
		}
		cf = p.ConfigPath
	}
	c := &Config{
		ConfigFile: cf,
		Redpanda: RedpandaConfig{
			Directory: "/var/lib/redpanda/data",
			RPCServer: SocketAddress{
				Address: "0.0.0.0",
				Port:    33145,
			},
			KafkaAPI: []NamedSocketAddress{{
				Address: "0.0.0.0",
				Port:    9092,
			}},
			AdminAPI: []NamedSocketAddress{{
				Address: "0.0.0.0",
				Port:    9644,
			}},
			DeveloperMode: true,
		},
		Rpk: RpkConfig{
			CoredumpDir: "/var/lib/redpanda/coredump",
		},
		// enable pandaproxy and schema_registry by default
		Pandaproxy:     &Pandaproxy{},
		SchemaRegistry: &SchemaRegistry{},
	}

	if err := p.readConfig(fs, c); err != nil {
		// Sometimes a config file will not exist (e.g. rpk running on MacOS),
		// which is OK. In those cases, just return the default config.
		if !errors.Is(err, afero.ErrFileNotFound) {
			return nil, err
		}
	}
	c.backcompat()
	if err := p.processOverrides(c); err != nil {
		return nil, err
	}
	c.addUnsetDefaults()
	return c, nil
}

// Write writes loaded configuration parameters to redpanda.yaml.
func (c *Config) Write(fs afero.Fs) (rerr error) {
	cfgPath := c.loadedPath
	if cfgPath == "" {
		cfgPath = c.ConfigFile
	}
	b, err := yaml.Marshal(c)
	if err != nil {
		return fmt.Errorf("marshal error in loaded config, err: %s", err)
	}

	// Create a temp file.
	layout := "20060102150405" // year-month-day-hour-min-sec
	bFilename := "redpanda-" + time.Now().Format(layout) + ".yaml"
	temp := filepath.Join(filepath.Dir(cfgPath), bFilename)

	err = afero.WriteFile(fs, temp, b, 0o644) // default permissions 644
	if err != nil {
		return fmt.Errorf("error writing to temporary file: %v", err)
	}
	defer func() {
		if rerr != nil {
			if removeErr := fs.Remove(temp); removeErr != nil {
				rerr = fmt.Errorf("%s, unable to remove temp file: %v", rerr, removeErr)
			} else {
				rerr = fmt.Errorf("%s, temp file removed from disk", rerr)
			}
		}
	}()

	// If we have a loaded file we keep permission and ownership of the
	// original config file.
	if c.loadedPath != "" {
		stat, err := fs.Stat(c.loadedPath)
		if err != nil {
			return fmt.Errorf("unable to stat existing file: %v", err)
		}

		err = fs.Chmod(temp, stat.Mode())
		if err != nil {
			return fmt.Errorf("unable to chmod temp config file: %v", err)
		}

		// Stat_t is only valid in unix not on Windows.
		if stat, ok := stat.Sys().(*syscall.Stat_t); ok {
			gid := int(stat.Gid)
			uid := int(stat.Uid)
			err = fs.Chown(temp, uid, gid)
			if err != nil {
				return fmt.Errorf("unable to chown temp config file: %v", err)
			}
		}
	}

	err = fs.Rename(temp, cfgPath)
	if err != nil {
		return err
	}

	return nil
}

func (p *Params) LocateConfig(fs afero.Fs) (string, error) {
	paths := []string{p.ConfigPath}
	if p.ConfigPath == "" {
		paths = nil
		if configDir, _ := os.UserConfigDir(); configDir != "" {
			paths = append(paths, filepath.Join(configDir, "rpk", "rpk.yaml"))
		}
		paths = append(paths, filepath.FromSlash("/etc/redpanda/redpanda.yaml"))
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
			fmt.Println("Existent path: ", path)
			return path, nil
		}
	}
	fmt.Println("Using default config! ")
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
	fmt.Printf("[readConfig] reading from path %q, config: %s", path, string(file))
	if err := yaml.Unmarshal(file, c); err != nil {
		return fmt.Errorf("unable to yaml decode %s: %v", path, err)
	}
	yaml.Unmarshal(file, &c.file) // cannot error since previous did not
	c.loadedPath = path
	c.ConfigFile = path
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

	// To override, we lookup any override key (e.g., kafka.tls.enabled or
	// admin.hosts) into this map. If the key exists, we processes the
	// value as appropriate (per the value function in the map).
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
		{EnvBrokers, xKafkaBrokers},
		{EnvTLSCA, xKafkaCACert},
		{EnvTLSCert, xKafkaClientCert},
		{EnvTLSKey, xKafkaClientKey},
		{EnvSASLMechanism, xKafkaSASLMechanism},
		{EnvSASLUser, xKafkaSASLUser},
		{EnvSASLPass, xKafkaSASLPass},
		{EnvAdminHosts, xAdminHosts},
		{EnvAdminTLSCA, xAdminCACert},
		{EnvAdminTLSCert, xAdminClientCert},
		{EnvAdminTLSKey, xAdminClientKey},
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
	r := &c.Rpk

	brokers := r.KafkaAPI.Brokers
	defer func() { r.KafkaAPI.Brokers = brokers }()
	if len(brokers) == 0 && len(c.Redpanda.KafkaAPI) > 0 {
		b0 := c.Redpanda.KafkaAPI[0]
		brokers = []string{net.JoinHostPort(b0.Address, strconv.Itoa(b0.Port))}
	}
	if len(brokers) == 0 {
		brokers = []string{"127.0.0.1:9092"}
	}

	if len(r.AdminAPI.Addresses) == 0 {
		r.AdminAPI.Addresses = []string{"127.0.0.1:9644"}
	}
}

// Set allow to set a single configuration property by passing a key value pair
//
//   Key:    string containing the yaml property tag, e.g: 'rpk.admin_api'.
//   Value:  string representation of the value, either single value or partial
//           representation if a format is passed (yaml / json).
//   Format: either single, json, or yaml (default: single).
func (c *Config) Set(key, value, format string) error {
	if key == "" {
		return fmt.Errorf("key field must not be empty")
	}
	props := strings.Split(key, ".")
	rv := reflect.ValueOf(c).Elem()
	found, other, err := getField(props, rv)
	if err != nil {
		return err
	}
	isOther := other != reflect.Value{}

	field := found
	if isOther {
		field = other
	}

	if field.CanAddr() {
		i := field.Addr().Interface()
		in := value
		switch strings.ToLower(format) {
		case "yaml", "single", "":
			if isOther {
				p := props[len(props)-1]
				in = fmt.Sprintf("%s: %s", p, value)
			}
			err = yaml.Unmarshal([]byte(in), i)
			if err != nil {
				return err
			}
			return nil
		case "json":
			if isOther {
				p := props[len(props)-1]
				in = fmt.Sprintf("{%q: %q}", p, value)
			}
			err = json.Unmarshal([]byte(in), i)
			if err != nil {
				return err
			}
			return nil
		default:
			return fmt.Errorf("unsupported format %s", format)
		}
	}
	return errors.New("rpk bug, please describe how you encountered this at https://github.com/redpanda-data/redpanda/issues/new?assignees=&labels=kind%2Fbug&template=01_bug_report.md")
}

// getField deeply search in p for the value that reflect property props.
func getField(props []string, p reflect.Value) (reflect.Value, reflect.Value, error) {
	if len(props) == 0 {
		return p, reflect.Value{}, nil
	}
	if p.Kind() == reflect.Slice && p.Len() > 0 {
		p = p.Index(0)
	}
	if p.Kind() == reflect.Slice && p.Len() == 0 {
		p.Set(reflect.New(p.Type().Elem()))
	}
	// If is a nil pointer we assign the zero value, and we reassign p to the
	// value that p points to
	if p.Kind() == reflect.Ptr {
		if p.IsNil() {
			p.Set(reflect.New(p.Type().Elem()))
		}
		p = reflect.Indirect(p)
	}
	if p.Kind() == reflect.Struct {
		newP, other, err := getFieldByTag(props[0], p)
		if err != nil {
			return reflect.Value{}, reflect.Value{}, err
		}
		// if is "Other" map field, we stop the recursion and return
		if (other != reflect.Value{}) {
			// user may try to set deep unmanaged property:
			// rpk.unmanaged.name = "name"
			if len(props) > 1 {
				return reflect.Value{}, reflect.Value{}, fmt.Errorf("cannot set property %v", strings.Join(props, "."))
			}
			return reflect.Value{}, other, nil
		}
		return getField(props[1:], newP)
	}
	return reflect.Value{}, reflect.Value{}, fmt.Errorf("cannot recurse on type %v", p.Type())
}

// getFieldByTag finds a field with a given yaml tag and returns 3 parameters:
//
//   1. if tag is found within the struct, return the field.
//   2. if tag is not found _but_ the struct has "Other" field, return Other.
//   3. Error if it can't find the given tag and "Other" field is unavailable.
func getFieldByTag(tag string, p reflect.Value) (reflect.Value, reflect.Value, error) {
	t := p.Type()
	var other bool
	// Loop struct to get the field that match tag.
	for i := 0; i < p.NumField(); i++ {
		// Rpk blindly set configuration parameters, we parse these parameters
		// in the "Other" field.
		if t.Field(i).Name == "Other" {
			other = true
		}
		yt := t.Field(i).Tag.Get("yaml")

		// yaml struct tags can contain flags such as omitempty,
		// when tag.Get("yaml") is called it will return
		//   "my_tag,omitempty"
		// so we only need first parameter of the string slice.
		ft := strings.Split(yt, ",")[0]

		if ft == tag {
			return p.Field(i), reflect.Value{}, nil
		} else {
			continue
		}
	}

	// If we can't find the tag but the struct has an 'Other' map field:
	if other {
		return reflect.Value{}, p.FieldByName("Other"), nil
	}

	return reflect.Value{}, reflect.Value{}, fmt.Errorf("couldn't find property %q", tag)
}
