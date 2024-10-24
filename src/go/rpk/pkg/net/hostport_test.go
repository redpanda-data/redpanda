package net

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSplitHostPortDefault(t *testing.T) {
	for _, test := range []struct {
		name   string
		inHost string
		inDef  int

		expHost string
		expPort int
	}{
		{"valid split", "foo:80", 90, "foo", 80},
		{"invalid host uses default", "foo::80", 90, "foo::80", 90},
		{"invalid port uses default", "foo:asdf", 90, "foo:asdf", 90},
	} {
		t.Run(test.name, func(t *testing.T) {
			h, p := SplitHostPortDefault(test.inHost, test.inDef)
			require.Equal(t, h, test.expHost)
			require.Equal(t, p, test.expPort)
		})
	}
}

func TestParseHostMaybeScheme(t *testing.T) {
	for _, test := range []struct {
		name  string
		input string

		expScheme string
		expHost   string
		expErr    bool
	}{
		{
			name:    "valid just host with path slash",
			input:   "foo.com/",
			expHost: "foo.com",
		},

		{
			name:    "valid hostport with path slash",
			input:   "foo.com:8080/",
			expHost: "foo.com:8080",
		},

		{
			name:    "valid hostport no path slash",
			input:   "foo.com:8080",
			expHost: "foo.com:8080",
		},

		{
			name:      "valid scheme://host",
			input:     "scheme://foo.com",
			expScheme: "scheme",
			expHost:   "foo.com",
		},

		{
			name:      "valid scheme://host:port",
			input:     "scheme://foo.com:0",
			expScheme: "scheme",
			expHost:   "foo.com:0",
		},

		{
			name:   "invalid missing slashes in scheme:host:port",
			input:  "scheme:foo.com:0",
			expErr: true,
		},

		{
			name:   "invalid missing numbers in port",
			input:  "scheme://foo.com:",
			expErr: true,
		},

		{
			name:      "relaxed restrictions allow underscore in scheme",
			input:     "scheme_bar://foo.com",
			expScheme: "scheme_bar",
			expHost:   "foo.com",
		},

		{
			name:   "invalid port",
			input:  "scheme://foo.com:9d",
			expErr: true,
		},

		{
			name:   "completely invalid",
			input:  "(",
			expErr: true,
		},

		{
			name:   "empty is invalid",
			input:  "",
			expErr: true,
		},

		// TestIsDomain and TestIsIP below offer more enhanced host
		// validation.
	} {
		t.Run(test.name, func(t *testing.T) {
			scheme, host, err := ParseHostMaybeScheme(test.input)

			gotErr := err != nil
			if gotErr != test.expErr {
				t.Errorf("input %q: got err? %v, exp err? %v",
					test.input, gotErr, test.expErr)
				return
			}
			if test.expErr {
				return
			}

			if scheme != test.expScheme {
				t.Errorf("input %q: got scheme %q != exp %q",
					test.input, scheme, test.expScheme)
			}
			if host != test.expHost {
				t.Errorf("input %q: got host %q != exp %q",
					test.input, host, test.expHost)
			}
		})
	}
}

func TestIsDomain(t *testing.T) {
	for _, test := range []struct {
		name     string
		input    string
		expValid bool
	}{
		{"valid", "foo.com.", true},
		{"underscores and dashes can be anywhere in middle of label", "a.0-_-0.foo.com", true},
		{"labels can be just numbers", "0.com", true},

		{"relaxed tld can contain number", "foo.a0", true},
		{"relaxed tld can contain underscore", "foo.a_0", true},
		{"relaxed tld can contain hyphen", "foo.a-a", true},
		{"tld cannot be numeric", "foo.120", false},
		{"tld length should be > 1", "foo.a", false},
		{"IDN tld are supported", "foo.xn--45brj9c", true},
		{"relaxed restrictions allows just tld", "foo", true},
		{"common docker naming test", "docker_n-1", true},
		{"invalid duplicate dots", "foo..com", false},
		{"label cannot start with dash", "a.-bar.foo.com", false},
		{"label cannot end with dash", "a.bar-.foo.com", false},

		{"labels can be up to 63 characters", strings.Repeat("a", 63) + ".com", true},
		{"labels cannot be more than 63 characters", strings.Repeat("a", 64) + ".com", false},

		{"the entire domain can be exactly 255 characters", strings.Repeat("a.", 125) + "abcde", true},        // 250 + 5
		{"the entire domain cannot be more than 255 characters", strings.Repeat("a.", 125) + "abcdef", false}, // 250 + 6
	} {
		t.Run(test.name, func(t *testing.T) {
			got := isDomain(test.input)
			if got != test.expValid {
				t.Errorf("input %q: got valid? %v, expected? %v",
					test.input, got, test.expValid)
			}
		})
	}
}

func TestIsIP(t *testing.T) {
	for _, test := range []struct {
		name     string
		input    string
		expValid bool
	}{
		{"valid 4", "0.0.0.0", true},
		{"invalid 4", "0.0.0", false},
		{"valid 6", "[::0]", true},
		{"valid 6 missing braces is invalid", "::0", false},
		{"valid 4 cannot be in braces", "[0.0.0.0]", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := isIP(test.input)
			if got != test.expValid {
				t.Errorf("input %q: got valid? %v, expected? %v",
					test.input, got, test.expValid)
			}
		})
	}
}
