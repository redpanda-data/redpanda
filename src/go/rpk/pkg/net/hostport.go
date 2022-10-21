package net

import (
	"fmt"
	"net"
	"regexp"
	"strconv"
	"strings"
)

// ParseHostMaybeScheme parses an optional scheme, required valid hostname, and
// optional port from the given input.
//
// In general, URIs require a scheme, so parsing an optional scheme makes
// things a good deal trickier. As well, we ensure that the host is either a
// valid domain name or a valid IP.
//
// This returns the scheme (which may be empty), and the host, optionally
// joined with a port if present.
func ParseHostMaybeScheme(h string) (scheme, host string, err error) {
	scheme, host, port, err := splitSchemeHostPort(h)
	if err != nil {
		return "", "", err
	}
	if port != "" {
		// We can return host + port since splitSchemeHostPort already
		// ensures an IPV6 host is wrapped in brackets.
		return scheme, host + ":" + port, nil
	}
	return scheme, host, nil
}

// SplitHostPortDefault splits h into its host and port parts and returns the
// port as an int. If the host has no port, the returns the default port.
//
// The input is expected to be a valid parsed address as returned from
// ParseHostMaybeScheme. This always returns the input host and default port if
// any split / int conversion error occurs.
func SplitHostPortDefault(h string, def int) (host string, port int) {
	host, p, err := net.SplitHostPort(h)
	if err != nil {
		return h, def
	}
	port, err = strconv.Atoi(p)
	if err != nil {
		return h, def
	}
	return host, port
}

// https://en.wikipedia.org/wiki/Uniform_Resource_Identifier#Syntax
// https://datatracker.ietf.org/doc/html/rfc3986#section-3.1
//
// The tricky thing about optional schemes is that in a URI, the host
// (authority) is actually the optional part. Blindly relying on url.Parse
// will have unexpected results almost all of the time.
func splitSchemeHostPort(h string) (scheme, host, port string, err error) {
	m := schemeHostPortRe.FindStringSubmatch(h)
	setErr := func() {
		err = fmt.Errorf(`invalid host %q does not match "host", nor "host:port", nor "scheme://host:port"`, h)
	}
	if len(m) == 0 {
		setErr()
		return
	}
	scheme, host, port = m[1], m[2], m[3]
	if !isDomain(host) && !isIP(host) {
		setErr()
		return
	}
	return
}

// This regexp captures an optional scheme, a required authority, and an
// optional port. A scheme has the following syntax:
//
// """
// A non-empty scheme component followed by a colon (:), consisting of
// a sequence of characters beginning with a letter and followed by any
// combination of letters, digits, plus (+), period (.), or hyphen (-).
// """
//
//   - If the scheme is present, we require :// to follow.
//   - We absolutely require an authority.
//   - We optionally allow a port :\d+.
//   - We optionally allow the single path, "/".
//
// This regexp is compilicated, but what FindStringSubmatch will return if it
// matches at all:
//   - index 0: the full match
//   - index 1: the scheme, if present
//   - index 2: the host
//   - index 3: the port, if present
//
// We then validate the host against isDomain / net.ParseIP.
//
// For schemes, we relax RFC3986 section 3.1 by also allowing underscores after
// the first alphabetic character. This allows us to parse "PLAINTEXT_HOST",
// which is technically invalid, but which Kafka uses.
// https://datatracker.ietf.org/doc/html/rfc3986#section-3.1
var schemeHostPortRe = regexp.MustCompile(`^(?:([a-zA-Z][a-zA-Z0-9+._-]*)://)?(.*?)(?::(\d+))?(?:/)?$`)

// https://serverfault.com/a/638270
// https://datatracker.ietf.org/doc/html/rfc3986#section-3.2.2
// https://stackoverflow.com/questions/9071279/number-in-the-top-level-domain
// https://www.icann.org/en/system/files/files/ua-factsheet-a4-17dec15-en.pdf
//
//   - labels must begin or end with alphanum
//   - labels can contain a-zA-Z0-9-
//   - labels must be 1 to 63 chars
//   - the full domain must not exceed 255 characters
//
// We could validate TLDs, but intranet / localhost resolver overrides can
// resolve single label hosts, as well as underscores (which are technically
// only valid in DNS names, not hostnames). However, we do require that the
// final label must start with a letter and be more than 1 byte long.
func isDomain(d string) bool {
	if len(d) > 255 {
		return false
	}

	// The rightmost label can be followed by a .; we strip that here.
	d = strings.TrimSuffix(d, ".")

	labels := strings.Split(d, ".")

	l := len(labels)
	if l == 0 {
		return false
	}

	last := labels[len(labels)-1]
	if len(last) < 2 {
		return false
	}
	if b := last[0]; b >= '0' && b <= '9' {
		return false
	}

	for _, label := range labels {
		if l := len(label); l == 0 || l > 63 {
			return false
		}
		if !labelRe.MatchString(label) {
			return false
		}
	}
	return true
}

// labels must begin or end with alphanum, but can include dashes or
// underscores in the middle.
var labelRe = regexp.MustCompile(`^[a-zA-Z0-9](?:[a-zA-Z0-9_-]*[a-zA-Z0-9])?$`)

// Returns whether the input is an ip address.
//
// IPv6 addresses must be wrapped in braces [], and IPv4 cannot be.
func isIP(i string) bool {
	if strings.HasPrefix(i, "[") && strings.HasSuffix(i, "]") {
		if strings.IndexByte(i, '.') != -1 { // if we have dots, this is ipv4 and should not be in braces
			return false
		}
		i = i[1 : len(i)-1]
	} else if strings.IndexByte(i, ':') != -1 { // if we have no braces, a colon implies ipv6 which is invalid without braces
		return false
	}
	// All IP addresses can be represented as IPv6, so if we can parse and
	// have an ipv6 form, the input is valid.
	return net.ParseIP(i).To16() != nil
}
