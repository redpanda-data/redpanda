// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// Package admin provides a client to interact with Redpanda's admin server.
package admin

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/spf13/afero"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/config"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/net"
)

// AdminAPI is a client to interact with Redpanda's admin server.
type AdminAPI struct {
	urls   []string
	client *http.Client
}

// NewClient returns an AdminAPI client that talks to each of the addresses in
// the rpk.admin_api section of the config.
func NewClient(fs afero.Fs, cfg *config.Config) (*AdminAPI, error) {
	a := &cfg.Rpk.AdminApi
	addrs := a.Addresses
	tc, err := a.TLS.Config(fs)
	if err != nil {
		return nil, fmt.Errorf("unable to create admin api tls config: %v", err)
	}
	return NewAdminAPI(addrs, tc)
}

// NewHostClient returns an AdminAPI that talks to the given host, which is
// either an int index into the rpk.admin_api section of the config, or a
// hostname.
func NewHostClient(
	fs afero.Fs, cfg *config.Config, host string,
) (*AdminAPI, error) {
	if host == "" {
		return nil, errors.New("invalid empty admin host")
	}

	a := &cfg.Rpk.AdminApi
	addrs := a.Addresses
	tc, err := a.TLS.Config(fs)
	if err != nil {
		return nil, fmt.Errorf("unable to create admin api tls config: %v", err)
	}

	i, err := strconv.Atoi(host)
	if err == nil {
		if i < 0 || i >= len(addrs) {
			return nil, fmt.Errorf("admin host %d is out of allowed range [0, %d)", i, len(addrs))
		}
		addrs = []string{addrs[0]}
	} else {
		addrs = []string{host} // trust input is hostname (validate below)
	}

	return NewAdminAPI(addrs, tc)
}

func NewAdminAPI(urls []string, tlsConfig *tls.Config) (*AdminAPI, error) {
	if len(urls) == 0 {
		return nil, errors.New("at least one url is required for the admin api")
	}

	a := &AdminAPI{
		urls:   make([]string, len(urls)),
		client: &http.Client{Timeout: 10 * time.Second},
	}
	if tlsConfig != nil {
		a.client.Transport = &http.Transport{TLSClientConfig: tlsConfig}
	}

	for i, u := range urls {
		scheme, host, err := net.ParseHostMaybeScheme(u)
		if err != nil {
			return nil, err
		}
		switch scheme {
		case "", "http":
			scheme = "http"
			if tlsConfig != nil {
				scheme = "https"
			}
		case "https":
		default:
			return nil, fmt.Errorf("unrecognized scheme %q in host %q", scheme, u)
		}
		a.urls[i] = fmt.Sprintf("%s://%s", scheme, host)
	}

	return a, nil
}

func (a *AdminAPI) urlsWithPath(path string) []string {
	urls := make([]string, len(a.urls))
	for i := 0; i < len(a.urls); i++ {
		urls[i] = fmt.Sprintf("%s%s", a.urls[i], path)
	}
	return urls
}

// rng is a package-scoped, mutex guarded, seeded *rand.Rand.
var rng = func() func(int) int {
	var mu sync.Mutex
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	return func(n int) int {
		mu.Lock()
		defer mu.Unlock()
		return rng.Intn(n)
	}
}()

// sendAny sends a single request to one of the client's urls and unmarshals
// the body into into, which is expected to be a pointer to a struct.
func (a *AdminAPI) sendAny(method, path string, body, into interface{}) error {
	pick := rng(len(a.urls))
	url := a.urls[pick] + path
	res, err := a.sendAndReceive(context.Background(), method, url, body)
	if err != nil {
		return err
	}
	return maybeUnmarshalRespInto(method, url, res, into)
}

// sendOne sends a request with sendAndReceive and unmarshals the body into
// into, which is expected to be a pointer to a struct.
func (a *AdminAPI) sendOne(method, path string, body, into interface{}) error {
	if len(a.urls) != 1 {
		return fmt.Errorf("unable to issue a single-admin-endpoint request to %d admin endpoints", len(a.urls))
	}
	url := a.urls[0] + path
	res, err := a.sendAndReceive(context.Background(), method, url, body)
	if err != nil {
		return err
	}
	return maybeUnmarshalRespInto(method, url, res, into)
}

// sendAll sends a request to all URLs in the admin client. The first successful
// response will be unmarshaled into into if it is non-nil.
//
// As of v21.4.15, the Redpanda admin API doesn't do request forwarding, which
// means that some requests (such as the ones made to /users) will fail unless
// the reached node is the leader. Therefore, a request needs to be made to
// each node, and of those requests at least one should succeed.
// FIXME (@david): when https://github.com/vectorizedio/redpanda/issues/1265
// is fixed.
func (a *AdminAPI) sendAll(method, path string, body, into interface{}) error {
	var (
		once   sync.Once
		resURL string
		res    *http.Response
		grp    multierror.Group

		// When one request is successful, we want to cancel all other
		// outstanding requests. We do not cancel the successful
		// request's context, because the context is used all the way
		// through reading a response body.
		cancels      []func()
		cancelExcept = func(except int) {
			for i, cancel := range cancels {
				if i != except {
					cancel()
				}
			}
		}
	)

	for i, url := range a.urlsWithPath(path) {
		ctx, cancel := context.WithCancel(context.Background())
		myURL := url
		except := i
		cancels = append(cancels, cancel)
		grp.Go(func() error {
			myRes, err := a.sendAndReceive(ctx, method, myURL, body)
			if err != nil {
				return err
			}
			cancelExcept(except) // kill all other requests

			// Only one request should be successful, but for
			// paranoia, we guard keeping the first successful
			// response.
			once.Do(func() { resURL, res = myURL, myRes })
			return nil
		})
	}

	err := grp.Wait()
	if res != nil {
		return maybeUnmarshalRespInto(method, resURL, res, into)
	}
	return err
}

// Unmarshals a response body into `into`, if it is non-nil.
//
// * If into is a *[]byte, the raw response put directly into `into`.
// * If into is a *string, the raw response put directly into `into` as a string.
// * Otherwise, the response is json unmarshaled into `into`.
func maybeUnmarshalRespInto(
	method, url string, resp *http.Response, into interface{},
) error {
	if into == nil {
		return nil
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("unable to read %s %s response body: %w", method, url, err)
	}
	switch t := into.(type) {
	case *[]byte:
		*t = body
	case *string:
		*t = string(body)
	default:
		if err := json.Unmarshal(body, into); err != nil {
			return fmt.Errorf("unable to decode %s %s response body: %w", method, url, err)
		}
	}
	return nil
}

// sendAndReceive sends a request and returns the response. If body is
// non-nil, this json encodes the body and sends it with the request.
func (a *AdminAPI) sendAndReceive(
	ctx context.Context, method, url string, body interface{},
) (*http.Response, error) {
	var r io.Reader
	if body != nil {
		bs, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("unable to encode request body for %s %s: %w", method, url, err) // should not happen
		}
		r = bytes.NewBuffer(bs)
	}

	req, err := http.NewRequestWithContext(ctx, method, url, r)
	if err != nil {
		return nil, err
	}

	const applicationJson = "application/json"
	req.Header.Set("Content-Type", applicationJson)
	req.Header.Set("Accept", applicationJson)

	res, err := a.client.Do(req)
	if err != nil {
		// When the server expects a TLS connection, but the TLS config isn't
		// set/ passed, The client returns an error like
		// Get "http://localhost:9644/v1/security/users": EOF
		// which doesn't make it obvious to the user what's going on.
		if errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("%s to server %s expected a tls connection: %w", method, url, err)
		}
		return nil, err
	}

	if res.StatusCode/100 != 2 {
		resBody, err := ioutil.ReadAll(res.Body)
		status := http.StatusText(res.StatusCode)
		if err != nil {
			return nil, fmt.Errorf("request %s %s failed: %s, unable to read body: %w", method, url, status, err)
		}
		return nil, fmt.Errorf("request %s %s failed: %s, body: %q", method, url, status, resBody)
	}

	return res, nil
}
