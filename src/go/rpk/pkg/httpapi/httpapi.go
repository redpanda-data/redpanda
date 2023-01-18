// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// Package httpapi provides a few small helper types and functions to help
// querying http endpoints. All requesting functions provide an 'into' argument
// that response bodies can be decoded into with the following rules:
//
//   - If into is nil, the response is discarded.
//   - If into is a *[]byte, the raw response body is put into 'into'.
//   - If into is a *string, the response is placed into 'into' as a string.
//   - If into is an *io.Reader, the raw response body is put into 'into' as a
//     bytes.Reader
//   - If into is an io.Writer, the response is copied directly to the writer.
//   - Otherwise, this attempts to json decode the response into 'into' and
//     returns any json unmarshaling error.
package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"math"
	mrand "math/rand"
	"net/http"
	"net/url"
	"runtime"
	"strings"
	"sync"
	"time"
)

// BodyError is returned on non-2xx status codes. For 4xx, you can optionally
// use Err4xx to return custom 4xx error types.
type BodyError struct {
	// StatusCode is the status code causing a retry error.
	StatusCode int
}

func (e *BodyError) Error() string {
	return fmt.Sprintf("unexpected status code %d", e.StatusCode)
}

// Opt configures a client / issuing http requests.
type Opt func(*Client)

// Client is a type that caches options for repeated Do invocations.
type Client struct {
	httpCl     *http.Client
	host       string // technically, the "origin", scheme+host+port
	headers    map[string]string
	retries    int
	maxBackoff time.Duration
	err4xxFn   func(int) error
}

// Close closes the underlying http client's idle connections, freeing any idle
// resources.
//
// This function is optional: if you use your own http.Client to initialize
// this client, then you have the power to close your own idle connections.  If
// you let this client initialize its own http.Client, the underlying
// http.Client will have idle connections closed on GC.
//
// You might want to use this function if you reuse your own http..Client
// widely, but query one specific host that you then stop querying.
func (cl *Client) Close() { cl.httpCl.CloseIdleConnections() }

func (cl *Client) setHeader(k, v string) {
	k = strings.ToLower(k)
	if v == "" {
		delete(cl.headers, k)
	} else {
		cl.headers[k] = v
	}
}

// Host sets all requests to be prefixed with the given host, which simplifies
// specifying paths in requests. Not using this means the full request url must
// be specified in every request.
func Host(host string) Opt { return func(cl *Client) { cl.host = host } }

// UserAgent sets the User-Agent header to use in all requests, overriding the
// default "redpanda".
func UserAgent(ua string) Opt {
	return func(cl *Client) {
		cl.setHeader("User-Agent", ua)
	}
}

// BasicAuth sets the basic authorization to use for the request.
func BasicAuth(user, pass string) Opt {
	return func(cl *Client) {
		h := make(http.Header)
		(&http.Request{Header: h}).SetBasicAuth(user, pass)
		Authorization(h.Get("Authorization"))(cl)
	}
}

// Authorization sets an authorization header for the request if auth is
// non-empty. Setting empty auth strips any authorization header.
func Authorization(auth string) Opt {
	return func(cl *Client) { cl.setHeader("Authorization", auth) }
}

// BearerAuth sets a bearer token authorization header for the request.
func BearerAuth(token string) Opt {
	return func(cl *Client) { cl.setHeader("Authorization", "Bearer "+token) }
}

// HTTPClient sets the http client to use for requests, overriding the default
// that has a 15s timeout.
func HTTPClient(httpCl *http.Client) Opt { return func(cl *Client) { cl.httpCl = httpCl } }

// Headers sets the headers to use for requests. This is an additive function
// and does not override default headers unless the default headers are
// explicitly overridden in this map. This function is an escape hatch if an
// option is not provided to override a specific header.
//
// The default headers are:
//
//	Accept: application/json
//	User-Agent: redpanda
//
// Setting a value to empty deletes the header.
func Headers(kvs ...string) Opt {
	return func(cl *Client) {
		if len(kvs)%2 != 0 {
			kvs = append(kvs, "VALUE_MISSING")
		}
		for i := 0; i < len(kvs); i += 2 {
			cl.setHeader(kvs[i], kvs[i+1])
		}
	}
}

// Retries sets the number of retries before quitting due to request errors,
// overriding the default of 10. Using a negative number opts into unlimited
// retries.
//
// The scheme for retries is to retry after 250ms +/- 50ms jitter, and to
// double the backoff after every failure until the max backoff is reached.
func Retries(n int) Opt { return func(cl *Client) { cl.retries = n } }

// MaxBackoff sets the max backoff between retries, overriding the default 5s.
func MaxBackoff(maxBackoff time.Duration) Opt { return func(cl *Client) { cl.maxBackoff = maxBackoff } }

// Err4xx sets a function to return a type when 4xx responses are encountered.
// On 4xx, the client will call this function for a new type and, if non-nil,
// will decode the 4xx body into this type and return it. The type must
// implement the error interface.
func Err4xx(err4xxFn func(statusCode int) error) Opt {
	return func(cl *Client) { cl.err4xxFn = err4xxFn }
}

// NewClient returns a client to issue HTTP requests.
func NewClient(opts ...Opt) *Client {
	cl := &Client{
		headers: map[string]string{
			"user-agent": "redpanda",
			"accept":     "application/json", // canonicalize as lowercase for Headers fn interaction
		},
		retries:    10,
		maxBackoff: 5 * time.Second,
	}
	for _, opt := range opts {
		opt(cl)
	}

	if cl.retries < 0 {
		cl.retries = math.MaxInt32 // effectively unlimited
	}

	// If no http client was specified, we create our own default and
	// attach a finalizer to avoid leaking goroutines / connections when
	// the client is no longer used.
	if cl.httpCl == nil {
		cl.httpCl = &http.Client{Timeout: 15 * time.Second}
		runtime.SetFinalizer(cl, func(cl *Client) { cl.httpCl.CloseIdleConnections() })
	}

	return cl
}

// With returns a client that can further override aspects of the original
// client. The original client is unaffected. This can be used to set
// per-request options.
func (cl *Client) With(opts ...Opt) *Client {
	dup := *cl
	dup.headers = make(map[string]string, len(cl.headers))
	for k, v := range cl.headers {
		dup.headers[k] = v
	}
	for _, opt := range opts {
		opt(&dup)
	}
	return &dup
}

// Pathfmt accepts a format string path and path-escapes all args into it with
// fmt.Sprintf. This only supports %s path formatting.
func Pathfmt(path string, args ...interface{}) string {
	encoded := make([]interface{}, len(args))
	for i, arg := range args {
		encoded[i] = url.PathEscape(fmt.Sprint(arg))
	}
	return fmt.Sprintf(path, encoded...)
}

// Values is a builder for url.Values that requires key,value pair inputs,
// appending a "VALUE_MISSING" value if there are an odd number of kvs.
func Values(kvs ...string) url.Values {
	if len(kvs)%2 != 0 {
		kvs = append(kvs, "VALUE_MISSING")
	}
	v := make(url.Values)
	for i := 0; i < len(kvs); i += 2 {
		v.Add(kvs[i], kvs[i+1])
	}
	return v
}

// Get GETs the given path, attaching qps as query parameters if non-nil, and
// optionally decodes the response body into 'into' if non-nil following the
// rules of the package documentation.
func (cl *Client) Get(ctx context.Context, path string, qps url.Values, into interface{}) error {
	return cl.do(ctx, http.MethodGet, path, qps, nil, into)
}

// Post POSTs the given path, attaching qps as query parameters if non-nil, and
// optionally decodes the response body into 'into' if non-nil following the
// rules of the package documentation. If body is non-nil, it is json encoded
// as a request body. The required ct parameter sets the Content-Type.
func (cl *Client) Post(ctx context.Context, path string, qps url.Values, ct string, body, into interface{}) error {
	cl = cl.With(Headers("Content-Type", ct))
	return cl.do(ctx, http.MethodPost, path, qps, body, into)
}

// PostForm POSTs the given path with the body specified by form values,
// attaching qps as query parameters if non-nil, and optionally decodes the
// response body into 'into' if non-nil following the rules of the package
// documentation. If body is non-nil, it is json encoded as a request body.
//
// This is the same as POST with form values, but the Content-Type is
// automatically set to "application/x-www-form-urlencoded".
func (cl *Client) PostForm(ctx context.Context, path string, qps, form url.Values, into interface{}) error {
	return cl.do(ctx, http.MethodPost, path, qps, form, into)
}

// Put PUTs the given path, attaching qps as query parameters if non-nil, and
// optionally decodes the response body into 'into' if non-nil following the
// rules of the package documentation. If body is non-nil, it is json encoded
// as a request body.
func (cl *Client) Put(ctx context.Context, path string, qps url.Values, body, into interface{}) error {
	return cl.do(ctx, http.MethodPut, path, qps, body, into)
}

// Delete DELETEs the given path, attaching qps as query parameters if non-nil, and
// optionally decodes the response body into 'into' if non-nil following the
// rules of the package documentation.
func (cl *Client) Delete(ctx context.Context, path string, qps url.Values, into interface{}) error {
	return cl.do(ctx, http.MethodDelete, path, qps, nil, into)
}

func (cl *Client) do(ctx context.Context, method, path string, qps url.Values, body, into interface{}) error {
	fullURL, err := cl.prepareURL(path, qps)
	if err != nil {
		return err
	}

	bodyBytes, isValues, err := prepareBody(body)
	if err != nil {
		return err
	} else if isValues {
		cl = cl.With(Headers("Content-Type", "application/x-www-form-urlencoded"))
	}

	backoff := newBackoff(cl.maxBackoff, cl.retries)

	var resp *http.Response
	for {
		var r io.Reader
		if bodyBytes != nil {
			r = bytes.NewReader(bodyBytes)
		}
		req, err := http.NewRequestWithContext(ctx, method, fullURL, r)
		if err != nil {
			return fmt.Errorf("unable to create request for %s %q: %v", method, fullURL, err)
		}
		for k, v := range cl.headers {
			req.Header.Set(k, v)
		}
		resp, err = cl.httpCl.Do(req)

		if isRetryable, err := backoff.maybeDo(ctx, resp, err); err != nil {
			return err
		} else if isRetryable {
			continue
		}
		break
	}
	defer resp.Body.Close()

	if resp.StatusCode/100 == 4 && cl.err4xxFn != nil {
		err4xx := cl.err4xxFn(resp.StatusCode)
		maybeUnmarshalRespBodyInto(resp.Body, err4xx)
		return err4xx
	}

	if resp.StatusCode/100 != 2 {
		return &BodyError{StatusCode: resp.StatusCode}
	}
	return maybeUnmarshalRespBodyInto(resp.Body, into)
}

func maybeUnmarshalRespBodyInto(r io.Reader, into interface{}) error {
	if into == nil {
		return nil
	}
	if w, ok := into.(io.Writer); ok {
		if _, err := io.Copy(w, r); err != nil {
			return fmt.Errorf("unable to copy response body to writer: %w", err)
		}
		return nil
	}
	body, err := io.ReadAll(r)
	if err != nil {
		return fmt.Errorf("unable to read entire response body: %w", err)
	}
	switch t := into.(type) {
	case *io.Reader:
		*t = bytes.NewReader(body)
	case *[]byte:
		*t = body
	case *string:
		*t = string(body)
	default:
		if err := json.Unmarshal(body, into); err != nil {
			if err := xml.Unmarshal(body, into); err != nil {
				return fmt.Errorf("unable to decode response body: %w", err)
			}
		}
	}
	return nil
}

func (cl *Client) prepareURL(path string, qps url.Values) (string, error) {
	u, err := url.Parse(fmt.Sprintf("%s%s", cl.host, path))
	if err != nil {
		return "", fmt.Errorf("unable to parse url %s%s: %w", cl.host, path, err)
	}
	if u.Scheme == "" || u.Host == "" {
		return "", errors.New("missing scheme://host")
	}
	q, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return "", fmt.Errorf("invalid query parameters in url %s: %w", u.Redacted(), err)
	}
	for k, vs := range qps {
		for _, v := range vs {
			q.Add(k, v)
		}
	}
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func prepareBody(body interface{}) (bodyBytes []byte, isValues bool, err error) {
	if body == nil {
		return nil, false, nil
	}
	switch v := body.(type) {
	case url.Values:
		return []byte(v.Encode()), true, nil
	case io.Reader:
		b, err := io.ReadAll(v)
		return b, false, err
	case []byte:
		return v, false, nil
	}
	bodyBytes, err = json.Marshal(body)
	if err != nil {
		return nil, false, fmt.Errorf("unable to encode request body: %w", err)
	}
	return bodyBytes, false, nil
}

// backoff is a helper that performs a jittery sleep to backoff failed
// requests. We hardcode the initial sleep to 250ms for simplicity and always
// jitter +/- 50ms.
type backoff struct {
	rng        func(int) int
	atBackoff  time.Duration
	atRetries  int
	maxBackoff time.Duration
	maxRetries int
}

func newBackoff(maxBackoff time.Duration, maxRetries int) backoff {
	return backoff{
		rng:        newPrng(),
		atBackoff:  250 * time.Millisecond,
		maxBackoff: maxBackoff,
		maxRetries: maxRetries,
	}
}

// maybeDo performs a backoff if we fail at issuing the request or if we
// receive a 5xx (except 501) or 429 response. These cases are always
// retryable: if our request fails to be issued, we could replay it, but that
// is fine and user controllable with the retry limit.
func (b *backoff) maybeDo(ctx context.Context, resp *http.Response, err error) (bool, error) {
	doBackoff := err != nil
	drainBody := func() {}
	if !doBackoff {
		doBackoff = resp.StatusCode/100 == 5 && resp.StatusCode != http.StatusNotImplemented || resp.StatusCode == http.StatusTooManyRequests
		drainBody = func() { resp.Body.Close() }
	}
	if doBackoff && b.atRetries < b.maxRetries {
		drainBody() // potentially allow connection reuse
		return true, b.do(ctx)
	}
	return false, err
}

func (b *backoff) do(ctx context.Context) error {
	timer := time.NewTimer(b.atBackoff + -50*time.Millisecond + time.Duration(b.rng(100))*time.Millisecond)
	defer func() {
		timer.Stop()
		b.atBackoff *= 2
		b.atRetries++
		if b.atBackoff > b.maxBackoff {
			b.atBackoff = b.maxBackoff
		}
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// newPrng returns a math/rand, mutex protected pseudo number generator.
func newPrng() func(int) int {
	var mu sync.Mutex
	r := mrand.New(mrand.NewSource(time.Now().UnixNano()))
	return func(n int) int {
		mu.Lock()
		defer mu.Unlock()
		return r.Intn(n)
	}
}
