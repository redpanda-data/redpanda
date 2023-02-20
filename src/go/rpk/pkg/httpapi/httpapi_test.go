package httpapi

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAll(t *testing.T) {
	var serverFn func(http.ResponseWriter, *http.Request)
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serverFn(w, r)
	}))
	defer s.Close()

	for _, test := range []struct {
		name string

		cl *Client
		fn func(*Client) (interface{}, error)

		respHeader int
		resp       string

		expTries   int
		expMethod  string
		expPath    string
		expHeaders http.Header
		expReqBody string
		expResp    interface{}
		expErr     bool
	}{
		{
			name: "basic_into_string",
			cl:   NewClient(Host(s.URL)),
			fn: func(cl *Client) (interface{}, error) {
				var s string
				return s, cl.Get(context.Background(), "/foo", nil, &s)
			},
			respHeader: 200,
			resp:       "bar",
			expTries:   1,
			expMethod:  http.MethodGet,
			expPath:    "/foo",
			expHeaders: http.Header{
				"Accept-Encoding": []string{"gzip"}, // default from Go
				"Accept":          []string{"application/json"},
				"User-Agent":      []string{"redpanda"},
			},
			expResp: "bar",
		}, {
			name: "basic_into_bytes_with_retries",
			cl:   NewClient(Host(s.URL)),
			fn: func(cl *Client) (interface{}, error) {
				var b []byte
				return b, cl.Get(context.Background(), "/foo2", nil, &b)
			},
			respHeader: 200,
			resp:       "resp",
			expTries:   4,
			expMethod:  http.MethodGet,
			expHeaders: http.Header{
				"Accept-Encoding": []string{"gzip"},
				"Accept":          []string{"application/json"},
				"User-Agent":      []string{"redpanda"},
			},
			expPath: "/foo2",
			expResp: []byte("resp"),
		}, {
			name: "all_options_post",
			cl: NewClient(
				Host(s.URL),
				BasicAuth("user", "pass"),
				Err4xx(func(code int) error { return fmt.Errorf("code: %d", code) }),
				HTTPClient(new(http.Client)),
				MaxBackoff(25*time.Millisecond),
				Retries(300),
				Headers(
					"custom1", "header1",
					"custom2", "header2",
					"accept", "", // delete the default
					"user-agent", "ua3",
				),
				UserAgent("foo"), // overrides Headers set above
			),
			fn: func(cl *Client) (interface{}, error) {
				return nil, cl.Post(
					context.Background(),
					"/bigly",
					Values("query", "param", "qp2", "p=2"),
					"content-type",
					struct{ Foo string }{"bar"}, // `{"Foo":"bar"}`
					nil,
				)
			},
			respHeader: 200,
			resp:       "discarded",
			expTries:   3,
			expMethod:  http.MethodPost,
			expHeaders: http.Header{
				"Accept-Encoding": []string{"gzip"},
				"Authorization":   []string{"Basic dXNlcjpwYXNz"}, // base64(user:pass)
				"User-Agent":      []string{"foo"},
				"Content-Type":    []string{"content-type"},
				"Custom1":         []string{"header1"},
				"Custom2":         []string{"header2"},
				"Content-Length":  []string{"13"}, // len(expBody)
			},
			expPath:    "/bigly?qp2=p%3D2&query=param", // %3D == '='
			expReqBody: `{"Foo":"bar"}`,
		}, {
			name: "post_form",
			cl:   NewClient(Host(s.URL)),
			fn: func(cl *Client) (interface{}, error) {
				var s struct {
					Body string
				}
				return s, cl.PostForm(
					context.Background(),
					"/bigly",
					Values("qp", "qpv"),
					Values("form", "formval", "form2", "formval2"),
					&s,
				)
			},
			respHeader: 200,
			resp:       `{"Body":"seen"}`,
			expTries:   1,
			expMethod:  http.MethodPost,
			expHeaders: http.Header{
				"Accept-Encoding": []string{"gzip"},
				"Accept":          []string{"application/json"},
				"User-Agent":      []string{"redpanda"},
				"Content-Type":    []string{"application/x-www-form-urlencoded"},
				"Content-Length":  []string{"27"},
			},
			expPath:    "/bigly?qp=qpv",
			expReqBody: `form=formval&form2=formval2`,
			expResp:    struct{ Body string }{"seen"},
		}, {
			name: "put_with_iowriter_resp",
			cl:   NewClient(Host(s.URL)),
			fn: func(cl *Client) (interface{}, error) {
				b := new(bytes.Buffer)
				return b, cl.Put(context.Background(), "/put", nil, true, b)
			},
			respHeader: 200,
			resp:       "hello",
			expTries:   1,
			expMethod:  http.MethodPut,
			expHeaders: http.Header{
				"Accept-Encoding": []string{"gzip"},
				"Accept":          []string{"application/json"},
				"User-Agent":      []string{"redpanda"},
				"Content-Length":  []string{"4"},
			},
			expPath:    "/put",
			expReqBody: "true",
			expResp:    func() interface{} { b := new(bytes.Buffer); b.WriteString("hello"); return b }(),
		}, {
			name: "delete",
			cl:   NewClient(Host(s.URL)),
			fn: func(cl *Client) (interface{}, error) {
				return nil, cl.Delete(context.Background(), "/del", nil, nil)
			},
			respHeader: 200,
			expTries:   1,
			expMethod:  http.MethodDelete,
			expHeaders: http.Header{
				"Accept-Encoding": []string{"gzip"},
				"Accept":          []string{"application/json"},
				"User-Agent":      []string{"redpanda"},
			},
			expPath: "/del",
		}, {
			name: "get_pathfmt_err",
			cl:   NewClient(Host(s.URL)),
			fn: func(cl *Client) (interface{}, error) {
				return nil, cl.Get(context.Background(), Pathfmt("/%s/%s", 2, "/"), nil, nil)
			},
			respHeader: 403,
			expTries:   1,
			expMethod:  http.MethodGet,
			expHeaders: http.Header{
				"Accept-Encoding": []string{"gzip"},
				"Accept":          []string{"application/json"},
				"User-Agent":      []string{"redpanda"},
			},
			expPath: "/2/%2F",
			expErr:  true,
		}, {
			name:   "missing_host_in_path",
			cl:     NewClient(),
			fn:     func(cl *Client) (interface{}, error) { return nil, cl.Get(context.Background(), "/", nil, nil) },
			expErr: true,
		}, {
			name:   "missing_url_entirely",
			cl:     NewClient(),
			fn:     func(cl *Client) (interface{}, error) { return nil, cl.Get(context.Background(), "", nil, nil) },
			expErr: true,
		}, {
			name:   "invalid_query",
			cl:     NewClient(Host((s.URL))),
			fn:     func(cl *Client) (interface{}, error) { return nil, cl.Get(context.Background(), "/?;", nil, nil) },
			expErr: true,
		}, {
			name: "canceled_context",
			cl:   NewClient(Host((s.URL))),
			fn: func(cl *Client) (interface{}, error) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return nil, cl.Get(ctx, "/", nil, nil)
			},
			expErr: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			var (
				tries   int
				method  string
				path    string
				headers http.Header
				reqBody string
			)
			serverFn = func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				tries++
				method = r.Method
				headers = r.Header
				path = r.URL.Path
				if r.URL.RawPath != "" {
					path = r.URL.RawPath
				}
				if q := r.URL.Query().Encode(); q != "" {
					path = path + "?" + q
				}
				reqBody = string(body)

				if tries < test.expTries {
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				w.WriteHeader(test.respHeader)
				io.WriteString(w, test.resp)
			}

			resp, err := test.fn(test.cl)

			if gotErr := err != nil; gotErr != test.expErr {
				t.Errorf("got err? %v (%v), exp err? %v", gotErr, err, test.expErr)
			}

			require.Equal(t, test.expTries, tries, "mismatching tries")
			require.Equal(t, test.expMethod, method, "mismatching method")
			require.Equal(t, test.expHeaders, headers, "mismatching headers")
			require.Equal(t, test.expPath, path, "mismatching path")
			require.Equal(t, test.expReqBody, reqBody, "mismatching request body")
			require.Equal(t, test.expResp, resp, "mismatching response")
		})
	}
}
