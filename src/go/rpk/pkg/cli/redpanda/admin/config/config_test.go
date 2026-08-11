package config

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"

	"github.com/redpanda-data/common-go/rpadmin"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

func TestParseHelpLoggersOutput(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    []string
		wantErr bool
	}{
		{
			name: "standard output with header",
			input: `Available loggers:
    abs
    admin_api_server
    raft
    storage
`,
			want: []string{"abs", "admin_api_server", "raft", "storage"},
		},
		{
			name: "full output with preamble",
			input: `Welcome to the Redpanda community!

Documentation: https://docs.redpanda.com - Product documentation site
GitHub Discussion: https://github.com/redpanda-data/redpanda/discussions - Longer, more involved discussions
Support: https://support.redpanda.com - Contact the support team privately
Slack: https://redpanda.com/slack - Chat about all things Redpanda. Join the conversation!
Twitter: https://twitter.com/redpandadata - All the latest Redpanda news!

Available loggers:
    abs
    admin_api_server
    raft
    storage
`,
			want: []string{"abs", "admin_api_server", "raft", "storage"},
		},
		{
			name:    "only non-indented lines",
			input:   "abs\nadmin_api_server\nraft\n",
			wantErr: true,
		},
		{
			name:    "indented lines without header",
			input:   "    abs\n    raft\n",
			wantErr: true,
		},
		{
			name:    "empty output",
			input:   "",
			wantErr: true,
		},
		{
			name:    "only header",
			input:   "Available loggers:\n",
			wantErr: true,
		},
		{
			name: "extra blank lines",
			input: `Available loggers:

    raft

    storage

`,
			want: []string{"raft", "storage"},
		},
		{
			name:  "unsorted input returns sorted",
			input: "Available loggers:\n    storage\n    abs\n    raft\n",
			want:  []string{"abs", "raft", "storage"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseHelpLoggersOutput(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestDiscoverLoggers_APISuccess(t *testing.T) {
	loggers := []rpadmin.LoggerLevel{
		{Name: "storage", Level: "info"},
		{Name: "abs", Level: "debug"},
		{Name: "raft", Level: "warn"},
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/loggers" {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(loggers)
			return
		}
		http.NotFound(w, r)
	}))
	defer ts.Close()

	cl, err := rpadmin.NewClient([]string{ts.URL}, nil, new(rpadmin.NopAuth), false)
	require.NoError(t, err)

	// Binary discovery fails (MemMapFs has no redpanda binary), so
	// discoverLoggers falls through to the Admin API, which succeeds.
	got := discoverLoggers(t.Context(), cl, afero.NewMemMapFs())
	require.Equal(t, []string{"abs", "raft", "storage"}, got)
}

func TestDiscoverLoggers_APIFailsFallsBackToDefault(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()

	cl, err := rpadmin.NewClient([]string{ts.URL}, nil, new(rpadmin.NopAuth), false)
	require.NoError(t, err)

	got := discoverLoggers(t.Context(), cl, afero.NewMemMapFs())
	require.Equal(t, defaultLoggers, got)
}

func TestDefaultLoggersIsSorted(t *testing.T) {
	// Just to make sure that when we manually add a logger we keep it sorted.
	require.True(t, sort.StringsAreSorted(defaultLoggers), "defaultLoggers must be sorted")
}
