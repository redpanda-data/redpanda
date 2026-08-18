// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package publicapi

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"buf.build/gen/go/redpandadata/dataplane/connectrpc/go/redpanda/api/dataplane/v1/dataplanev1connect"
	dataplanev1 "buf.build/gen/go/redpandadata/dataplane/protocolbuffers/go/redpanda/api/dataplane/v1"
	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
)

// pagedSecretService serves totalSecrets secrets, honoring page_size and
// page_token the way the Public API does, so that a client which ignores
// pagination only ever sees the first page.
type pagedSecretService struct {
	dataplanev1connect.UnimplementedSecretServiceHandler

	totalSecrets int
	gotFilters   []*dataplanev1.ListSecretsFilter
	gotPageSizes []int32
}

func (s *pagedSecretService) ListSecrets(_ context.Context, req *connect.Request[dataplanev1.ListSecretsRequest]) (*connect.Response[dataplanev1.ListSecretsResponse], error) {
	s.gotFilters = append(s.gotFilters, req.Msg.GetFilter())
	s.gotPageSizes = append(s.gotPageSizes, req.Msg.GetPageSize())

	pageSize := int(req.Msg.GetPageSize())
	if pageSize <= 0 {
		pageSize = 100
	}

	// The page token is the index of the first secret in the page.
	var start int
	if token := req.Msg.GetPageToken(); token != "" {
		if _, err := fmt.Sscanf(token, "%d", &start); err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("bad page token %q", token))
		}
	}

	end := min(start+pageSize, s.totalSecrets)
	secrets := make([]*dataplanev1.Secret, 0, end-start)
	for i := start; i < end; i++ {
		secrets = append(secrets, dataplanev1.Secret_builder{Id: fmt.Sprintf("SECRET_%d", i)}.Build())
	}

	var nextPageToken string
	if end < s.totalSecrets {
		nextPageToken = fmt.Sprintf("%d", end)
	}
	return connect.NewResponse(dataplanev1.ListSecretsResponse_builder{
		Secrets:       secrets,
		NextPageToken: nextPageToken,
	}.Build()), nil
}

func newTestSecretClientSet(t *testing.T, svc *pagedSecretService) *DataPlaneClientSet {
	t.Helper()
	mux := http.NewServeMux()
	mux.Handle(dataplanev1connect.NewSecretServiceHandler(svc))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	cl, err := NewDataPlaneClientSet(srv.URL, "test-token")
	require.NoError(t, err)
	return cl
}

func TestListAllSecrets(t *testing.T) {
	// 250 secrets span three pages at a page size of 100. A client that does
	// not follow next_page_token would stop at 100.
	for _, tt := range []struct {
		name         string
		totalSecrets int
		expRequests  int
	}{
		{name: "empty", totalSecrets: 0, expRequests: 1},
		{name: "single partial page", totalSecrets: 42, expRequests: 1},
		{name: "exactly one page", totalSecrets: 100, expRequests: 1},
		{name: "beyond the first page", totalSecrets: 101, expRequests: 2},
		{name: "several pages", totalSecrets: 250, expRequests: 3},
	} {
		t.Run(tt.name, func(t *testing.T) {
			svc := &pagedSecretService{totalSecrets: tt.totalSecrets}
			cl := newTestSecretClientSet(t, svc)

			secrets, err := cl.ListAllSecrets(context.Background(), nil)
			require.NoError(t, err)
			require.Len(t, secrets, tt.totalSecrets)
			require.Len(t, svc.gotPageSizes, tt.expRequests)

			// Every secret is returned exactly once, in order.
			for i, secret := range secrets {
				require.Equal(t, fmt.Sprintf("SECRET_%d", i), secret.GetId())
			}
			// A page size must be requested, otherwise we are at the mercy of
			// the server default.
			for _, pageSize := range svc.gotPageSizes {
				require.NotZero(t, pageSize)
			}
		})
	}
}

func TestListAllSecretsForwardsFilterToEveryPage(t *testing.T) {
	svc := &pagedSecretService{totalSecrets: 250}
	cl := newTestSecretClientSet(t, svc)

	filter := dataplanev1.ListSecretsFilter_builder{
		NameContains: "MY_SECRET",
		Scopes:       []dataplanev1.Scope{dataplanev1.Scope_SCOPE_REDPANDA_CLUSTER},
	}.Build()

	_, err := cl.ListAllSecrets(context.Background(), filter)
	require.NoError(t, err)

	require.Len(t, svc.gotFilters, 3)
	for _, got := range svc.gotFilters {
		require.Equal(t, "MY_SECRET", got.GetNameContains())
		require.Equal(t, []dataplanev1.Scope{dataplanev1.Scope_SCOPE_REDPANDA_CLUSTER}, got.GetScopes())
	}
}
