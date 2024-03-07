// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package publicapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"

	"connectrpc.com/connect"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/httpapi"
	v1alpha1 "github.com/redpanda-data/redpanda/src/go/rpk/proto/gen/go/redpanda/api/dataplane/v1alpha1"
	"github.com/redpanda-data/redpanda/src/go/rpk/proto/gen/go/redpanda/api/dataplane/v1alpha1/dataplanev1alpha1connect"
)

const transformPath = "/v1alpha1/transforms"

// transformServiceClient is an extension of the
// dataplanev1alpha1connect.TransformServiceClient to support the
// DeployTransform request.
type transformServiceClient struct {
	tCl    dataplanev1alpha1connect.TransformServiceClient
	httpCl *httpapi.Client
}

type DeployTransformRequest struct {
	metadata   *v1alpha1.DeployTransformRequest
	wasmBinary io.Reader
}

func newTransformServiceClient(httpClient *http.Client, host, authToken string, opts ...connect.ClientOption) transformServiceClient {
	httpOpts := []httpapi.Opt{
		httpapi.Host(host),
		httpapi.BearerAuth(authToken),
		httpapi.HTTPClient(httpClient),
	}
	return transformServiceClient{
		tCl:    dataplanev1alpha1connect.NewTransformServiceClient(httpClient, host, opts...),
		httpCl: httpapi.NewClient(httpOpts...),
	}
}

func (tsc *transformServiceClient) ListTransforms(ctx context.Context, r *connect.Request[v1alpha1.ListTransformsRequest]) (*connect.Response[v1alpha1.ListTransformsResponse], error) {
	return tsc.tCl.ListTransforms(ctx, r)
}

func (tsc *transformServiceClient) GetTransform(ctx context.Context, r *connect.Request[v1alpha1.GetTransformRequest]) (*connect.Response[v1alpha1.GetTransformResponse], error) {
	return tsc.tCl.GetTransform(ctx, r)
}

func (tsc *transformServiceClient) DeleteTransform(ctx context.Context, r *connect.Request[v1alpha1.DeleteTransformRequest]) (*connect.Response[v1alpha1.DeleteTransformResponse], error) {
	return tsc.tCl.DeleteTransform(ctx, r)
}

func (tsc *transformServiceClient) DeployTransform(ctx context.Context, r DeployTransformRequest) (*connect.Response[v1alpha1.TransformMetadata], error) {
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)

	metadataPart, err := writer.CreateFormField("metadata")
	if err != nil {
		return nil, fmt.Errorf("unable to create 'metadata' form field: %v", err)
	}
	if err := json.NewEncoder(metadataPart).Encode(r.metadata); err != nil {
		return nil, fmt.Errorf("unable to encode 'metadata': %v", err)
	}

	wasmPart, err := writer.CreateFormFile("wasm_binary", r.metadata.Name+".wasm")
	if err != nil {
		return nil, fmt.Errorf("unable to create form file: %v", err)
	}
	data, err := io.ReadAll(r.wasmBinary)
	if err != nil {
		return nil, fmt.Errorf("unable to read file: %v", err)
	}
	_, err = wasmPart.Write(data)

	var response v1alpha1.TransformMetadata
	err = tsc.httpCl.Put(ctx, transformPath, nil, body, &response)
	if err != nil {
		return nil, fmt.Errorf("unable to request transform deployment: %v", err)
	}

	return connect.NewResponse(&response), nil
}
