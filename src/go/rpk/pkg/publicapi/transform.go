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

	commonv1alpha1 "buf.build/gen/go/redpandadata/common/protocolbuffers/go/redpanda/api/common/v1alpha1"
	"buf.build/gen/go/redpandadata/dataplane/connectrpc/go/redpanda/api/dataplane/v1alpha1/dataplanev1alpha1connect"
	v1alpha1 "buf.build/gen/go/redpandadata/dataplane/protocolbuffers/go/redpanda/api/dataplane/v1alpha1"
	"connectrpc.com/connect"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/httpapi"
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
	Metadata   *v1alpha1.DeployTransformRequest
	WasmBinary io.Reader
}

func newTransformServiceClient(httpClient *http.Client, host, authToken string, opts ...connect.ClientOption) transformServiceClient {
	httpOpts := []httpapi.Opt{
		httpapi.Host(host),
		httpapi.BearerAuth(authToken),
		httpapi.HTTPClient(httpClient),
		httpapi.Err4xx(func(code int) error {
			return &ConnectError{StatusCode: code}
		}),
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

func (tsc *transformServiceClient) DeployTransform(ctx context.Context, r DeployTransformRequest) error {
	body := &bytes.Buffer{}
	// The body is a multipart/form data with 2 fields: metadata, and
	// wasm_binary.
	writer := multipart.NewWriter(body)

	// Write metadata to the form.
	metadataPart, err := writer.CreateFormField("metadata")
	if err != nil {
		return fmt.Errorf("unable to create 'metadata' form field: %v", err)
	}
	if err := json.NewEncoder(metadataPart).Encode(r.Metadata); err != nil {
		return fmt.Errorf("unable to encode 'metadata': %v", err)
	}

	// Write binary bytes to the form.
	wasmPart, err := writer.CreateFormFile("wasm_binary", r.Metadata.Name+".wasm")
	if err != nil {
		return fmt.Errorf("unable to create form file: %v", err)
	}
	_, err = io.Copy(wasmPart, r.WasmBinary)
	if err != nil {
		return fmt.Errorf("unable to copy wasm binary: %v", err)
	}

	err = writer.Close()
	if err != nil {
		return fmt.Errorf("unable to close form writer: %v", err)
	}

	tsc.httpCl = tsc.httpCl.With(httpapi.Headers("Content-Type", writer.FormDataContentType()))
	err = tsc.httpCl.Put(ctx, transformPath, nil, body.Bytes(), nil)
	if err != nil {
		return fmt.Errorf("unable to request transform deployment: %v", err)
	}
	return nil
}

// ConnectError is the error returned by the data plane API.
type ConnectError struct {
	commonv1alpha1.ErrorStatus
	StatusCode int
}

func (e *ConnectError) Error() string {
	return fmt.Sprintf("unexpected status code %d: %v", e.StatusCode, e.Message)
}
