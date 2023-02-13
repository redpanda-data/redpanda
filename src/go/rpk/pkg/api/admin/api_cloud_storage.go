// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package admin

import (
	"context"
	"net/http"
)

// RecoveryRequestParams represents the request body schema for the automated recovery API endpoint.
type RecoveryRequestParams struct {
	TopicNamesPattern string `json:"topic_names_pattern"`
	RetentionBytes    int    `json:"retention_bytes"`
	RetentionMs       int    `json:"retention_ms"`
}

type RecoveryStartResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// StartAutomatedRecovery starts the automated recovery process by sending a request to the automated recovery API endpoint.
func (a *AdminAPI) StartAutomatedRecovery(ctx context.Context, topicNamesPattern string) (RecoveryStartResponse, error) {
	requestParams := &RecoveryRequestParams{
		TopicNamesPattern: topicNamesPattern,
	}
	var response RecoveryStartResponse

	return response, a.sendAny(ctx, http.MethodPost, "/v1/cloud_storage/automated_recovery", requestParams, &response)
}
