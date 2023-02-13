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

// TopicDownloadCounts represents the count of downloads for a topic.
type TopicDownloadCounts struct {
	TopicNamespace      string `json:"topic_namespace"`
	PendingDownloads    int    `json:"pending_downloads"`
	SuccessfulDownloads int    `json:"successful_downloads"`
	FailedDownloads     int    `json:"failed_downloads"`
}

// TopicRecoveryStatus represents the status of the automated recovery for a topic.
type TopicRecoveryStatus struct {
	State           string                `json:"state"`
	TopicDownloads  []TopicDownloadCounts `json:"topic_download_counts"`
	RecoveryRequest RecoveryRequestParams `json:"request"`
}

// StartAutomatedRecovery starts the automated recovery process by sending a request to the automated recovery API endpoint.
func (a *AdminAPI) StartAutomatedRecovery(ctx context.Context, topicNamesPattern string) (RecoveryStartResponse, error) {
	requestParams := &RecoveryRequestParams{
		TopicNamesPattern: topicNamesPattern,
	}
	var response RecoveryStartResponse

	return response, a.sendAny(ctx, http.MethodPost, "/v1/cloud_storage/automated_recovery", requestParams, &response)
}

// PollAutomatedRecoveryStatus polls the automated recovery status API endpoint to retrieve the latest status of the recovery process.
func (a *AdminAPI) PollAutomatedRecoveryStatus(ctx context.Context) (*TopicRecoveryStatus, error) {
	var response TopicRecoveryStatus
	return &response, a.sendAny(ctx, http.MethodGet, "/v1/cloud_storage/automated_recovery", http.NoBody, &response)
}
