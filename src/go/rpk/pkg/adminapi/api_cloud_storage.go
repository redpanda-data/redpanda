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
	"fmt"
	"net/http"
)

// RecoveryRequestParams represents the request body schema for the automated recovery API endpoint.
type RecoveryRequestParams struct {
	TopicNamesPattern string `json:"topic_names_pattern"`
	RetentionBytes    *int   `json:"retention_bytes,omitempty"`
	RetentionMs       *int   `json:"retention_ms,omitempty"`
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

// CloudStorageStatus represents the status of a partition of a topic in the
// cloud storage.
type CloudStorageStatus struct {
	CloudStorageMode          string `json:"cloud_storage_mode"`                    // The partition's cloud storage mode (one of: disabled, write_only, read_only, full and read_replica).
	MsSinceLastManifestUpload int    `json:"ms_since_last_manifest_upload"`         // Delta in milliseconds since the last upload of the partition's manifest.
	MsSinceLastSegmentUpload  int    `json:"ms_since_last_segment_upload"`          // Delta in milliseconds since the last segment upload for the partition.
	MsSinceLastManifestSync   *int   `json:"ms_since_last_manifest_sync,omitempty"` // Delta in milliseconds since the last manifest sync (only present for read replicas).
	TotalLogBytes             int    `json:"total_log_size_bytes"`                  // Total size of the log for the partition (overlap between local and cloud log is excluded).
	CloudLogBytes             int    `json:"cloud_log_size_bytes"`                  // Total size of the addressable cloud log for the partition.
	LocalLogBytes             int    `json:"local_log_size_bytes"`                  // Total size of the addressable local log for the partition.
	CloudLogSegmentCount      int    `json:"cloud_log_segment_count"`               // Number of segments in the cloud log (does not include segments queued for removal).
	LocalLogSegmentCount      int    `json:"local_log_segment_count"`               // Number of segments in the local log.
	CloudLogStartOffset       int    `json:"cloud_log_start_offset"`                // The first Kafka offset accessible from the cloud (inclusive).
	CloudLogLastOffset        int    `json:"cloud_log_last_offset"`                 // The last Kafka offset accessible from the cloud (inclusive).
	LocalLogStartOffset       int    `json:"local_log_start_offset"`                // The first Kafka offset accessible locally (inclusive).
	LocalLogLastOffset        int    `json:"local_log_last_offset"`                 // The last Kafka offset accessible locally (inclusive).
	MetadataUpdatePending     bool   `json:"metadata_update_pending"`               // If true, the remote metadata may not yet include all segments that have been uploaded.
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

func (a *AdminAPI) CloudStorageStatus(ctx context.Context, topic, partition string) (CloudStorageStatus, error) {
	var response CloudStorageStatus
	path := fmt.Sprintf("/v1/cloud_storage/status/%s/%s", topic, partition)
	return response, a.sendAny(ctx, http.MethodGet, path, http.NoBody, &response)
}
