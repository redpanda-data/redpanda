/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once
#include "kafka/protocol/metadata.h"
#include "kafka/server/handlers/handler.h"

namespace kafka {

/**
 * Estimate the size of a metadata request.
 *
 * Metadata requests are generally very small (a request for *all* metadata
 * about a cluster is less than 30 bytes) but the response may be very large, so
 * the default estimator is unsuitable. See the implementation for further
 * notes.
 */
memory_estimate_fn metadata_memory_estimator;

// IMPORTANT: Do not bump support to v11 (or beyond) unless DescribeCluster v0
// has been implemented.  v11 drops support for the authorized operations list
// and moves those lists to DCv0
// Keep this at v8.  Moving to v9 appears to cause issues with the Kafka Java
// Client
using metadata_handler
  = single_stage_handler<metadata_api, 0, 8, metadata_memory_estimator>;

} // namespace kafka
