// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "cloud_storage/remote.h"
#include "cluster/types.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>

#include <absl/container/flat_hash_map.h>

namespace cluster {

enum class validation_result {
    // validation was passed (no_check will be converted to passed)
    passed = 0,
    // no manifest found in ts. it can be ok, if for example there was not
    // enough time to upload a manifest before the crash
    missing_manifest,
    // reading the manifest generated a failure that cannot be handled by
    // redpanda. for example a serde_exception indicating a corrupt file, or
    // missing segments
    anomaly_detected,
    // download of a critical object for validation failed due to timeout or
    // generic failure. might indicate an issue with the external api, or
    // cluster configuration
    download_issue,
};

/// For each partition implicitly referenced by assignable_config, validate its
/// partition manifest. Perform checks based on recovery_checks.mode .
/// Returns a map partition->validation_result that can be used to drive further
/// decisions. In the future, the value of the map can be extended to contain
/// the partition_manifest and other data.
auto maybe_validate_recovery_topic(
  custom_assignable_topic_configuration const& assignable_config,
  cloud_storage_clients::bucket_name bucket,
  cloud_storage::remote& remote,
  ss::abort_source& as)
  -> ss::future<absl::flat_hash_map<model::partition_id, validation_result>>;

} // namespace cluster
