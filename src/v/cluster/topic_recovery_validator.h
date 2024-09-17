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
#include "cloud_storage/remote_path_provider.h"
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
/// if the map is empty, it has to be interpreted as "validation ok"
ss::future<absl::flat_hash_map<model::partition_id, validation_result>>
maybe_validate_recovery_topic(
  const custom_assignable_topic_configuration& assignable_config,
  cloud_storage_clients::bucket_name bucket,
  cloud_storage::remote& remote,
  ss::abort_source& as);

/// used by maybe_validate_recovery_topic to validate a single partition
/// manifest.
class partition_validator {
public:
    struct recovery_checks {
        model::recovery_validation_mode mode;
        size_t max_segment_depth;
    };

    partition_validator(
      cloud_storage::remote& remote,
      const cloud_storage_clients::bucket_name& bucket,
      ss::abort_source& as,
      model::ntp ntp,
      model::initial_revision_id rev_id,
      const cloud_storage::remote_path_provider&,
      recovery_checks checks);

    /// Perform validation on the ntp as specified with checks
    ss::future<validation_result> run();

private:
    // check manifest existence, handle download_result::success as Found
    // and the rest as NotFound (but warn for failed and timedout).
    // This existence check could be flattened in anomalies_detector, but
    // it's not a perfect overlap with the functionalities, so it's
    // performed externally
    ss::future<validation_result> do_validate_manifest_existence();

    // delegate to anomalies_detector to run few checks on the remote partition,
    // interpret the result to produce a validation_result
    ss::future<validation_result> do_validate_manifest_metadata();

    // utility method for logging
    cloud_storage::remote_manifest_path get_path();

    cloud_storage::remote* remote_;
    const cloud_storage_clients::bucket_name* bucket_;
    ss::abort_source* as_;
    model::ntp ntp_;
    model::initial_revision_id rev_id_;
    const cloud_storage::remote_path_provider& remote_path_provider_;
    retry_chain_node op_rtc_;
    retry_chain_logger op_logger_;
    recovery_checks checks_;
};
} // namespace cluster
