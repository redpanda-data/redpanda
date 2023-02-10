/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_storage_clients/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"

class retry_chain_node;

namespace storage {
class ntp_config;
}

namespace cloud_storage {

class remote;

struct recovery_result {
    model::topic_namespace tp_ns;
    model::partition_id partition;
    ss::sstring uuid;
    bool result;

    recovery_result(
      model::topic_namespace, model::partition_id, ss::sstring, bool);
};

ss::sstring
generate_result_path(const storage::ntp_config& ntp_cfg, bool result);

/// \brief Uploads a result for a completed download by formatting using the
/// recovery_result_format path, this is an empty file so that the list
/// bucket API can be used to get the status.
ss::future<> place_download_result(
  remote& remote,
  cloud_storage_clients::bucket_name bucket,
  const storage::ntp_config& ntp_cfg,
  bool result_completed,
  retry_chain_node& parent);

/// \brief collects recovery results by listing the prefix on the bucket and
/// matching and parsing the recovery file format.
ss::future<std::vector<recovery_result>> gather_recovery_results(
  remote& remote,
  cloud_storage_clients::bucket_name bucket,
  retry_chain_node& parent);

/// \brief removes all recovery results from the result prefix on bucket. If the
/// items_to_delete vector is passed in then it is removed. If not, the results
/// are gathered from the prefix and then removed.
ss::future<> clear_recovery_results(
  remote& remote,
  cloud_storage_clients::bucket_name bucket,
  retry_chain_node& parent,
  std::optional<std::vector<recovery_result>> items_to_delete);

cloud_storage_clients::object_key make_result_path(const recovery_result& r);

} // namespace cloud_storage
