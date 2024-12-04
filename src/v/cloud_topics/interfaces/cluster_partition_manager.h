/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cluster/fwd.h"
#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "storage/types.h"

namespace experimental::cloud_topics {

/// This is a substitute for the 'cluster::partition' in 'cloud_topics'.
/// The implementation should be straightforward. The interface just includes
/// a subset of the 'cluster::partition' interface which is used by
/// cloud_topics.
///
/// - access dl_stm associated with the partition
/// - make a log_reader
/// - return aborted transactions
struct cluster_partition_api {
    cluster_partition_api() = default;
    cluster_partition_api(const cluster_partition_api&) = delete;
    cluster_partition_api(cluster_partition_api&&) = delete;
    cluster_partition_api& operator=(const cluster_partition_api&) = delete;
    cluster_partition_api& operator=(cluster_partition_api&&) = delete;

    virtual ~cluster_partition_api() = default;

    /// Return aborted transactions
    virtual ss::future<fragmented_vector<model::tx_range>>
    aborted_transactions(model::offset base, model::offset last) const = 0;

    /// Create partition's record batch reader
    virtual ss::future<model::record_batch_reader> make_reader(
      storage::log_reader_config config,
      std::optional<model::timeout_clock::time_point> debounce_deadline
      = std::nullopt)
      = 0;
};

/// Wrapper for the 'cluster::partition_manager'.
/// Creates a wrapper for the 'cluster::partition' and returns it.
struct cluster_partition_manager_api {
    cluster_partition_manager_api() = default;
    virtual ~cluster_partition_manager_api() = default;
    cluster_partition_manager_api(const cluster_partition_manager_api&)
      = delete;
    cluster_partition_manager_api(cluster_partition_manager_api&&) = delete;
    cluster_partition_manager_api&
    operator=(const cluster_partition_manager_api&)
      = delete;
    cluster_partition_manager_api& operator=(cluster_partition_manager_api&&)
      = delete;

    /// Returns partition or nullptr if the NTP can't be found
    virtual ss::shared_ptr<cluster_partition_api>
    get_partition(const model::ntp&) = 0;
};

/// Make cluster::partition_manager wrapper that can be used in cloud topics
ss::shared_ptr<cluster_partition_manager_api>
make_cluster_partition_manager(cluster::partition_manager&);

} // namespace experimental::cloud_topics
