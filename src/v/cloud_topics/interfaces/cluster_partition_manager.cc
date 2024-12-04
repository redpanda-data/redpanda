/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/interfaces/cluster_partition_manager.h"

#include "cluster/partition.h"
#include "cluster/partition_manager.h"

#include <seastar/core/shared_ptr.hh>

namespace experimental::cloud_topics {

struct cluster_partition : cluster_partition_api {
    explicit cluster_partition(ss::lw_shared_ptr<cluster::partition> p)
      : _partition(std::move(p)) {}

    /// Return aborted transactions
    ss::future<fragmented_vector<model::tx_range>> aborted_transactions(
      model::offset base, model::offset last) const override {
        co_return co_await _partition->aborted_transactions(base, last);
    }

    /// Create partition's record batch reader
    ss::future<model::record_batch_reader> make_reader(
      storage::log_reader_config config,
      std::optional<model::timeout_clock::time_point> debounce_deadline
      = std::nullopt) override {
        // TODO: translate
        co_return co_await _partition->make_reader(config, debounce_deadline);
    }

    ss::lw_shared_ptr<cluster::partition> _partition;
};

struct cluster_partition_manager : cluster_partition_manager_api {
    explicit cluster_partition_manager(cluster::partition_manager& pm)
      : _pm(&pm) {}

    /// Returns partition or nullptr if the NTP can't be found
    ss::shared_ptr<cluster_partition_api>
    get_partition(const model::ntp& ntp) override {
        auto p = _pm->get(ntp);
        if (p == nullptr) {
            return nullptr;
        }
        return ss::make_shared<cluster_partition>(p);
    }

    cluster::partition_manager* _pm;
};

ss::shared_ptr<cluster_partition_manager_api>
make_cluster_partition_manager(cluster::partition_manager& pm) {
    return ss::make_shared<cluster_partition_manager>(pm);
}

} // namespace experimental::cloud_topics
