/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_topics/cluster_services.h"
#include "cluster/cluster_epoch_service.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>

#include <expected>

namespace experimental::cloud_topics::l0 {

class cluster_services : public experimental::cloud_topics::cluster_services {
public:
    explicit cluster_services(
      ss::sharded<cluster::cluster_epoch_service<>>& epoch_generator)
      : _epoch_service(epoch_generator) {}

    seastar::future<experimental::cloud_topics::cluster_epoch>
    current_epoch(seastar::abort_source* as) override {
        std::expected<int64_t, std::error_code> epoch
          = co_await _epoch_service.local().get_cached_epoch(as);
        if (!epoch) {
            throw std::system_error(epoch.error());
        }
        co_return experimental::cloud_topics::cluster_epoch(epoch.value());
    }

private:
    ss::sharded<cluster::cluster_epoch_service<>>& _epoch_service;
};

} // namespace experimental::cloud_topics::l0
