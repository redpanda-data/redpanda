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

#include "base/seastarx.h"
#include "cloud_storage_clients/client_pool.h"

#include <seastar/core/sharded.hh>
#include <seastar/util/defer.hh>

namespace cloud_storage_clients::tests {

class [[nodiscard]] client_pool_stop_guard {
public:
    explicit client_pool_stop_guard(
      std::unique_ptr<ss::sharded<upstream_registry>> upstreams,
      ss::sharded<cloud_storage_clients::client_pool>& pool)
      : upstreams_(std::move(upstreams))
      , pool_(&pool) {}

    client_pool_stop_guard(const client_pool_stop_guard&) = delete;
    client_pool_stop_guard& operator=(const client_pool_stop_guard&) = delete;

    client_pool_stop_guard(client_pool_stop_guard&& other) noexcept
      : upstreams_(std::move(other.upstreams_))
      , pool_(other.pool_) {
        other.pool_ = nullptr;
    }
    client_pool_stop_guard& operator=(client_pool_stop_guard&& other) noexcept {
        if (this != &other) {
            upstreams_ = std::exchange(other.upstreams_, nullptr);
            pool_ = std::exchange(other.pool_, nullptr);
        }
        return *this;
    }

    void release() {
        vassert(
          upstreams_ == nullptr,
          "Cannot release when upstream registry is owned by the guard");
        pool_ = nullptr;
    }

    ~client_pool_stop_guard() {
        if (pool_) {
            pool_->stop().get();
        }
        if (upstreams_) {
            upstreams_->stop().get();
        }
    }

private:
    std::unique_ptr<ss::sharded<upstream_registry>> upstreams_;
    ss::sharded<cloud_storage_clients::client_pool>* pool_;
};

class [[nodiscard]] client_pool_builder {
public:
    explicit constexpr client_pool_builder(client_configuration conf) noexcept
      : conf_(std::move(conf)) {}

    client_pool_builder connections_per_shard(size_t count) const {
        auto copy = *this;
        copy.num_connections_ = count;
        return copy;
    }

    client_pool_builder overdraft_policy(
      cloud_storage_clients::client_pool_overdraft_policy policy) const {
        auto copy = *this;
        copy.overdraft_policy_ = policy;
        return copy;
    }

    client_pool_builder copy() const { return *this; }

    ss::future<client_pool_stop_guard>
    build(ss::sharded<cloud_storage_clients::client_pool>& pool) && {
        auto upstreams = std::make_unique<ss::sharded<upstream_registry>>();
        co_await upstreams->start(conf_);

        co_await pool.start(
          ss::sharded_parameter(
            [&upstreams] { return std::ref(upstreams->local()); }),
          num_connections_,
          std::move(conf_),
          overdraft_policy_);

        std::exception_ptr e;
        try {
            co_await pool.invoke_on_all(
              &cloud_storage_clients::client_pool::start, std::nullopt);
        } catch (...) {
            e = std::current_exception();
        }
        if (e) {
            std::rethrow_exception(e);
        }

        co_return client_pool_stop_guard{std::move(upstreams), pool};
    }

private:
    client_configuration conf_;
    size_t num_connections_{10};
    cloud_storage_clients::client_pool_overdraft_policy overdraft_policy_{
      cloud_storage_clients::client_pool_overdraft_policy::wait_if_empty};
};

}; // namespace cloud_storage_clients::tests
