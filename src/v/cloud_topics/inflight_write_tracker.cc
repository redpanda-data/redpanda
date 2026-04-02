/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/inflight_write_tracker.h"

#include "container/chunked_vector.h"
#include "ssx/when_all.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/sharded.hh>

#include <ranges>

namespace cloud_topics {

namespace {

/// Per-shard state: an intrusive list of inflight write tokens.
struct shard_state {
    inflight_write_list writes;

    ss::future<> stop() {
        inflight_write_list draining;
        draining.swap(writes);
        co_await ssx::when_all_succeed(
          std::views::transform(
            draining, [](auto& t) { return t.done.get_future(); })
          | std::ranges::to<chunked_vector<ss::future<>>>());
    }
};

class impl final : public inflight_write_tracker {
public:
    ss::future<> start() override { co_await _shards.start(); }

    ss::future<> stop() override { co_await _shards.stop(); }

    std::unique_ptr<inflight_write_token> track() override {
        auto token = std::make_unique<inflight_write_token>();
        _shards.local().writes.push_back(*token);
        return token;
    }

    ss::future<> drain() override {
        co_await _shards.invoke_on_all([](shard_state& s) -> ss::future<> {
            inflight_write_list local;
            local.swap(s.writes);
            co_await ssx::when_all_succeed(
              std::views::transform(
                local, [](auto& tok) { return tok.done.get_future(); })
              | std::ranges::to<chunked_vector<ss::future<>>>());
        });
    }

private:
    ss::sharded<shard_state> _shards;
};

} // namespace

std::unique_ptr<inflight_write_tracker> inflight_write_tracker::make_default() {
    return std::make_unique<impl>();
}

} // namespace cloud_topics
