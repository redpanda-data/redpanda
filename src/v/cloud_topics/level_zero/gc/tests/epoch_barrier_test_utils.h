/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_topics/data_plane_api.h"
#include "cloud_topics/inflight_write_tracker.h"
#include "cloud_topics/level_zero/gc/epoch_barrier.h"
#include "container/chunked_hash_map.h"
#include "model/fundamental.h"
#include "model/namespace.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/shared_future.hh>

#include <atomic>

namespace cloud_topics::l0::gc::testing {

using pinfo = epoch_barrier::partition_source::info;

class mock_partition_source : public epoch_barrier::partition_source {
public:
    struct partition_state {
        model::term_id term;
        bool is_leader{true};
        bool has_epoch{true};
        // Tracks which gc epochs have been written, keyed by ntp string.
        std::optional<cluster_epoch> written_gc_epoch;
        // When true, write_gc_epoch returns false.
        bool fail_writes{false};
    };

    chunked_hash_map<model::ntp, partition_state> partitions;

    chunked_vector<std::pair<model::ntp, info>>
    cloud_topic_partitions() const override {
        chunked_vector<std::pair<model::ntp, info>> result;
        for (const auto& [ntp, state] : partitions) {
            result.emplace_back(
              ntp,
              info{
                .term = state.term,
                .is_leader = state.is_leader,
                .has_epoch = state.has_epoch,
              });
        }
        return result;
    }

    std::optional<info> get(const model::ntp& ntp) const override {
        auto it = partitions.find(ntp);
        if (it == partitions.end()) {
            return std::nullopt;
        }
        const auto& state = it->second;
        return info{
          .term = state.term,
          .is_leader = state.is_leader,
          .has_epoch = state.has_epoch,
        };
    }

    ss::future<bool> write_gc_epoch(
      const model::ntp& ntp,
      cluster_epoch epoch,
      model::timeout_clock::time_point,
      ss::abort_source&) override {
        auto it = partitions.find(ntp);
        if (it == partitions.end()) {
            co_return false;
        }
        if (!it->second.is_leader || it->second.fail_writes) {
            co_return false;
        }
        it->second.written_gc_epoch = epoch;
        co_return true;
    }
};

inline model::ntp
make_ntp(const ss::sstring& topic, model::partition_id::type pid) {
    return model::ntp(
      model::kafka_namespace, model::topic(topic), model::partition_id(pid));
}

class mock_data_plane : public data_plane_api {
public:
    std::atomic<int> drain_count{0};

    ss::future<> start() override { return ss::now(); }
    ss::future<> stop() override { return ss::now(); }

    ss::future<std::expected<staged_write, std::error_code>>
    stage_write(chunked_vector<model::record_batch>) override {
        throw std::logic_error("not implemented");
    }

    ss::future<std::expected<upload_meta, std::error_code>> execute_write(
      model::ntp,
      cluster_epoch,
      staged_write,
      model::timeout_clock::time_point) override {
        throw std::logic_error("not implemented");
    }

    ss::future<result<chunked_vector<model::record_batch>>> materialize(
      model::ntp,
      size_t,
      chunked_vector<extent_meta>,
      model::timeout_clock::time_point,
      model::opt_abort_source_t,
      allow_materialization_failure) override {
        throw std::logic_error("not implemented");
    }

    size_t materialize_max_bytes() const override { return 0; }

    void cache_put(
      const model::topic_id_partition&, const model::record_batch&) override {}

    std::optional<model::record_batch>
    cache_get(const model::topic_id_partition&, model::offset) override {
        return std::nullopt;
    }

    void cache_put_ordered(
      const model::topic_id_partition&,
      chunked_vector<model::record_batch>) override {}

    ss::future<std::optional<cloud_topics::cluster_epoch>>
    get_current_epoch(ss::abort_source*) override {
        throw std::logic_error("not implemented");
    }

    ss::future<> invalidate_epoch_below(cloud_topics::cluster_epoch) override {
        co_return;
    }

    ss::future<> cache_wait(
      const model::topic_id_partition&,
      model::offset,
      model::offset,
      model::timeout_clock::time_point,
      std::optional<std::reference_wrapper<ss::abort_source>>) override {
        co_return;
    }

    std::unique_ptr<inflight_write_token> track_inflight_write() override {
        return std::make_unique<inflight_write_token>();
    }
};

/// Mock node_source for single-node tests.
class mock_node_source : public epoch_barrier::node_source {
public:
    explicit mock_node_source(model::node_id self)
      : _self(self) {}

    model::node_id self() const override { return _self; }
    std::vector<model::node_id> node_ids() const override { return {_self}; }

private:
    model::node_id _self;
};

/// Mock inflight_write_tracker for barrier tests.
class mock_inflight_write_tracker : public inflight_write_tracker {
public:
    std::atomic<int> drain_count{0};
    std::optional<ss::shared_promise<>> drain_gate;

    ss::future<> start() override { return ss::now(); }
    ss::future<> stop() override { return ss::now(); }

    std::unique_ptr<inflight_write_token> track() override {
        return std::make_unique<inflight_write_token>();
    }

    ss::future<> drain() override {
        ++drain_count;
        if (drain_gate.has_value()) {
            co_await drain_gate->get_shared_future();
        }
    }
};

} // namespace cloud_topics::l0::gc::testing
