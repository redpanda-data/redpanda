// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/partition.h"
#include "config/configuration.h"
#include "container/chunked_vector.h"
#include "kafka/protocol/offset_commit.h"
#include "kafka/server/group.h"
#include "model/fundamental.h"
#include "model/timestamp.h"

#include <seastar/testing/perf_tests.hh>

#include <optional>
#include <utility>
#include <vector>

namespace kafka {

namespace {

/// The coordinator-shard CPU cost of an offset commit, minus raft replication
/// and cross-shard hops:
///
///   1. group::prepare_offset_commits() - record batch serialization plus
///      pending-commit registration
///   2. group::complete_offset_commit() per partition - committed offset
///      upsert and pending-commit cleanup
///   3. offset_commit_response echo construction
///
/// Request shapes mirror production traffic: single-partition commits from
/// per-message committers and wide commits from checkpointing consumers
/// (e.g. Flink jobs committing hundreds of partitions at once).

group make_group() {
    auto& conf = config::shard_local_cfg();
    conf.enable_consumer_group_metrics.set_value(std::vector<ss::sstring>{});
    static ss::sharded<cluster::tx_gateway_frontend> tx_frontend;
    static ss::sharded<features::feature_table> feature_table;
    return group(
      kafka::group_id("bench-group"),
      group_state::empty,
      conf,
      nullptr,
      nullptr,
      model::term_id(),
      tx_frontend,
      feature_table);
}

offset_commit_request
make_request(size_t topics, size_t partitions_per_topic, int64_t offset) {
    offset_commit_request req;
    req.data.group_id = kafka::group_id("bench-group");
    req.data.generation_id = kafka::generation_id(1);
    req.data.topics.reserve(topics);
    for (size_t t = 0; t < topics; ++t) {
        offset_commit_request_topic topic;
        // realistic production-style topic name length
        topic.name = model::topic(
          fmt::format("bench.topic.with.a.realistic.name.length-{}", t));
        topic.partitions.reserve(partitions_per_topic);
        for (size_t p = 0; p < partitions_per_topic; ++p) {
            topic.partitions.push_back(
              offset_commit_request_partition{
                .partition_index = model::partition_id(static_cast<int32_t>(p)),
                .committed_offset = model::offset(offset),
                .committed_leader_epoch = kafka::leader_epoch(5),
              });
        }
        req.data.topics.push_back(std::move(topic));
    }
    return req;
}

constexpr size_t inner_iters = 100;

struct group_bench {
    group g = make_group();
    int64_t committed_offset = 0;
    int64_t log_offset = 0;

    std::vector<offset_commit_request> make_requests(size_t t, size_t p) {
        std::vector<offset_commit_request> reqs;
        reqs.reserve(inner_iters);
        for (size_t i = 0; i < inner_iters; ++i) {
            reqs.push_back(make_request(t, p, ++committed_offset));
        }
        return reqs;
    }

    /// prepare_offset_commits() only: batch serialization + pending map
    /// registration. drains pending afterwards (untimed) to keep state
    /// steady across iterations.
    size_t run_prepare(size_t topics, size_t partitions) {
        auto reqs = make_requests(topics, partitions);
        perf_tests::start_measuring_time();
        for (auto& req : reqs) {
            auto prepared = g.prepare_offset_commits(req);
            perf_tests::do_not_optimize(prepared);
        }
        perf_tests::stop_measuring_time();
        return inner_iters;
    }

    /// the full coordinator-shard cycle: prepare, assign log offsets,
    /// complete each partition, build the response echo. this is
    /// store_offsets() minus the raft replicate.
    size_t run_cycle(size_t topics, size_t partitions) {
        auto reqs = make_requests(topics, partitions);
        perf_tests::start_measuring_time();
        for (auto& req : reqs) {
            auto prepared = g.prepare_offset_commits(req);
            for (auto& e : prepared->commits) {
                e.second.log_offset = model::offset(++log_offset);
                g.complete_offset_commit(e.first, e.second);
            }
            auto resp = offset_commit_response(req, error_code::none);
            perf_tests::do_not_optimize(resp);
        }
        perf_tests::stop_measuring_time();
        return inner_iters;
    }

    /// all-topics offset fetch (topics == null) over a group with tracked
    /// offsets. the require_stable variant seeds pending offset commits
    /// (never completed), the state a backlogged coordinator is in, where
    /// every fetched partition consults the pending set.
    ss::future<size_t> run_fetch_all(
      size_t topics, size_t partitions, bool require_stable, size_t pending) {
        if (!fetch_populated) {
            for (size_t t = 0; t < topics; ++t) {
                model::topic topic(fmt::format("fetch-topic-{}", t));
                for (size_t p = 0; p < partitions; ++p) {
                    g.insert_offset(
                      model::topic_partition(
                        topic, model::partition_id(static_cast<int32_t>(p))),
                      group::offset_metadata{
                        .log_offset = model::offset(++log_offset),
                        .offset = model::offset(++committed_offset),
                        .committed_leader_epoch = kafka::leader_epoch(5),
                      });
                }
            }
            if (pending > 0) {
                // the pending commits are deliberately seeded under a
                // different topic than the fetched offsets, so that every
                // per-partition stability check is a miss: a miss must
                // consider the entire pending set, making it the worst case
                // for the stability probe, while a hit could return after
                // examining only part of it.
                auto prepared = g.prepare_offset_commits(
                  make_request(1, pending, ++committed_offset));
                perf_tests::do_not_optimize(prepared);
            }
            fetch_populated = true;
        }
        std::vector<offset_fetch_request_group> reqs;
        reqs.reserve(inner_iters);
        for (size_t i = 0; i < inner_iters; ++i) {
            reqs.push_back(
              offset_fetch_request_group{
                .group_id = kafka::group_id("bench-group")});
        }
        perf_tests::start_measuring_time();
        for (auto& req : reqs) {
            auto resp = co_await g.handle_offset_fetch(
              std::move(req), require_stable);
            perf_tests::do_not_optimize(resp);
        }
        perf_tests::stop_measuring_time();
        co_return inner_iters;
    }

    bool fetch_populated{false};

    void populate_fetch_offsets(size_t topics, size_t partitions) {
        for (size_t t = 0; t < topics; ++t) {
            model::topic topic(fmt::format("fetch-topic-{}", t));
            for (size_t p = 0; p < partitions; ++p) {
                g.insert_offset(
                  model::topic_partition(
                    topic, model::partition_id(static_cast<int32_t>(p))),
                  group::offset_metadata{
                    .log_offset = model::offset(++log_offset),
                    .offset = model::offset(++committed_offset),
                    .committed_leader_epoch = kafka::leader_epoch(5),
                  });
            }
        }
    }

    /// specific-topics fetch: the shape consumers issue on partition
    /// assignment ("committed offsets for these partitions of this topic")
    ss::future<size_t> run_fetch_specific(size_t partitions) {
        if (!fetch_populated) {
            populate_fetch_offsets(1, partitions);
            fetch_populated = true;
        }
        std::vector<offset_fetch_request_group> reqs;
        reqs.reserve(inner_iters);
        for (size_t i = 0; i < inner_iters; ++i) {
            offset_fetch_request_topics t;
            t.name = model::topic("fetch-topic-0");
            t.partition_indexes.reserve(partitions);
            for (size_t p = 0; p < partitions; ++p) {
                t.partition_indexes.emplace_back(static_cast<int32_t>(p));
            }
            offset_fetch_request_group req{
              .group_id = kafka::group_id("bench-group")};
            req.topics.emplace();
            req.topics->push_back(std::move(t));
            reqs.push_back(std::move(req));
        }
        perf_tests::start_measuring_time();
        for (auto& req : reqs) {
            auto resp = co_await g.handle_offset_fetch(
              std::move(req), false);
            perf_tests::do_not_optimize(resp);
        }
        perf_tests::stop_measuring_time();
        co_return inner_iters;
    }

    /// response echo construction alone (copies the request's topic
    /// partition structure into the response)
    size_t run_response(size_t topics, size_t partitions) {
        auto req = make_request(topics, partitions, ++committed_offset);
        perf_tests::start_measuring_time();
        for (size_t i = 0; i < inner_iters; ++i) {
            auto resp = offset_commit_response(req, error_code::none);
            perf_tests::do_not_optimize(resp);
        }
        perf_tests::stop_measuring_time();
        return inner_iters;
    }
};

} // namespace

PERF_TEST_F(group_bench, prepare_1t_1p) { return run_prepare(1, 1); }
PERF_TEST_F(group_bench, prepare_1t_50p) { return run_prepare(1, 50); }
PERF_TEST_F(group_bench, prepare_4t_64p) { return run_prepare(4, 64); }

PERF_TEST_F(group_bench, cycle_1t_1p) { return run_cycle(1, 1); }
PERF_TEST_F(group_bench, cycle_1t_50p) { return run_cycle(1, 50); }
PERF_TEST_F(group_bench, cycle_4t_64p) { return run_cycle(4, 64); }

PERF_TEST_CN(group_bench, fetch_all_1t_256p) {
    co_return co_await this->run_fetch_all(1, 256, false, 0);
}
PERF_TEST_CN(group_bench, fetch_all_32t_8p) {
    co_return co_await this->run_fetch_all(32, 8, false, 0);
}
PERF_TEST_CN(group_bench, fetch_specific_1t_256p) {
    co_return co_await this->run_fetch_specific(256);
}
PERF_TEST_CN(group_bench, fetch_all_stable_1t_256p) {
    co_return co_await this->run_fetch_all(1, 256, true, 512);
}

PERF_TEST_F(group_bench, response_1t_1p) { return run_response(1, 1); }
PERF_TEST_F(group_bench, response_4t_64p) { return run_response(4, 64); }

} // namespace kafka
