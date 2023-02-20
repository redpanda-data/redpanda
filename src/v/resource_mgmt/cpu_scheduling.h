/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "seastarx.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/scheduling.hh>

// manage cpu scheduling groups. scheduling groups are global, so one instance
// of this class can be created at the top level and passed down into any server
// and any shard that needs to schedule continuations into a given group.
class scheduling_groups final {
public:
    ss::future<> create_groups() {
        _admin = co_await ss::create_scheduling_group("admin", 100);
        _raft = co_await ss::create_scheduling_group("raft", 1000);
        _kafka = co_await ss::create_scheduling_group("kafka", 1000);
        _cluster = co_await ss::create_scheduling_group("cluster", 300);
        _coproc = co_await ss::create_scheduling_group("coproc", 100);
        _cache_background_reclaim = co_await ss::create_scheduling_group(
          "cache_background_reclaim", 200);
        _compaction = co_await ss::create_scheduling_group(
          "log_compaction", 100);
        _raft_learner_recovery = co_await ss::create_scheduling_group(
          "raft_learner_recovery", 50);
        _archival_upload = co_await ss::create_scheduling_group(
          "archival_upload", 100);
        _node_status = co_await ss::create_scheduling_group("node_status", 50);
        _self_test = co_await ss::create_scheduling_group("self_test", 100);
    }

    ss::future<> destroy_groups() {
        co_await destroy_scheduling_group(_admin);
        co_await destroy_scheduling_group(_raft);
        co_await destroy_scheduling_group(_kafka);
        co_await destroy_scheduling_group(_cluster);
        co_await destroy_scheduling_group(_coproc);
        co_await destroy_scheduling_group(_cache_background_reclaim);
        co_await destroy_scheduling_group(_compaction);
        co_await destroy_scheduling_group(_raft_learner_recovery);
        co_await destroy_scheduling_group(_archival_upload);
        co_await destroy_scheduling_group(_node_status);
        co_await destroy_scheduling_group(_self_test);
        co_return;
    }

    ss::scheduling_group admin_sg() { return _admin; }
    ss::scheduling_group raft_sg() { return _raft; }
    ss::scheduling_group kafka_sg() { return _kafka; }
    ss::scheduling_group cluster_sg() { return _cluster; }
    ss::scheduling_group coproc_sg() { return _coproc; }
    ss::scheduling_group cache_background_reclaim_sg() {
        return _cache_background_reclaim;
    }
    ss::scheduling_group compaction_sg() { return _compaction; }
    ss::scheduling_group raft_learner_recovery_sg() {
        return _raft_learner_recovery;
    }
    ss::scheduling_group archival_upload() { return _archival_upload; }
    ss::scheduling_group node_status() { return _node_status; }
    ss::scheduling_group self_test_sg() { return _self_test; }

    std::vector<std::reference_wrapper<const ss::scheduling_group>>
    all_scheduling_groups() const {
        return {
          std::cref(_default),
          std::cref(_admin),
          std::cref(_raft),
          std::cref(_kafka),
          std::cref(_cluster),
          std::cref(_coproc),
          std::cref(_cache_background_reclaim),
          std::cref(_compaction),
          std::cref(_raft_learner_recovery),
          std::cref(_archival_upload),
          std::cref(_node_status),
          std::cref(_self_test)};
    }

private:
    ss::scheduling_group _default{
      seastar::default_scheduling_group()}; // created and managed by seastar
    ss::scheduling_group _admin;
    ss::scheduling_group _raft;
    ss::scheduling_group _kafka;
    ss::scheduling_group _cluster;
    ss::scheduling_group _coproc;
    ss::scheduling_group _cache_background_reclaim;
    ss::scheduling_group _compaction;
    ss::scheduling_group _raft_learner_recovery;
    ss::scheduling_group _archival_upload;
    ss::scheduling_group _node_status;
    ss::scheduling_group _self_test;
};
