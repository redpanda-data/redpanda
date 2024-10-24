
// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "bytes/iobuf_parser.h"
#include "raft/heartbeats.h"
#include "raft/types.h"
#include "random/generators.h"
#include "test_utils/randoms.h"

#include <seastar/core/future.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test_log.hpp>
#include <fmt/ostream.h>
#include <fmt/ranges.h>

#include <utility>
#include <vector>

std::vector<raft::group_id> random_group_list() {
    const auto sz = random_generators::get_int(100, 10000);
    raft::group_id g_id{random_generators::get_int(0, 1000)};
    std::vector<raft::group_id> groups;
    groups.reserve(sz);
    for (auto i = 0; i < sz; ++i) {
        groups.push_back(g_id);
        g_id += raft::group_id(random_generators::get_int(1, 20));
    }
    return groups;
}
raft::heartbeat_request_v2 random_request() {
    raft::heartbeat_request_v2 req(
      tests::random_named_int<model::node_id>(),
      tests::random_named_int<model::node_id>());
    auto groups = random_group_list();
    for (auto& g : groups) {
        raft::group_heartbeat hb{.group = g};
        if (tests::random_bool()) {
            raft::heartbeat_request_data data{
              .commit_index = tests::random_named_int<model::offset>(),
              .term = tests::random_named_int<model::term_id>(),
              .prev_log_index = tests::random_named_int<model::offset>(),
              .prev_log_term = tests::random_named_int<model::term_id>(),
              .last_visible_index = tests::random_named_int<model::offset>(),
            };
            hb.data = data;
        }
        req.add(hb);
    }
    return req;
}

raft::reply_result random_reply_status() {
    return random_generators::random_choice(std::vector<raft::reply_result>{
      raft::reply_result::success,
      raft::reply_result::failure,
      raft::reply_result::timeout,
      raft::reply_result::group_unavailable});
}

raft::heartbeat_reply_v2 random_reply() {
    raft::heartbeat_reply_v2 reply(
      tests::random_named_int<model::node_id>(),
      tests::random_named_int<model::node_id>());
    auto groups = random_group_list();
    for (auto& g : groups) {
        if (tests::random_bool()) {
            reply.add(
              g,
              random_reply_status(),
              raft::heartbeat_reply_data{
                .source_revision
                = tests::random_named_int<model::revision_id>(),
                .target_revision
                = tests::random_named_int<model::revision_id>(),
                .term = tests::random_named_int<model::term_id>(),
                .last_flushed_log_index
                = tests::random_named_int<model::offset>(),
                .last_dirty_log_index
                = tests::random_named_int<model::offset>(),
                .last_term_base_offset
                = tests::random_named_int<model::offset>(),
                .may_recover = tests::random_bool(),
              });
        } else {
            reply.add(g, random_reply_status());
        }
    }
    return reply;
}

template<typename T>
void serde_rt(const T& value) {
    iobuf target;
    serde::write_async(target, value.copy()).get();

    iobuf_parser parser(std::move(target));

    auto from_serde_async = serde::read_async<T>(parser).get();

    BOOST_REQUIRE(value == from_serde_async);
}

SEASTAR_THREAD_TEST_CASE(heartbeat_request_v2_rt) {
    serde_rt(random_request());
}

SEASTAR_THREAD_TEST_CASE(heartbeat_reply_v2_rt) { serde_rt(random_reply()); }

SEASTAR_THREAD_TEST_CASE(heartbeat_copy) {
    auto request = random_request();
    auto repl = random_reply();

    BOOST_REQUIRE(request == request.copy());
    BOOST_REQUIRE(repl == repl.copy());
}

SEASTAR_THREAD_TEST_CASE(heartbeat_reply_for_each) {
    raft::heartbeat_reply_v2 reply(
      tests::random_named_int<model::node_id>(),
      tests::random_named_int<model::node_id>());

    auto groups = random_group_list();
    std::vector<std::pair<raft::group_id, raft::reply_result>> expected;
    for (auto& g : groups) {
        if (tests::random_bool()) {
            reply.add(
              g,
              random_reply_status(),
              raft::heartbeat_reply_data{
                .source_revision
                = tests::random_named_int<model::revision_id>(),
                .target_revision
                = tests::random_named_int<model::revision_id>(),
                .term = tests::random_named_int<model::term_id>(),
                .last_flushed_log_index
                = tests::random_named_int<model::offset>(),
                .last_dirty_log_index
                = tests::random_named_int<model::offset>(),
                .last_term_base_offset
                = tests::random_named_int<model::offset>(),
                .may_recover = tests::random_bool(),
              });
        } else {
            auto status = random_reply_status();
            expected.emplace_back(g, status);
            reply.add(g, status);
        }
    }

    std::vector<std::pair<raft::group_id, raft::reply_result>> actual;
    reply.for_each_lw_reply(
      [&actual](raft::group_id g, raft::reply_result result) {
          actual.emplace_back(g, result);
      });
    BOOST_REQUIRE_EQUAL(expected.size(), actual.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        BOOST_REQUIRE_EQUAL(expected[i].first, actual[i].first);
        BOOST_REQUIRE_EQUAL(int(expected[i].second), (int)actual[i].second);
    }
    BOOST_REQUIRE(expected == actual);
}

SEASTAR_THREAD_TEST_CASE(heartbeat_request_for_each) {
    raft::heartbeat_request_v2 req(
      tests::random_named_int<model::node_id>(),
      tests::random_named_int<model::node_id>());

    auto groups = random_group_list();
    std::vector<raft::group_id> expected;

    for (auto& g : groups) {
        raft::group_heartbeat hb{.group = g};
        if (tests::random_bool()) {
            raft::heartbeat_request_data data{
              .commit_index = tests::random_named_int<model::offset>(),
              .term = tests::random_named_int<model::term_id>(),
              .prev_log_index = tests::random_named_int<model::offset>(),
              .prev_log_term = tests::random_named_int<model::term_id>(),
              .last_visible_index = tests::random_named_int<model::offset>(),
            };
            hb.data = data;
        }
        if (!hb.data) {
            expected.push_back(g);
        }
        req.add(hb);
    }

    std::vector<raft::group_id> actual;
    req.for_each_lw_heartbeat(
      [&actual](raft::group_id g) { actual.push_back(g); });

    BOOST_REQUIRE_EQUAL(expected.size(), actual.size());
    BOOST_REQUIRE(expected == actual);
}
