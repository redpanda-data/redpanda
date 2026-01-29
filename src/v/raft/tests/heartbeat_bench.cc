/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "raft/heartbeats.h"
#include "raft/types.h"
#include "random/generators.h"
#include "test_utils/randoms.h"

#include <seastar/core/sstring.hh>
#include <seastar/testing/perf_tests.hh>
#include <seastar/util/log.hh>

#include <vector>

using namespace std::chrono_literals; // NOLINT
namespace {
static ss::logger hblog("hb-perf");
}

struct fixture {
    static raft::reply_result random_status() {
        return random_generators::random_choice(
          std::vector<raft::reply_result>{
            raft::reply_result::success,
            raft::reply_result::failure,
            raft::reply_result::follower_busy,
            raft::reply_result::group_unavailable});
    }

    std::vector<raft::group_id> make_groups(size_t cnt) {
        raft::group_id gr{0};
        std::vector<raft::group_id> ret;
        ret.reserve(cnt);
        for (size_t i = 0; i < cnt; ++i) {
            ret.push_back(gr);
            gr += raft::group_id(random_generators::get_int(1, 200));
        }
        return ret;
    }

    raft::heartbeat_request_v2
    make_new_request(size_t full_heartbeat_count, size_t lw_heartbeat_count) {
        raft::heartbeat_request_v2 req(
          tests::random_named_int<model::node_id>(),
          tests::random_named_int<model::node_id>());
        for (raft::group_id group : make_groups(full_heartbeat_count)) {
            req.add(
              {.group = group,
               .data = {
                 {.source_revision
                  = tests::random_named_int<model::revision_id>(),
                  .target_revision
                  = tests::random_named_int<model::revision_id>(),
                  .commit_index = tests::random_named_int<model::offset>(),
                  .term = tests::random_named_int<model::term_id>(),
                  .prev_log_index = tests::random_named_int<model::offset>(),
                  .prev_log_term = tests::random_named_int<model::term_id>(),
                  .last_visible_index
                  = tests::random_named_int<model::offset>()}}});
        }
        for (raft::group_id group : make_groups(lw_heartbeat_count)) {
            req.add({.group = group});
        }
        return req;
    }

    raft::heartbeat_reply_v2
    make_new_reply(const raft::heartbeat_request_v2& req) {
        raft::heartbeat_reply_v2 reply(req.target(), req.source());
        req.for_each_lw_heartbeat(
          [&reply](raft::group_id id) { reply.add(id, random_status()); });
        for (auto& fhb : req.full_heartbeats()) {
            reply.add(
              fhb.group,
              random_status(),
              raft::heartbeat_reply_data{
                .source_revision = fhb.data.target_revision,
                .target_revision = fhb.data.source_revision,
                .term = fhb.data.term,
                .last_flushed_log_index = fhb.data.commit_index,
                .last_dirty_log_index = fhb.data.prev_log_index,
                .last_term_base_offset = fhb.data.last_visible_index,
              });
        }
        return reply;
    }
    template<typename T>
    ss::future<> test_serde_write(const T& data) {
        iobuf buffer;
        auto data_copy = data.copy();
        perf_tests::start_measuring_time();

        co_await serde::write_async(buffer, std::move(data_copy));

        perf_tests::stop_measuring_time();
        cnt++;
        sz += buffer.size_bytes();
    }

    fixture() {
        new_req_full = make_new_request(10000, 0);
        new_req_lw = make_new_request(0, 10000);
        new_req_mixed = make_new_request(2000, 8000);
        new_reply_full = make_new_reply(new_req_full);
        new_reply_mixed = make_new_reply(new_req_mixed);
        new_reply_lw = make_new_reply(new_req_lw);
    }

    raft::heartbeat_request_v2 new_req;
    raft::heartbeat_request_v2 new_req_full;
    raft::heartbeat_request_v2 new_req_mixed;
    raft::heartbeat_request_v2 new_req_lw;

    raft::heartbeat_reply_v2 new_reply_full;
    raft::heartbeat_reply_v2 new_reply_mixed;
    raft::heartbeat_reply_v2 new_reply_lw;

    size_t cnt = 0;
    size_t sz = 0;

    ~fixture() {
        hblog.info(
          "average serialized size: {} bytes", static_cast<double>(sz) / cnt);
    }
};

PERF_TEST_C(fixture, test_new_hb_request_full) {
    co_await test_serde_write(new_req_full);
}
PERF_TEST_C(fixture, test_new_hb_request_mixed) {
    co_await test_serde_write(new_req_mixed);
}

PERF_TEST_C(fixture, test_new_hb_request_lw) {
    co_await test_serde_write(new_req_lw);
}

PERF_TEST_C(fixture, test_new_hb_reply_full) {
    co_await test_serde_write(new_reply_full);
}

PERF_TEST_C(fixture, test_new_hb_reply_mixed) {
    co_await test_serde_write(new_reply_mixed);
}

PERF_TEST_C(fixture, test_new_hb_reply_lw) {
    co_await test_serde_write(new_reply_lw);
}
