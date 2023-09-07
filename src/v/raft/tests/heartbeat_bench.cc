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
#include "serde/serde.h"
#include "test_utils/randoms.h"

#include <seastar/core/sstring.hh>
#include <seastar/testing/perf_tests.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/log.hh>

#include <vector>

using namespace std::chrono_literals; // NOLINT
namespace {
static ss::logger hblog("hb-perf");
}

struct fixture {
    static raft::reply_result random_status() {
        return random_generators::random_choice(std::vector<raft::reply_result>{
          raft::reply_result::success,
          raft::reply_result::failure,
          raft::reply_result::timeout,
          raft::reply_result::group_unavailable});
    }

    std::vector<raft::group_id> make_groups(size_t cnt) {
        raft::group_id gr{0};
        std::vector<raft::group_id> ret;
        ret.reserve(cnt);
        for (auto i = 0; i < cnt; ++i) {
            ret.push_back(gr);
            gr += raft::group_id(random_generators::get_int(1, 200));
        }
        return ret;
    }

    raft::heartbeat_request make_request(size_t full_beats_cnt) {
        raft::heartbeat_request req;
        req.heartbeats.reserve(full_beats_cnt);
        auto groups = make_groups(full_beats_cnt);
        auto source = tests::random_named_int<model::node_id>();
        auto target = tests::random_named_int<model::node_id>();
        for (auto gr : groups) {
            raft::protocol_metadata meta{
              .group = gr,
              .commit_index = tests::random_named_int<model::offset>(),
              .term = tests::random_named_int<model::term_id>(),
              .prev_log_index = tests::random_named_int<model::offset>(),
              .prev_log_term = tests::random_named_int<model::term_id>(),
              .last_visible_index = tests::random_named_int<model::offset>(),
            };
            meta.dirty_offset = meta.prev_log_index;
            req.heartbeats.push_back(raft::heartbeat_metadata{
              .meta = meta,
              .node_id = raft::vnode(
                source, tests::random_named_int<model::revision_id>()),
              .target_node_id = raft::vnode(
                target, tests::random_named_int<model::revision_id>()),
            });
        }

        return req;
    }

    raft::heartbeat_request_v2 make_new_request(size_t full_heartbeat_count) {
        raft::heartbeat_request_v2 req(
          old_req.heartbeats.front().node_id.id(),
          old_req.heartbeats.front().target_node_id.id());

        auto i = 0;
        for (auto& hb_meta : old_req.heartbeats) {
            raft::group_heartbeat group_beat{.group = hb_meta.meta.group};
            if (i < full_heartbeat_count) {
                group_beat.data = raft::heartbeat_request_data{
                  .source_revision = hb_meta.node_id.revision(),
                  .target_revision = hb_meta.target_node_id.revision(),
                  .commit_index = hb_meta.meta.commit_index,
                  .term = hb_meta.meta.term,
                  .prev_log_index = hb_meta.meta.prev_log_index,
                  .prev_log_term = hb_meta.meta.prev_log_term,
                  .last_visible_index = hb_meta.meta.last_visible_index,
                };
            }
            req.add(group_beat);
            ++i;
        }
        return req;
    }

    raft::heartbeat_reply make_reply() {
        raft::heartbeat_reply reply;
        reply.meta.reserve(old_req.heartbeats.size());
        for (auto& hb : old_req.heartbeats) {
            reply.meta.push_back(raft::append_entries_reply{
              .target_node_id = hb.node_id,
              .node_id = hb.target_node_id,
              .group = hb.meta.group,
              .term = hb.meta.term,
              .last_flushed_log_index = hb.meta.prev_log_index,
              .last_dirty_log_index = hb.meta.prev_log_index,
              .last_term_base_offset = hb.meta.prev_log_index,
              .result = raft::reply_result::success,
            });
        }
        return reply;
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
        old_req = make_request(10000);
        new_req_full = make_new_request(10000);
        new_req_lw = make_new_request(0);
        new_req_mixed = make_new_request(2000);
        old_reply = make_reply();
        new_reply_full = make_new_reply(new_req_full);
        new_reply_mixed = make_new_reply(new_req_mixed);
        new_reply_lw = make_new_reply(new_req_lw);
    }

    raft::heartbeat_request old_req;
    raft::heartbeat_request_v2 new_req;
    raft::heartbeat_request_v2 new_req_full;
    raft::heartbeat_request_v2 new_req_mixed;
    raft::heartbeat_request_v2 new_req_lw;

    raft::heartbeat_reply old_reply;
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

PERF_TEST_C(fixture, test_old_hb_request) {
    perf_tests::start_measuring_time();

    iobuf buffer;
    co_await serde::write_async(buffer, old_req);

    perf_tests::stop_measuring_time();
    cnt++;
    sz += buffer.size_bytes();
}

PERF_TEST_C(fixture, test_new_hb_request_full) {
    co_await test_serde_write(new_req_full);
}
PERF_TEST_C(fixture, test_new_hb_request_mixed) {
    co_await test_serde_write(new_req_mixed);
}

PERF_TEST_C(fixture, test_new_hb_request_lw) {
    co_await test_serde_write(new_req_lw);
}

PERF_TEST_C(fixture, test_old_hb_reply) {
    perf_tests::start_measuring_time();

    iobuf buffer;
    co_await serde::write_async(buffer, old_reply);

    perf_tests::stop_measuring_time();
    cnt++;
    sz += buffer.size_bytes();
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
