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
#include "model/fundamental.h"
#include "model/metadata.h"
#include "raft/group_configuration.h"
#include "raft/tests/raft_group_fixture.h"
#include "raft/types.h"
#include "random/generators.h"
#include "serde/serde.h"
#include "test_utils/randoms.h"

#include <seastar/core/sstring.hh>
#include <seastar/testing/perf_tests.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/log.hh>

using namespace std::chrono_literals; // NOLINT
namespace {
static ss::logger hblog("hb-perf");
}
struct fixture {
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
        raft::heartbeat_request_v2 req;
        req.source_node = old_req.heartbeats.front().node_id.id();
        req.target_node = old_req.heartbeats.front().target_node_id.id();
        auto i = 0;
        for (auto& hb_meta : old_req.heartbeats) {
            raft::hb_request_envelope envelope{.group = hb_meta.meta.group};
            if (i < full_heartbeat_count) {
                envelope.data = raft::heartbeat_request_state{
                  .source_revision = hb_meta.node_id.revision(),
                  .target_revision = hb_meta.target_node_id.revision(),
                  .commit_index = hb_meta.meta.commit_index,
                  .term = hb_meta.meta.term,
                  .prev_log_index = hb_meta.meta.prev_log_index,
                  .prev_log_term = hb_meta.meta.prev_log_term,
                  .last_visible_index = hb_meta.meta.last_visible_index,
                };
            }
            req.group_requests.push_back(envelope);
            ++i;
        }
        return req;
    }

    fixture() {
        old_req = make_request(10000);
        new_req = make_new_request(10000);
    }

    raft::heartbeat_request old_req;
    raft::heartbeat_request_v2 new_req;
    size_t cnt = 0;
    size_t sz = 0;

    ~fixture() {
        hblog.info(
          "average serialized size: {} bytes", static_cast<double>(sz) / cnt);
    }
};

PERF_TEST_C(fixture, test_old_heartbeat_serde) {
    perf_tests::start_measuring_time();

    iobuf buffer;
    co_await serde::write_async(buffer, old_req);

    perf_tests::stop_measuring_time();
    cnt++;
    sz += buffer.size_bytes();
}

PERF_TEST_C(fixture, test_new_heartbeat_serde) {
    iobuf buffer;

    perf_tests::start_measuring_time();

    co_await serde::write_async(buffer, new_req.copy());

    perf_tests::stop_measuring_time();
    cnt++;
    sz += buffer.size_bytes();
}
