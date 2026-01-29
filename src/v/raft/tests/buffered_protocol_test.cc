/**
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "raft/buffered_protocol.h"
#include "raft/types.h"
#include "random/generators.h"
#include "test_utils/test.h"

#include <seastar/core/sleep.hh>

static ss::logger test_logger("buffered_protocol_test");

using namespace raft;

class test_protocol : public consensus_client_protocol::impl {
public:
    ss::future<bool> ensure_disconnect(model::node_id) final {
        return ss::make_ready_future<bool>(true);
    }
    ss::future<> reset_backoff(model::node_id) final {
        return ss::make_ready_future<>();
    }
    ss::future<result<raft::vote_reply>>
    vote(model::node_id, raft::vote_request, rpc::client_opts) final {
        return ss::make_ready_future<result<raft::vote_reply>>(
          raft::vote_reply{});
    }
    ss::future<result<raft::append_entries_reply>> append_entries(
      model::node_id, raft::append_entries_request, rpc::client_opts) final {
        return ss::make_ready_future<result<raft::append_entries_reply>>(
          raft::append_entries_reply{});
    }
    ss::future<result<raft::heartbeat_reply_v2>> heartbeat_v2(
      model::node_id, raft::heartbeat_request_v2, rpc::client_opts) final {
        return ss::make_ready_future<result<raft::heartbeat_reply_v2>>(
          raft::heartbeat_reply_v2{});
    }
    ss::future<result<raft::install_snapshot_reply>> install_snapshot(
      model::node_id, raft::install_snapshot_request, rpc::client_opts) final {
        return ss::make_ready_future<result<raft::install_snapshot_reply>>(
          raft::install_snapshot_reply{});
    }
    ss::future<result<raft::timeout_now_reply>> timeout_now(
      model::node_id, raft::timeout_now_request, rpc::client_opts) final {
        return ss::make_ready_future<result<raft::timeout_now_reply>>(
          raft::timeout_now_reply{});
    }
    ss::future<result<get_compaction_mcco_reply>> get_compaction_mcco(
      model::node_id, get_compaction_mcco_request, rpc::client_opts) final {
        return ss::make_ready_future<result<raft::get_compaction_mcco_reply>>(
          raft::get_compaction_mcco_reply{});
    }
    ss::future<result<distribute_compaction_mtro_reply>>
    distribute_compaction_mtro(
      model::node_id,
      distribute_compaction_mtro_request,
      rpc::client_opts) final {
        return ss::make_ready_future<
          result<raft::distribute_compaction_mtro_reply>>(
          raft::distribute_compaction_mtro_reply{});
    }
};

class BufferedProtocolFixture : public seastar_test {
public:
    ss::future<> SetUpAsync() override {
        _protocol = std::make_unique<buffered_protocol>(
          ss::default_scheduling_group(),
          make_consensus_client_protocol<test_protocol>(),
          config::mock_binding(
            static_cast<size_t>(std::numeric_limits<ssize_t>::max())),
          config::mock_binding(size_t(0)),
          1ms);
        return ss::now();
    }

    ss::future<> TearDownAsync() override {
        if (_protocol) {
            co_await _protocol->stop();
        }
    }

protected:
    std::unique_ptr<buffered_protocol> _protocol;
};

TEST_F_CORO(BufferedProtocolFixture, TestAppendBufferingWithGC) {
    static constexpr auto num_nodes = 5;
    static constexpr auto num_append_fibers = 100;
    static constexpr auto test_runtime = 10s;

    auto& protocol = *_protocol.get();
    auto stop = false;

    auto make_append_entries_request = []() {
        return raft::append_entries_request{
          {}, {}, {}, 0, flush_after_append::no};
    };

    auto random_node_id = []() {
        return model::node_id(random_generators::get_int(0, num_nodes - 1));
    };

    auto append_until_stopped = [&]() {
        return ss::do_until(
          [&stop] { return stop; },
          [&]() {
              return protocol
                .append_entries(
                  random_node_id(),
                  make_append_entries_request(),
                  rpc::client_opts{1s})
                .then([](auto) {
                    auto sleep_for = std::chrono::milliseconds{
                      random_generators::get_int(0, 1)};
                    return ss::sleep(sleep_for);
                });
          });
    };

    std::vector<ss::future<>> append_futures;
    append_futures.reserve(num_append_fibers);
    for (size_t i = 0; i < num_append_fibers; ++i) {
        append_futures.push_back(append_until_stopped());
    }

    co_await ss::sleep(test_runtime);
    vlog(test_logger.info, "Stopped append fibers, waiting for completion");
    stop = true;
    co_await ss::when_all_succeed(append_futures.begin(), append_futures.end());
}
