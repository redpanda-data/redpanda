// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cloud_io/remote.h"
#include "cloud_storage/remote.h"
#include "cloud_storage/types.h"
#include "cloud_storage_clients/client_pool.h"
#include "cluster/archival/archival_metadata_stm.h"
#include "model/record.h"
#include "raft/tests/raft_fixture.h"
#include "test_utils/scoped_config.h"
#include "test_utils/test.h"
#include "utils/available_promise.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/sleep.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/later.hh>

using cloud_storage::segment_name;
using segment_meta = cloud_storage::partition_manifest::segment_meta;

namespace {
ss::logger fixture_logger{"archival_stm_fixture"};

constexpr const char* httpd_host_name = "localhost";
constexpr uint16_t httpd_port_number = 4442;

cloud_storage_clients::s3_configuration get_configuration() {
    net::unresolved_address server_addr(httpd_host_name, httpd_port_number);
    cloud_storage_clients::s3_configuration conf;
    conf.uri = cloud_storage_clients::access_point_uri(httpd_host_name);
    conf.access_key = cloud_roles::public_key_str("access-key");
    conf.secret_key = cloud_roles::private_key_str("secret-key");
    conf.region = cloud_roles::aws_region_name("us-east-1");
    conf.url_style = cloud_storage_clients::s3_url_style::virtual_host;
    conf.server_addr = server_addr;
    conf.disable_metrics = net::metrics_disabled::yes;
    conf.disable_public_metrics = net::public_metrics_disabled::yes;
    conf._probe = ss::make_shared<cloud_storage_clients::client_probe>(
      net::metrics_disabled::yes,
      net::public_metrics_disabled::yes,
      cloud_roles::aws_region_name{"us-east-1"},
      cloud_storage_clients::endpoint_url{httpd_host_name});
    return conf;
}

constexpr model::cloud_credentials_source config_file{
  model::cloud_credentials_source::config_file};
} // namespace

struct archival_stm_node {
    archival_stm_node() = default;

    ss::shared_ptr<cluster::archival_metadata_stm> archival_stm;
    ss::sharded<cloud_storage_clients::client_pool> client_pool;
    ss::sharded<cloud_io::remote> cloud_io;
    ss::sharded<cloud_storage::remote> remote;
};

class archival_metadata_stm_gtest_fixture : public raft::raft_fixture {
public:
    static constexpr auto node_count = 3;

    seastar::future<> TearDownAsync() override {
        co_await seastar::coroutine::parallel_for_each(
          _archival_stm_nodes, [](archival_stm_node& node) {
              return node.remote.stop()
                .then([&node]() { return node.cloud_io.stop(); })
                .then([&node]() { return node.client_pool.stop(); });
          });

        co_await raft::raft_fixture::TearDownAsync();
    }

    ss::future<> start() {
        for (auto i = 0; i < node_count; ++i) {
            add_node(model::node_id(i), model::revision_id(0));
        }

        for (auto& [id, node] : nodes()) {
            auto& stm_node = _archival_stm_nodes.at(id());

            co_await stm_node.client_pool.start(
              10, ss::sharded_parameter([]() { return get_configuration(); }));
            co_await stm_node.cloud_io.start(
              std::ref(stm_node.client_pool),
              ss::sharded_parameter([]() { return get_configuration(); }),
              ss::sharded_parameter([] { return config_file; }));

            co_await stm_node.remote.start(
              std::ref(stm_node.cloud_io),
              ss::sharded_parameter([] { return get_configuration(); }));

            co_await node->initialise(all_vnodes());

            raft::state_machine_manager_builder builder;
            auto stm = builder.create_stm<cluster::archival_metadata_stm>(
              node->raft().get(),
              stm_node.remote.local(),
              node->get_feature_table().local(),
              fixture_logger,
              std::nullopt,
              std::nullopt);

            stm_node.archival_stm = std::move(stm);

            vlog(fixture_logger.info, "Starting node {}", id);

            co_await node->start(std::move(builder));
        }
    }

    cluster::archival_metadata_stm& get_leader_stm() {
        const auto leader = get_leader();
        if (!leader) {
            throw std::runtime_error{"No leader"};
        }

        auto ptr = _archival_stm_nodes.at(*leader).archival_stm;
        if (!ptr) {
            throw std::runtime_error{
              ssx::sformat("Achival stm for node {} not initialised", *leader)};
        }

        return *ptr;
    }

    ss::future<> wait_for_apply() {
        auto committed_offset = co_await with_leader(
          10s, [](auto& node) { return node.raft()->committed_offset(); });

        co_await parallel_for_each_node([committed_offset](auto& node) {
            return node.raft()->stm_manager()->wait(
              committed_offset, model::no_timeout);
        });
        co_return;
    }

private:
    std::array<archival_stm_node, node_count> _archival_stm_nodes;
};

TEST_F_CORO(archival_metadata_stm_gtest_fixture, test_archival_stm_happy_path) {
    ss::abort_source never_abort;

    co_await start();

    std::vector<cloud_storage::segment_meta> m;
    m.push_back(segment_meta{
      .base_offset = model::offset(0),
      .committed_offset = model::offset(99),
      .archiver_term = model::term_id(1),
      .segment_term = model::term_id(1)});

    co_await wait_for_leader(10s);

    ASSERT_EQ_CORO(
      get_leader_stm().get_dirty(),
      cluster::archival_metadata_stm::state_dirty::dirty);

    co_await get_leader_stm().add_segments(
      m,
      std::nullopt,
      model::producer_id{},
      ss::lowres_clock::now() + 10s,
      never_abort,
      cluster::segment_validated::yes);

    ASSERT_EQ_CORO(get_leader_stm().manifest().size(), 1);
    ASSERT_EQ_CORO(
      get_leader_stm().manifest().begin()->base_offset, model::offset(0));
    ASSERT_EQ_CORO(
      get_leader_stm().manifest().begin()->committed_offset, model::offset(99));

    ASSERT_EQ_CORO(
      get_leader_stm().get_dirty(),
      cluster::archival_metadata_stm::state_dirty::dirty);

    co_await get_leader_stm().mark_clean(
      ss::lowres_clock::now() + 10s,
      get_leader_stm().get_insync_offset(),
      never_abort);

    ASSERT_EQ_CORO(
      get_leader_stm().get_dirty(),
      cluster::archival_metadata_stm::state_dirty::clean);

    co_await wait_for_apply();
}

TEST_F_CORO(
  archival_metadata_stm_gtest_fixture,
  test_same_term_sync_pending_replication) {
    /*
     * Test that archival_metadata_stm::sync is able to sync
     * within the same term and that it will wait for on-going
     * replication futures to complete before doing so. To simulate
     * this scenario we introduce a small delay on append entries
     * response processing.
     */

    ss::abort_source never_abort;

    std::vector<cloud_storage::segment_meta> m;
    m.push_back(segment_meta{
      .base_offset = model::offset(0),
      .committed_offset = model::offset(99),
      .archiver_term = model::term_id(1),
      .segment_term = model::term_id(1)});

    co_await start();

    auto res = co_await with_leader(
      10s, [this, &m, &never_abort](raft::raft_node_instance&) {
          return get_leader_stm().add_segments(
            m,
            std::nullopt,
            model::producer_id{},
            ss::lowres_clock::now() + 10s,
            never_abort,
            cluster::segment_validated::yes);
      });

    ASSERT_TRUE_CORO(!res);

    // Allocated on the heap as the lambda will outlive the scope of the test.
    auto may_resume_append = ss::make_shared<ss::shared_promise<>>();
    auto reached_dispatch_append = ss::make_shared<available_promise<bool>>();

    // Ensure that we always resume the append operation to allow the test to
    // exit in case of failure.
    auto deferred_fix_nodes = ss::defer([may_resume_append] {
        if (!may_resume_append->available()) {
            may_resume_append->set_value();
        }
    });

    auto plagued_node = co_await with_leader(
      10s,
      [&reached_dispatch_append,
       &may_resume_append](raft::raft_node_instance& node) {
          node.on_dispatch([reached_dispatch_append, may_resume_append](
                             model::node_id, raft::msg_type t) {
              if (t == raft::msg_type::append_entries) {
                  if (!reached_dispatch_append->available()) {
                      reached_dispatch_append->set_value(true);
                  }
                  return may_resume_append->get_shared_future();
              }

              return ss::now();
          });

          return node.get_vnode();
      });

    m.clear();
    m.push_back(segment_meta{
      .base_offset = model::offset(100),
      .committed_offset = model::offset(199),
      .archiver_term = model::term_id(2),
      .segment_term = model::term_id(1)});

    auto slow_replication_fut = with_leader(
      10s,
      [this, &m, &never_abort, &plagued_node](raft::raft_node_instance& node) {
          if (node.get_vnode() != plagued_node) {
              throw std::runtime_error{"Leadership moved"};
          }

          return get_leader_stm().add_segments(
            m,
            std::nullopt,
            model::producer_id{},
            ss::lowres_clock::now() + 10s,
            never_abort,
            cluster::segment_validated::yes);
      });

    co_await reached_dispatch_append->get_future();

    // Expecting this to fail as we have the replication blocked.
    auto sync_result_before_replication = co_await with_leader(
      10s, [this, &plagued_node](raft::raft_node_instance& node) mutable {
          if (node.get_vnode() != plagued_node) {
              throw std::runtime_error{"Leadership moved"};
          }
          return get_leader_stm().sync(10ms);
      });
    ASSERT_FALSE_CORO(sync_result_before_replication);

    // Subsequent calls to sync should fail too.
    auto second_sync_result_before_replication = co_await with_leader(
      10s, [this, &plagued_node](raft::raft_node_instance& node) mutable {
          if (node.get_vnode() != plagued_node) {
              throw std::runtime_error{"Leadership moved"};
          }
          return get_leader_stm().sync(10ms);
      });
    ASSERT_FALSE_CORO(second_sync_result_before_replication);

    // Allow replication to progress.
    may_resume_append->set_value();

    // This sync will succeed and will wait for replication to progress.
    auto synced = co_await with_leader(
      10s, [this, &plagued_node](raft::raft_node_instance& node) mutable {
          if (node.get_vnode() != plagued_node) {
              throw std::runtime_error{"Leadership moved"};
          }
          return get_leader_stm().sync(10s);
      });

    ASSERT_TRUE_CORO(synced);

    auto slow_replication_res = co_await std::move(slow_replication_fut);
    ASSERT_TRUE_CORO(!slow_replication_res);

    auto [committed_offset, term] = co_await with_leader(
      10s, [](raft::raft_node_instance& node) mutable {
          return std::make_tuple(
            node.raft()->committed_offset(), node.raft()->term());
      });

    ASSERT_EQ_CORO(committed_offset, model::offset{2});
    ASSERT_EQ_CORO(term, model::term_id{1});

    co_await wait_for_apply();
}

TEST_F_CORO(
  archival_metadata_stm_gtest_fixture, test_same_term_sync_abort_source) {
    /*
     * Test that archival_metadata_stm::sync is able to sync
     * within the same term and that it will wait for on-going
     * replication futures to complete before doing so. To simulate
     * this scenario we introduce a small delay on append entries
     * response processing.
     *
     * Like the test above, but this time we break out of the replicate call
     * by using an abort source.
     */

    ss::abort_source never_abort;

    std::vector<cloud_storage::segment_meta> m;
    m.push_back(segment_meta{
      .base_offset = model::offset(0),
      .committed_offset = model::offset(99),
      .archiver_term = model::term_id(1),
      .segment_term = model::term_id(1)});

    co_await start();

    auto res = co_await with_leader(
      10s, [this, &m, &never_abort](raft::raft_node_instance&) {
          return get_leader_stm().add_segments(
            m,
            std::nullopt,
            model::producer_id{},
            ss::lowres_clock::now() + 10s,
            never_abort,
            cluster::segment_validated::yes);
      });

    ASSERT_TRUE_CORO(!res);

    // Allocated on the heap as the lambda will outlive the scope of the test.
    auto may_resume_append = ss::make_shared<ss::shared_promise<>>();
    auto reached_dispatch_append = ss::make_shared<available_promise<bool>>();

    // Ensure that we always resume the append operation to allow the test to
    // exit in case of failure.
    auto deferred_fix_nodes = ss::defer([may_resume_append] {
        if (!may_resume_append->available()) {
            may_resume_append->set_value();
        }
    });

    auto plagued_node = co_await with_leader(
      10s,
      [&reached_dispatch_append,
       &may_resume_append](raft::raft_node_instance& node) {
          node.on_dispatch([reached_dispatch_append, may_resume_append](
                             model::node_id, raft::msg_type t) {
              if (t == raft::msg_type::append_entries) {
                  if (!reached_dispatch_append->available()) {
                      reached_dispatch_append->set_value(true);
                  }
                  return may_resume_append->get_shared_future();
              }

              return ss::now();
          });

          return node.get_vnode();
      });

    m.clear();
    m.push_back(segment_meta{
      .base_offset = model::offset(100),
      .committed_offset = model::offset(199),
      .archiver_term = model::term_id(2),
      .segment_term = model::term_id(1)});

    ss::abort_source replication_abort_source;
    auto slow_replication_fut = with_leader(
      10s,
      [this, &m, &replication_abort_source, &plagued_node](
        raft::raft_node_instance& node) {
          if (node.get_vnode() != plagued_node) {
              throw std::runtime_error{"Leadership moved"};
          }

          return get_leader_stm().add_segments(
            m,
            std::nullopt,
            model::producer_id{},
            ss::lowres_clock::now() + 10s,
            replication_abort_source,
            cluster::segment_validated::yes);
      });

    co_await reached_dispatch_append->get_future();
    replication_abort_source.request_abort();

    // Expecting this to fail as we have the replication blocked.
    auto sync_result_before_replication = co_await with_leader(
      10s, [this, &plagued_node](raft::raft_node_instance& node) mutable {
          if (node.get_vnode() != plagued_node) {
              throw std::runtime_error{"Leadership moved"};
          }
          return get_leader_stm().sync(10ms);
      });
    ASSERT_FALSE_CORO(sync_result_before_replication);

    // Subsequent calls to sync should fail too.
    auto second_sync_result_before_replication = co_await with_leader(
      10s, [this, &plagued_node](raft::raft_node_instance& node) mutable {
          if (node.get_vnode() != plagued_node) {
              throw std::runtime_error{"Leadership moved"};
          }
          return get_leader_stm().sync(10ms);
      });
    ASSERT_FALSE_CORO(second_sync_result_before_replication);

    // Allow replication to progress.
    may_resume_append->set_value();

    // This sync will succeed and will wait for replication to progress.
    auto synced = co_await with_leader(
      10s, [this, &plagued_node](raft::raft_node_instance& node) mutable {
          if (node.get_vnode() != plagued_node) {
              throw std::runtime_error{"Leadership moved"};
          }
          return get_leader_stm().sync(10s);
      });

    ASSERT_TRUE_CORO(synced);

    auto slow_replication_res = co_await std::move(slow_replication_fut);
    ASSERT_TRUE_CORO(!slow_replication_res);

    auto [committed_offset, term] = co_await with_leader(
      10s, [](raft::raft_node_instance& node) mutable {
          return std::make_tuple(
            node.raft()->committed_offset(), node.raft()->term());
      });

    ASSERT_EQ_CORO(committed_offset, model::offset{2});
    ASSERT_EQ_CORO(term, model::term_id{1});

    co_await wait_for_apply();
}

TEST_F_CORO(
  archival_metadata_stm_gtest_fixture, test_archival_stm_error_propagation) {
    ss::abort_source never_abort;

    auto s_cfg = scoped_config{};
    s_cfg.get("cloud_storage_disable_metadata_consistency_checks")
      .set_value(false);
    co_await start();

    std::vector<cloud_storage::segment_meta> good_segment;
    good_segment.push_back(segment_meta{
      .base_offset = model::offset(0),
      .committed_offset = model::offset(99),
      .archiver_term = model::term_id(1),
      .segment_term = model::term_id(1)});

    co_await wait_for_leader(10s);
    auto timeout = 30s;
    auto deadline = ss::lowres_clock::now() + timeout;

    ASSERT_EQ_CORO(
      get_leader_stm().get_dirty(),
      cluster::archival_metadata_stm::state_dirty::dirty);

    auto is_synced = co_await get_leader_stm().sync(timeout);

    ASSERT_TRUE_CORO(is_synced);

    auto repl_err = co_await get_leader_stm()
                      .batch_start(deadline, never_abort)
                      .add_segments(
                        good_segment, cluster::segment_validated::yes)
                      .replicate();

    ASSERT_EQ_CORO(repl_err, cluster::errc::success);

    ASSERT_EQ_CORO(get_leader_stm().manifest().size(), 1);
    ASSERT_EQ_CORO(
      get_leader_stm().manifest().begin()->base_offset, model::offset(0));
    ASSERT_EQ_CORO(
      get_leader_stm().manifest().begin()->committed_offset, model::offset(99));

    ASSERT_EQ_CORO(
      get_leader_stm().get_dirty(),
      cluster::archival_metadata_stm::state_dirty::dirty);

    repl_err = co_await get_leader_stm()
                 .batch_start(deadline, never_abort)
                 .mark_clean(get_leader_stm().manifest().get_insync_offset())
                 .replicate();
    ASSERT_EQ_CORO(repl_err, cluster::errc::success);

    ASSERT_EQ_CORO(
      get_leader_stm().get_dirty(),
      cluster::archival_metadata_stm::state_dirty::clean);

    // Attempt to replicate incorrect record batch
    std::vector<cloud_storage::segment_meta> poisoned_segment;
    poisoned_segment.push_back(segment_meta{
      .base_offset = model::offset(101),
      .committed_offset = model::offset(999),
      .archiver_term = model::term_id(1),
      .segment_term = model::term_id(1)});

    repl_err = co_await get_leader_stm()
                 .batch_start(deadline, never_abort)
                 .add_segments(
                   std::move(poisoned_segment), cluster::segment_validated::yes)
                 .replicate();
    ASSERT_EQ_CORO(repl_err, cluster::errc::inconsistent_stm_update);

    // Check that it still works with consistent updates
    good_segment.clear();
    good_segment.push_back(segment_meta{
      .base_offset = model::offset(100),
      .committed_offset = model::offset(999),
      .archiver_term = model::term_id(1),
      .segment_term = model::term_id(1)});

    repl_err = co_await get_leader_stm()
                 .batch_start(deadline, never_abort)
                 .add_segments(good_segment, cluster::segment_validated::yes)
                 .replicate();
    ASSERT_EQ_CORO(repl_err, cluster::errc::success);

    co_await wait_for_apply();
}

TEST_F_CORO(
  archival_metadata_stm_gtest_fixture, test_archival_stm_read_write_fence) {
    ss::abort_source never_abort;
    auto timeout = 30s;
    auto deadline = ss::lowres_clock::now() + timeout;

    auto s_cfg = scoped_config{};
    s_cfg.get("cloud_storage_disable_metadata_consistency_checks")
      .set_value(false);
    co_await start();

    std::vector<cloud_storage::segment_meta> good_segment;
    good_segment.push_back(segment_meta{
      .base_offset = model::offset(0),
      .committed_offset = model::offset(99),
      .archiver_term = model::term_id(1),
      .segment_term = model::term_id(1)});

    co_await wait_for_leader(10s);

    ASSERT_EQ_CORO(
      get_leader_stm().get_dirty(),
      cluster::archival_metadata_stm::state_dirty::dirty);

    auto is_synced = co_await get_leader_stm().sync(timeout);

    ASSERT_TRUE_CORO(is_synced);

    auto applied_offset = get_leader_stm().manifest().get_applied_offset();

    auto repl_err = co_await get_leader_stm()
                      .batch_start(deadline, never_abort)
                      .read_write_fence(applied_offset)
                      .add_segments(
                        good_segment, cluster::segment_validated::yes)
                      .replicate();
    ASSERT_EQ_CORO(repl_err, cluster::errc::success);

    ASSERT_EQ_CORO(get_leader_stm().manifest().size(), 1);
    ASSERT_EQ_CORO(
      get_leader_stm().manifest().begin()->base_offset, model::offset(0));
    ASSERT_EQ_CORO(
      get_leader_stm().manifest().begin()->committed_offset, model::offset(99));

    // It's guaranteed that the batch is already applied to the STM
    applied_offset = get_leader_stm().manifest().get_applied_offset();
    ASSERT_TRUE_CORO(applied_offset > model::offset(0));

    good_segment.clear();
    good_segment.push_back(segment_meta{
      .base_offset = model::offset(100),
      .committed_offset = model::offset(199),
      .archiver_term = model::term_id(1),
      .segment_term = model::term_id(1)});

    repl_err = co_await get_leader_stm()
                 .batch_start(deadline, never_abort)
                 .read_write_fence(applied_offset)
                 .add_segments(good_segment, cluster::segment_validated::yes)
                 .replicate();
    ASSERT_EQ_CORO(repl_err, cluster::errc::success);

    // Emulate concurrency violation
    applied_offset = model::offset{0};
    good_segment.clear();
    good_segment.push_back(segment_meta{
      .base_offset = model::offset(200),
      .committed_offset = model::offset(299),
      .archiver_term = model::term_id(1),
      .segment_term = model::term_id(1)});

    repl_err = co_await get_leader_stm()
                 .batch_start(deadline, never_abort)
                 .read_write_fence(applied_offset)
                 .add_segments(good_segment, cluster::segment_validated::yes)
                 .replicate();
    ASSERT_EQ_CORO(repl_err, cluster::errc::concurrent_modification_error);

    co_await wait_for_apply();
}
