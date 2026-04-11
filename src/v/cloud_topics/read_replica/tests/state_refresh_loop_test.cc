/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_io/remote.h"
#include "cloud_io/tests/s3_imposter.h"
#include "cloud_io/tests/scoped_remote.h"
#include "cloud_topics/level_one/metastore/domain_uuid.h"
#include "cloud_topics/level_one/metastore/manifest_io.h"
#include "cloud_topics/level_one/metastore/metastore_manifest.h"
#include "cloud_topics/logger.h"
#include "cloud_topics/read_replica/snapshot_manager.h"
#include "cloud_topics/read_replica/state_refresh_loop.h"
#include "cloud_topics/read_replica/stm.h"
#include "cloud_topics/read_replica/tests/db_utils.h"
#include "lsm/lsm.h"
#include "raft/tests/raft_fixture.h"
#include "test_utils/async.h"
#include "test_utils/scoped_config.h"
#include "test_utils/test.h"
#include "test_utils/tmp_dir.h"

#include <seastar/core/coroutine.hh>

#include <gmock/gmock.h>

using namespace cloud_topics;
using namespace cloud_topics::read_replica;
using namespace std::chrono_literals;

namespace {

using o = kafka::offset;

auto state_matches(
  const l1::domain_uuid& expected_domain,
  std::optional<lsm::sequence_number> expected_seqno,
  kafka::offset expected_start_offset,
  kafka::offset expected_next_offset,
  model::term_id expected_term) {
    using testing::AllOf;
    using testing::Eq;
    using testing::Field;
    using testing::Optional;

    return AllOf(
      Field("domain", &state::domain, Eq(expected_domain)),
      Field("seqno", &state::seqno, Eq(expected_seqno)),
      Field("start_offset", &state::start_offset, Eq(expected_start_offset)),
      Field("next_offset", &state::next_offset, Eq(expected_next_offset)),
      Field("latest_term", &state::latest_term, Eq(expected_term)));
}

class StateRefreshLoopTest
  : public raft::stm_raft_fixture<stm>
  , public s3_imposter_fixture {
public:
    StateRefreshLoopTest()
      : reader_staging_base_dir_("state_refresh_loop_test") {}

    struct refresh_node {
        ss::shared_ptr<stm> stm_ptr;
        cloud_io::remote* remote;
        const cloud_storage_clients::bucket_name& bucket;
        std::filesystem::path staging_path;
        ss::abort_source as;
        std::unique_ptr<snapshot_manager> snapshot_mgr;
        std::unique_ptr<state_refresh_loop> refresh_loop;

        refresh_node(
          ss::shared_ptr<stm> stm,
          cloud_io::remote* r,
          const cloud_storage_clients::bucket_name& b,
          std::filesystem::path staging)
          : stm_ptr(std::move(stm))
          , remote(r)
          , bucket(b)
          , staging_path(std::move(staging)) {
            snapshot_mgr = std::make_unique<snapshot_manager>(
              staging_path, remote, nullptr);
        }

        void start_refresh_loop(
          model::term_id term,
          const model::topic_id_partition& tidp,
          const cloud_storage::remote_label& remote_label) {
            refresh_loop = std::make_unique<state_refresh_loop>(
              term,
              tidp,
              stm_ptr,
              snapshot_mgr.get(),
              bucket,
              remote_label,
              *remote);
            refresh_loop->start();
        }

        ss::future<> stop_refresh_loop() {
            if (refresh_loop) {
                co_await refresh_loop->stop_and_wait();
                refresh_loop.reset();
            }
        }

        ss::future<> stop_snapshot_mgr() {
            if (snapshot_mgr) {
                co_await snapshot_mgr->stop();
            }
        }
    };

    void SetUp() override {
        set_expectations_and_listen({});

        cfg_.get("election_timeout_ms").set_value(1000ms);
        cfg_.get("raft_heartbeat_interval_ms").set_value(100ms);
        cfg_.get("cloud_storage_readreplica_manifest_sync_timeout_ms")
          .set_value(100ms);

        sr_ = cloud_io::scoped_remote::create(10, conf);

        test_tidp_ = model::topic_id_partition{
          model::topic_id{uuid_t::create()}, model::partition_id{0}};
        test_remote_label_ = cloud_storage::remote_label{
          model::cluster_uuid{uuid_t::create()}};

        writer_staging_dir_ = std::filesystem::path{"state_refresh_loop_test"}
                              / "writer";

        // Call base class SetUpAsync to initialize raft fixture
        raft::raft_fixture::SetUpAsync().get();

        // Create nodes with STMs
        for (auto i = 0; i < 3; ++i) {
            add_node(model::node_id(i), model::revision_id(0));
        }

        int node_idx = 0;
        for (auto& [id, node_ptr] : nodes()) {
            node_ptr->initialise(all_vnodes()).get();
            auto* raft = node_ptr->raft().get();
            raft::state_machine_manager_builder builder;
            auto stm_ptr = builder.create_stm<stm>(cd_log, raft);
            node_ptr->start(std::move(builder)).get();

            // Create refresh node with staging directory
            auto node_staging_path = reader_staging_base_dir_.get_path()
                                     / fmt::format("node_{}", node_idx++);
            std::filesystem::create_directories(node_staging_path);
            refresh_nodes_[id] = std::make_unique<refresh_node>(
              stm_ptr, &sr_->remote.local(), bucket_name, node_staging_path);
        }
    }

    stm_shptrs_t create_stms(
      raft::state_machine_manager_builder& builder,
      raft::raft_node_instance& node) override {
        return builder.create_stm<stm>(cd_log, node.raft().get());
    }

    void TearDown() override {
        for (auto& [id, node_ptr] : refresh_nodes_) {
            node_ptr->stop_refresh_loop().get();
            node_ptr->stop_snapshot_mgr().get();
        }

        for (auto& [domain, db_ptr] : writer_dbs_) {
            db_ptr->close().get();
        }
        writer_dbs_.clear();
        raft::stm_raft_fixture<stm>::TearDownAsync().get();
        refresh_nodes_.clear();
        sr_.reset();
    }

    refresh_node* wait_get_leader() {
        wait_for_leader(raft::default_timeout()).get();
        for (auto& [id, node_ptr] : nodes()) {
            if (node_ptr && node_ptr->raft()->is_leader()) {
                auto it = refresh_nodes_.find(id);
                if (it != refresh_nodes_.end()) {
                    return it->second.get();
                }
            }
        }
        return nullptr;
    }

    ss::future<> upload_metastore_manifest(
      const cloud_storage::remote_label& remote_label,
      l1::metastore_manifest manifest) {
        l1::manifest_io io{sr_->remote.local(), bucket_name};
        auto result = co_await io.upload_metastore_manifest(
          remote_label, std::move(manifest));
        if (!result.has_value()) {
            throw std::runtime_error(
              fmt::format("Failed to upload manifest: {}", result.error()));
        }
    }

    ss::future<l1::domain_uuid> upload_single_domain_manifest() {
        auto domain = l1::domain_uuid(uuid_t::create());
        l1::metastore_manifest manifest;
        manifest.partitioning_strategy = "murmur";
        manifest.domains.push_back(domain);
        co_await upload_metastore_manifest(
          test_remote_label_, std::move(manifest));
        co_return domain;
    }

    ss::future<lsm::sequence_number> write_metastore_data(
      l1::domain_uuid domain,
      const model::topic_id_partition& tidp,
      o base_offset,
      o last_offset,
      model::term_id term) {
        auto* db = co_await test_utils::get_or_create_writer_db(
          domain,
          writer_staging_dir_,
          &sr_->remote.local(),
          bucket_name,
          writer_dbs_);
        co_return co_await test_utils::write_metastore_data(
          db, tidp, base_offset, last_offset, term);
    }

    ss::future<> wait_for_stm_domain(
      ss::shared_ptr<stm> stm, const l1::domain_uuid& expected_domain) {
        RPTEST_REQUIRE_EVENTUALLY_CORO(10s, [&] {
            return stm->has_domain() && stm->domain() == expected_domain;
        });
    }

    ss::future<> wait_for_stm_state_update(
      ss::shared_ptr<stm> stm, lsm::sequence_number min_seqno) {
        RPTEST_REQUIRE_EVENTUALLY_CORO(
          10s, [&] { return stm->get_state().seqno >= min_seqno; });
    }

protected:
    temporary_dir reader_staging_base_dir_;
    absl::node_hash_map<model::node_id, std::unique_ptr<refresh_node>>
      refresh_nodes_;
    scoped_config cfg_;
    std::unique_ptr<cloud_io::scoped_remote> sr_;
    chunked_hash_map<l1::domain_uuid, std::unique_ptr<lsm::database>>
      writer_dbs_;
    std::filesystem::path writer_staging_dir_;
    model::topic_id_partition test_tidp_;
    cloud_storage::remote_label test_remote_label_;
};

} // namespace

TEST_F(StateRefreshLoopTest, DiscoverDomain) {
    auto* leader_node = wait_get_leader();
    auto leader_stm = leader_node->stm_ptr;

    // Verify STM has no domain initially.
    ASSERT_FALSE(leader_stm->has_domain());

    auto leader_term = leader_stm->raft()->term();
    leader_node->start_refresh_loop(
      leader_term, test_tidp_, test_remote_label_);

    // Sleep briefly to let loop attempt discovery.
    ss::sleep(500ms).get();
    ASSERT_FALSE(leader_stm->has_domain());

    // Create and upload metastore manifest and wait for the refresh loop to
    // discover the domain.
    auto domain = upload_single_domain_manifest().get();

    RPTEST_REQUIRE_EVENTUALLY(10s, [&] {
        return leader_stm->has_domain() && leader_stm->domain() == domain;
    });

    // Before writing anything to object storage the refresh loop should just
    // replicated the domain and nothing else.
    ASSERT_THAT(
      leader_stm->get_state(),
      state_matches(domain, std::nullopt, o{0}, o{0}, model::term_id{0}));

    auto seqno = write_metastore_data(
                   domain, test_tidp_, o{0}, o{9}, model::term_id{1})
                   .get();
    wait_for_stm_state_update(leader_stm, seqno).get();
    ASSERT_THAT(
      leader_stm->get_state(),
      state_matches(domain, seqno, o{0}, o{10}, model::term_id{1}));
}

TEST_F(StateRefreshLoopTest, PeriodicRefresh) {
    auto* leader_node = wait_get_leader();
    auto leader_stm = leader_node->stm_ptr;
    auto domain = upload_single_domain_manifest().get();

    // Write a few times and ensure that the state refresh loop catches up.
    auto initial_seqno = write_metastore_data(
                           domain, test_tidp_, o{0}, o{4}, model::term_id{1})
                           .get();

    leader_node->start_refresh_loop(
      leader_stm->raft()->term(), test_tidp_, test_remote_label_);
    wait_for_stm_state_update(leader_stm, initial_seqno).get();
    ASSERT_THAT(
      leader_stm->get_state(),
      state_matches(domain, initial_seqno, o{0}, o{5}, model::term_id{1}));

    // Another update.
    auto second_seqno = write_metastore_data(
                          domain, test_tidp_, o{5}, o{11}, model::term_id{1})
                          .get();
    wait_for_stm_state_update(leader_stm, second_seqno).get();
    ASSERT_THAT(
      leader_stm->get_state(),
      state_matches(domain, second_seqno, o{0}, o{12}, model::term_id{1}));

    // Another update.
    auto third_seqno = write_metastore_data(
                         domain, test_tidp_, o{12}, o{14}, model::term_id{2})
                         .get();
    wait_for_stm_state_update(leader_stm, third_seqno).get();
    ASSERT_THAT(
      leader_stm->get_state(),
      state_matches(domain, third_seqno, o{0}, o{15}, model::term_id{2}));

    // Wait for all nodes to catch up.
    for (auto& [id, node_ptr] : refresh_nodes_) {
        wait_for_stm_state_update(node_ptr->stm_ptr, third_seqno).get();
        ASSERT_THAT(
          node_ptr->stm_ptr->get_state(),
          state_matches(domain, third_seqno, o{0}, o{15}, model::term_id{2}));
    }
}

TEST_F(StateRefreshLoopTest, OnlyOnTerm) {
    auto* leader_node = wait_get_leader();
    auto leader_stm = leader_node->stm_ptr;
    auto domain = upload_single_domain_manifest().get();

    leader_node->start_refresh_loop(
      leader_stm->raft()->term(), test_tidp_, test_remote_label_);
    RPTEST_REQUIRE_EVENTUALLY(10s, [&] {
        return leader_stm->has_domain() && leader_stm->domain() == domain;
    });

    auto seqno = write_metastore_data(
                   domain, test_tidp_, o{0}, o{9}, model::term_id{1})
                   .get();
    wait_for_stm_state_update(leader_stm, seqno).get();
    ASSERT_THAT(
      leader_stm->get_state(),
      state_matches(domain, seqno, o{0}, o{10}, model::term_id{1}));

    // Step down and wait a bit -- our refreshes should stop because the term
    // has changed.
    leader_stm->raft()->step_down("test").get();
    auto second_seqno = write_metastore_data(
                          domain, test_tidp_, o{10}, o{11}, model::term_id{1})
                          .get();
    ss::sleep(500ms).get();
    ASSERT_THAT(
      leader_stm->get_state(),
      state_matches(domain, seqno, o{0}, o{10}, model::term_id{1}));

    // Restart the refresh loop in the new term on the new leader.
    leader_node->stop_refresh_loop().get();
    leader_node = wait_get_leader();
    leader_stm = leader_node->stm_ptr;
    leader_node->start_refresh_loop(
      leader_stm->raft()->term(), test_tidp_, test_remote_label_);
    leader_stm = leader_node->stm_ptr;
    wait_for_stm_state_update(leader_stm, second_seqno).get();
    ASSERT_THAT(
      leader_stm->get_state(),
      state_matches(domain, second_seqno, o{0}, o{12}, model::term_id{1}));
}
