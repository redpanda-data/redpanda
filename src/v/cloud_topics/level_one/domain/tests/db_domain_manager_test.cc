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
#include "cloud_topics/level_one/common/remote_io.h"
#include "cloud_topics/level_one/common/object_id.h"
#include "cloud_topics/level_one/common/object_utils.h"
#include "cloud_topics/level_one/domain/db_domain_manager.h"
#include "cloud_topics/level_one/metastore/lsm/state_reader.h"
#include "cloud_topics/level_one/metastore/lsm/state_update.h"
#include "cloud_topics/level_one/metastore/lsm/stm.h"
#include "cloud_topics/level_one/metastore/rpc_types.h"
#include "cloud_topics/level_one/metastore/state_update.h"
#include "config/node_config.h"
#include "lsm/io/cloud_persistence.h"
#include "lsm/io/persistence.h"
#include "model/fundamental.h"
#include "raft/tests/raft_fixture.h"
#include "test_utils/async.h"
#include "test_utils/scoped_config.h"
#include "test_utils/tmp_dir.h"

#include <seastar/util/defer.hh>

#include <gtest/gtest.h>

#include <functional>
#include <list>

using namespace cloud_topics::l1;
using namespace std::chrono_literals;

namespace l1_rpc = cloud_topics::l1::rpc;

namespace {

ss::logger dm_test_log("db_domain_manager_test");

ss::future<> random_sleep_ms(int max_ms) {
    co_await ss::sleep(random_generators::get_int(max_ms) * 1ms);
}

// Per-node wrapper struct that manages db_domain_manager instances.
struct domain_manager_node {
    domain_manager_node(
      ss::shared_ptr<stm> s,
      cloud_io::remote* remote,
      const cloud_storage_clients::bucket_name& bucket,
      const ss::sstring& staging_path)
      : stm_ptr(std::move(s))
      , remote(remote)
      , bucket(bucket)
      , staging_directory(staging_path.data())
      , object_io(
          staging_directory.get_path(),
          remote,
          bucket,
          /*cache=*/nullptr) {}

    // Open a new db_domain_manager for the current term. Previous managers are
    // retained in the list too, to validate that their usage fails.
    db_domain_manager* open_manager(bool start_gc) {
        auto mgr = std::make_unique<db_domain_manager>(
          stm_ptr->raft()->confirmed_term(),
          stm_ptr,
          staging_directory.get_path(),
          remote,
          bucket,
          &object_io);
        if (start_gc) {
            mgr->start();
        }
        auto* ptr = mgr.get();
        managers.push_back(std::move(mgr));
        return ptr;
    }

    // Stop all managers, ignoring errors during teardown.
    ss::future<> stop_managers() {
        for (auto& mgr : managers) {
            try {
                co_await mgr->stop_and_wait();
            } catch (...) {
                // Ignore errors during teardown.
            }
        }
    }

    ss::shared_ptr<stm> stm_ptr;
    cloud_io::remote* remote;
    const cloud_storage_clients::bucket_name& bucket;
    temporary_dir staging_directory;
    remote_io object_io;
    std::list<std::unique_ptr<db_domain_manager>> managers;
};

model::topic_id_partition
make_tp(model::partition_id pid = model::partition_id(0)) {
    return model::topic_id_partition{
      model::topic_id{uuid_t::create()},
      pid,
    };
}

new_object make_new_object(
  const model::topic_id_partition& tp,
  kafka::offset base_offset,
  kafka::offset last_offset) {
    new_object obj;
    obj.oid = create_object_id();
    obj.footer_pos = 100;
    obj.object_size = 1024;

    new_object::metadata meta;
    meta.base_offset = base_offset;
    meta.last_offset = last_offset;
    meta.max_timestamp = model::timestamp::now();
    meta.filepos = 0;
    meta.len = 512;

    chunked_hash_map<model::partition_id, new_object::metadata> partition_map;
    partition_map.emplace(tp.partition, meta);

    obj.extent_metas.emplace(tp.topic_id, std::move(partition_map));
    return obj;
}

chunked_vector<new_object> make_new_objects(
  const model::topic_id_partition& tp,
  kafka::offset start_offset,
  size_t count,
  size_t offsets_per_object) {
    chunked_vector<new_object> objects;
    objects.reserve(count);

    auto next_offset = start_offset;
    for (size_t i = 0; i < count; ++i) {
        auto base = next_offset;
        auto last = kafka::offset(next_offset() + offsets_per_object - 1);
        objects.push_back(make_new_object(tp, base, last));
        next_offset = kafka::next_offset(last);
    }
    return objects;
}

term_state_update_t make_terms(
  const model::topic_id_partition& tp,
  kafka::offset start_offset,
  model::term_id term) {
    term_state_update_t terms;
    terms[tp].emplace_back(
      term_start{
        .term_id = term,
        .start_offset = start_offset,
      });
    return terms;
}

// Creates a manifest in cloud storage under the given domain UUID prefix.
void flush_as_manifest(
  cloud_io::remote* remote,
  const cloud_storage_clients::bucket_name& bucket,
  domain_uuid uuid,
  uint64_t db_epoch,
  chunked_vector<new_object> new_objects,
  term_state_update_t new_terms) {
    auto domain_prefix = cloud_storage_clients::object_key{
      fmt::format("{}", uuid)};
    temporary_dir tmp("test");
    auto cloud_db = lsm::database::open(
                      {.database_epoch = db_epoch},
                      lsm::io::persistence{
                        .data = lsm::io::open_cloud_data_persistence(
                                  tmp.get_path(), remote, bucket, domain_prefix)
                                  .get(),
                        .metadata = lsm::io::open_cloud_metadata_persistence(
                                      remote, bucket, domain_prefix)
                                      .get()})
                      .get();

    // Build the rows for the given new objects.
    add_objects_db_update update{
      .new_objects = std::move(new_objects),
      .new_terms = std::move(new_terms),
    };
    auto snap = cloud_db.create_snapshot();
    state_reader reader(std::move(snap));
    chunked_vector<write_batch_row> rows;
    auto build_res = update.build_rows(reader, rows).get();
    ASSERT_TRUE(build_res.has_value()) << build_res.error();

    // Write them to the database and flush to make it recoverable.
    auto wb = cloud_db.create_write_batch();
    for (auto& r : rows) {
        wb.put(r.key, std::move(r.value), lsm::sequence_number{1});
    }
    cloud_db.apply(std::move(wb)).get();

    cloud_db.flush(ssx::instant::infinite_future()).get();
    cloud_db.close().get();
}

} // namespace

struct test_params {
    bool with_flush_loop{false};
};

class DbDomainManagerTest
  : public raft::raft_fixture
  , public s3_imposter_fixture {
public:
    static constexpr auto num_nodes = 3;
    using opt_ref = std::optional<std::reference_wrapper<domain_manager_node>>;

    virtual test_params params() const {
        return {
          .with_flush_loop = false,
        };
    }

    void SetUp() override {
        ss::smp::invoke_on_all([] {
            config::node().node_id.set_value(model::node_id{1});
        }).get();
        cfg.get("raft_heartbeat_interval_ms").set_value(50ms);
        cfg.get("raft_heartbeat_timeout_ms").set_value(500ms);

        set_expectations_and_listen({});
        sr = cloud_io::scoped_remote::create(10, conf);

        raft::raft_fixture::SetUpAsync().get();

        // Create our STMs.
        for (auto i = 0; i < num_nodes; ++i) {
            add_node(model::node_id(i), model::revision_id(0));
        }
        for (auto& [id, node] : nodes()) {
            node->initialise(all_vnodes()).get();
            auto* raft = node->raft().get();
            raft::state_machine_manager_builder builder;
            auto s = builder.create_stm<stm>(
              dm_test_log,
              raft,
              config::mock_binding<std::chrono::seconds>(1s));

            node->start(std::move(builder)).get();

            // Create staging directory for this node.
            auto staging_path = fmt::format("db_domain_manager_test_{}", id());
            dm_nodes.at(id()) = std::make_unique<domain_manager_node>(
              std::move(s), &sr->remote.local(), bucket_name, staging_path);
        }
        opt_ref leader;
        ASSERT_NO_FATAL_FAILURE(wait_for_leader(leader).get());
        initial_leader = &leader->get();

        initial_manager = initial_leader->open_manager(false);
    }

    void TearDown() override {
        for (auto& node : dm_nodes) {
            if (node) {
                try {
                    node->stop_managers().get();
                } catch (...) {
                    // Ignore errors during teardown.
                }
            }
        }
        raft::raft_fixture::TearDownAsync().get();
        sr.reset();
    }

    // Returns the node of the current leader.
    opt_ref leader_node() {
        auto leader_id = get_leader();
        if (!leader_id.has_value()) {
            return std::nullopt;
        }
        auto& node = *dm_nodes.at(leader_id.value()());
        if (!node.stm_ptr->raft()->is_leader()) {
            return std::nullopt;
        }
        return node;
    }

    // Waits for a leader to be elected, and returns it.
    ss::future<> wait_for_leader(opt_ref& leader) {
        RPTEST_REQUIRE_EVENTUALLY_CORO(10s, [&] {
            leader = leader_node();
            return leader.has_value();
        });
    }

    // Repeatedly add objects to every domain manager on the node.
    //
    // The expectation is that only the domain manager for the current term
    // will actually succeed, but we send to every domain manager to exercise
    // behavior of racing leadership transfers and adds.
    ss::future<> adder_loop(
      domain_manager_node& node,
      const model::topic_id_partition& tp,
      kafka::offset& expected_next,
      bool& done) {
        while (!done) {
            std::vector<ss::future<l1_rpc::add_objects_reply>> futs;
            std::vector<db_domain_manager*> managers;
            managers.reserve(node.managers.size());
            for (auto& mgr : node.managers) {
                managers.emplace_back(mgr.get());
            }
            for (auto* mgr : managers) {
                l1_rpc::add_objects_request req;
                req.new_objects = make_new_objects(tp, expected_next, 1, 1);
                req.new_terms = make_terms(
                  tp, expected_next, model::term_id(1));
                futs.emplace_back(mgr->add_objects(std::move(req)));

                co_await ss::maybe_yield();
            }
            auto reps = co_await ss::when_all_succeed(std::move(futs));
            for (const auto& rep : reps) {
                if (rep.ec == l1_rpc::errc::ok) {
                    auto corr_it = rep.corrected_next_offsets.find(tp);
                    if (corr_it != rep.corrected_next_offsets.end()) {
                        // Update expected_next based on the corrections to
                        // signal to all fibers that the metastore has accepted
                        // this offset and we can move on.
                        expected_next = std::max(
                          expected_next, corr_it->second);
                    }
                }
            }
            co_await random_sleep_ms(10);
        }
    }

    ss::future<> extent_validator_loop(
      domain_manager_node& node,
      const model::topic_id_partition& tp,
      bool& done) {
        std::vector<ss::future<l1_rpc::get_extent_metadata_reply>> futs;
        std::vector<db_domain_manager*> managers;
        managers.reserve(node.managers.size());
        for (auto& mgr : node.managers) {
            managers.emplace_back(mgr.get());
        }
        for (auto* mgr : managers) {
            auto extents_reply
              = mgr
                  ->get_extent_metadata(
                    {.tp = tp,
                     .min_offset = kafka::offset(0),
                     .max_offset = kafka::offset::max(),
                     .o = l1_rpc::get_extent_metadata_request::order::forwards,
                     .max_num_extents = std::numeric_limits<size_t>::max()})
                  .get();
            co_await ss::maybe_yield();
        }
        // Validate the extents are exactly contiguous.
        auto reps = co_await ss::when_all_succeed(std::move(futs));
        for (const auto& rep : reps) {
            if (rep.ec == l1_rpc::errc::ok) {
                kafka::offset expected_next{0};
                for (const auto& e : rep.extents) {
                    EXPECT_EQ(e.base_offset, expected_next);
                    expected_next = kafka::next_offset(e.last_offset);
                }
            }
        }
        co_await random_sleep_ms(10);
    }

    // Repeatedly try to replace a random single offset (expecting the adder
    // loop adds extents of size 1) on every domain manager on the node.
    ss::future<> replacer_loop(
      domain_manager_node& node,
      const model::topic_id_partition& tp,
      kafka::offset& expected_next,
      bool& done) {
        while (!done) {
            // Pick an offset in the latest few offsets. To exercise edge
            // cases, include replacement of next (which is an offset that
            // doesn't exist).
            auto max_replaced_offset = expected_next();
            auto min_replaced_offset = std::max(
              max_replaced_offset - 5, static_cast<int64_t>(0));
            auto offset_to_replace = kafka::offset(
              random_generators::get_int<int64_t>(
                min_replaced_offset, max_replaced_offset));

            std::vector<ss::future<l1_rpc::replace_objects_reply>> futs;
            std::vector<db_domain_manager*> managers;
            managers.reserve(node.managers.size());
            for (auto& mgr : node.managers) {
                managers.emplace_back(mgr.get());
            }
            for (auto* mgr : managers) {
                l1_rpc::replace_objects_request req{
                  .metastore_partition = model::partition_id(0),
                  .new_objects = make_new_objects(tp, offset_to_replace, 1, 1),
                };
                futs.emplace_back(mgr->replace_objects(std::move(req)));

                co_await ss::maybe_yield();
            }
            co_await ss::when_all_succeed(std::move(futs));
            co_await random_sleep_ms(10);
        }
    }

    ss::future<> flusher_loop(domain_manager_node& node, bool& done) {
        while (!done) {
            std::vector<db_domain_manager*> managers;
            std::vector<ss::future<l1_rpc::flush_domain_reply>> futs;
            managers.reserve(node.managers.size());
            for (auto& mgr : node.managers) {
                managers.emplace_back(mgr.get());
            }
            for (auto* mgr : managers) {
                l1_rpc::flush_domain_request req{
                  .metastore_partition = model::partition_id(0),
                };
                futs.emplace_back(mgr->flush_domain(req));
                co_await ss::maybe_yield();
            }
            co_await ss::when_all_succeed(std::move(futs));
            co_await random_sleep_ms(100);
        }
    }

    using exact_next = ss::bool_class<struct exact_next_tag>;
    void validate_metadata(
      const model::topic_id_partition& tp,
      kafka::offset start,
      kafka::offset next,
      exact_next exact = exact_next::yes,
      std::optional<size_t> expected_extents = std::nullopt) {
        opt_ref leader_opt;
        ASSERT_NO_FATAL_FAILURE(wait_for_leader(leader_opt).get());
        auto& leader_node = leader_opt->get();
        auto& mgr = leader_node.managers.back();

        // First check the offsets metadata matches our expectations.
        auto offsets_reply = mgr->get_offsets({.tp = tp}).get();
        EXPECT_EQ(offsets_reply.ec, l1_rpc::errc::ok);
        EXPECT_EQ(offsets_reply.start_offset, start);
        if (exact) {
            EXPECT_EQ(offsets_reply.next_offset, next);
        } else {
            EXPECT_GE(offsets_reply.next_offset, next);
        }

        // Then check the extents to see that they are exactly contiguous.
        auto extents_reply
          = mgr
              ->get_extent_metadata(
                {.tp = tp,
                 .min_offset = kafka::offset(0),
                 .max_offset = kafka::offset::max(),
                 .o = l1_rpc::get_extent_metadata_request::order::forwards,
                 .max_num_extents = std::numeric_limits<size_t>::max()})
              .get();
        EXPECT_EQ(extents_reply.ec, l1_rpc::errc::ok);
        if (expected_extents) {
            EXPECT_EQ(extents_reply.extents.size(), *expected_extents);
        } else {
            EXPECT_GT(extents_reply.extents.size(), 0);
        }
        kafka::offset expected_next{start};
        for (const auto& e : extents_reply.extents) {
            EXPECT_EQ(e.base_offset, expected_next);
            expected_next = kafka::next_offset(e.last_offset);
        }
    }
    ss::future<bool> object_exists(object_id oid) {
        ss::abort_source as;
        retry_chain_node rtc(as, 10s, 100ms);
        auto path = object_path_factory::level_one_path(oid);
        auto result = co_await sr->remote.local().object_exists(
          bucket_name, path, rtc, "l1_object");
        switch (result) {
        case cloud_io::download_result::success:
            co_return true;
        case cloud_io::download_result::notfound:
            co_return false;
        case cloud_io::download_result::timedout:
        case cloud_io::download_result::failed:
            EXPECT_FALSE(true) << "Unexpected error checking object";
        }
        co_return false;
    }

    ss::future<bool>
    all_objects_missing(const chunked_vector<object_id>& object_ids) {
        for (const auto& oid : object_ids) {
            if (co_await object_exists(oid)) {
                co_return false;
            }
        }
        co_return true;
    }

    std::array<std::unique_ptr<domain_manager_node>, num_nodes> dm_nodes;
    scoped_config cfg;
    std::unique_ptr<cloud_io::scoped_remote> sr;

    // Initial leader and manager on that leader.
    domain_manager_node* initial_leader{nullptr};
    db_domain_manager* initial_manager{nullptr};
};

class DbDomainManagerTestWithParams
  : public DbDomainManagerTest
  , public ::testing::WithParamInterface<test_params> {
public:
    test_params params() const override { return GetParam(); }
};

TEST_P(DbDomainManagerTestWithParams, TestConcurrentUpdates) {
    auto args = params();
    auto tp = make_tp();
    bool done = false;
    std::vector<ss::future<>> futs;
    kafka::offset expected_add_next{0};
    // Add several adder, replacer, and validator fibers for each node so
    // domain managers are hit concurrently.
    for (const auto& node : dm_nodes) {
        for (int i = 0; i < 5; ++i) {
            futs.emplace_back(adder_loop(*node, tp, expected_add_next, done));
            futs.emplace_back(extent_validator_loop(*node, tp, done));
            futs.emplace_back(
              replacer_loop(*node, tp, expected_add_next, done));
            if (args.with_flush_loop) {
                futs.emplace_back(flusher_loop(*node, done));
            }
        }
    }
    for (int i = 0; i < 10; ++i) {
        opt_ref leader_opt;
        ASSERT_NO_FATAL_FAILURE(wait_for_leader(leader_opt).get());
        auto raft = leader_opt->get().stm_ptr->raft();
        auto start_term = raft->confirmed_term();

        // Allow for some progress in the current term.
        auto starting_next = expected_add_next;
        while (starting_next == expected_add_next && raft->is_leader()
               && raft->term() == start_term) {
            random_sleep_ms(10).get();
        }

        // Step down and create a domain manager for the new leader.
        leader_opt->get().stm_ptr->raft()->step_down("test stepdown").get();
        ASSERT_NO_FATAL_FAILURE(wait_for_leader(leader_opt).get());
        auto& leader_node = leader_opt->get();
        leader_node.open_manager(/*start_gc=*/true);
    }
    done = true;
    ss::when_all_succeed(std::move(futs)).get();
    EXPECT_FALSE(HasFailure());

    // NOTE: expected_add_next may not necessarily be the next offset --
    // adder_loop isn't very strict with its accounting.
    validate_metadata(tp, kafka::offset(0), expected_add_next, exact_next::no);
}

TEST_P(DbDomainManagerTestWithParams, TestUpdatesWithDroppedAppends) {
    auto args = params();
    auto tp = make_tp();
    bool done = false;
    std::vector<ss::future<>> futs;
    kafka::offset expected_add_next{0};
    // Add several adder, replacer, and validator fibers for each node so
    // domain managers are hit concurrently.
    for (const auto& node : dm_nodes) {
        for (int i = 0; i < 5; ++i) {
            futs.emplace_back(adder_loop(*node, tp, expected_add_next, done));
            futs.emplace_back(extent_validator_loop(*node, tp, done));
            futs.emplace_back(
              replacer_loop(*node, tp, expected_add_next, done));
            if (args.with_flush_loop) {
                futs.emplace_back(flusher_loop(*node, done));
            }
        }
    }
    for (int i = 0; i < 3; ++i) {
        opt_ref leader_opt;
        ASSERT_NO_FATAL_FAILURE(wait_for_leader(leader_opt).get());
        auto raft = leader_opt->get().stm_ptr->raft();
        auto start_term = raft->confirmed_term();
        auto starting_next = expected_add_next;
        auto leader_id = leader_opt->get().stm_ptr->raft()->self();
        auto& leader = node(leader_id.id());

        // Wait for there to make progress before mucking with appends.
        while (starting_next == expected_add_next && raft->is_leader()
               && raft->term() == start_term) {
            random_sleep_ms(10).get();
        }
        leader.on_dispatch([](model::node_id, raft::msg_type mt) {
            // Drop append entries from the leader to followers. This should
            // cause the db_domain_manager to step down because of timeouts to
            // replicate and wait.
            if (mt == raft::msg_type::append_entries) {
                throw std::runtime_error("dropping append entries");
            }
            return ss::now();
        });

        // Wait until the domain manager steps down.
        auto deadline = ss::lowres_clock::now() + 30s;
        while (start_term == leader.raft()->term()) {
            ASSERT_LT(
              ss::lowres_clock::now().time_since_epoch(),
              deadline.time_since_epoch());
            random_sleep_ms(1000).get();
        }
        leader.reset_dispatch_handlers();

        // Open a new domain manager in the new term for the new leader.
        ASSERT_NO_FATAL_FAILURE(wait_for_leader(leader_opt).get());
        auto& leader_node = leader_opt->get();
        leader_node.open_manager(/*start_gc=*/true);
    }
    done = true;
    ss::when_all_succeed(std::move(futs)).get();
    EXPECT_FALSE(HasFailure());

    // NOTE: expected_add_next may not necessarily be the next offset --
    // adder_loop isn't very strict with its accounting.
    validate_metadata(tp, kafka::offset(0), expected_add_next, exact_next::no);
}

INSTANTIATE_TEST_SUITE_P(
  WithFlushLoop,
  DbDomainManagerTestWithParams,
  ::testing::Values(
    test_params{
      .with_flush_loop = false,
    },
    test_params{
      .with_flush_loop = true,
    }));

TEST_F(DbDomainManagerTest, TestBasicAddObjects) {
    auto tp = make_tp();
    // Add [0, 29].
    {
        l1_rpc::add_objects_request req;
        req.new_objects = make_new_objects(tp, kafka::offset(0), 3, 10);
        req.new_terms = make_terms(tp, kafka::offset(0), model::term_id(1));
        auto reply = initial_manager->add_objects(std::move(req)).get();
        ASSERT_EQ(reply.ec, l1_rpc::errc::ok);
    }
    // Add [30, 59].
    {
        l1_rpc::add_objects_request req;
        req.new_objects = make_new_objects(tp, kafka::offset(30), 3, 10);
        req.new_terms = make_terms(tp, kafka::offset(30), model::term_id(1));
        auto reply = initial_manager->add_objects(std::move(req)).get();
        ASSERT_EQ(reply.ec, l1_rpc::errc::ok);
    }

    validate_metadata(tp, kafka::offset(0), kafka::offset(60));
}

TEST_F(DbDomainManagerTest, TestBasicReplaceObjects) {
    auto tp = make_tp();
    // Add [0, 9] in several batches.
    for (int i = 0; i < 10; ++i) {
        l1_rpc::add_objects_request req;
        req.new_objects = make_new_objects(tp, kafka::offset(i), 1, 1);
        req.new_terms = make_terms(tp, kafka::offset(i), model::term_id(1));
        auto reply = initial_manager->add_objects(std::move(req)).get();
        ASSERT_EQ(reply.ec, l1_rpc::errc::ok);
    }
    validate_metadata(
      tp, kafka::offset(0), kafka::offset(10), exact_next::yes, 10);
    // Replace [0, 9] in with one object.
    l1_rpc::replace_objects_request req{
      .metastore_partition = model::partition_id(0),
      .new_objects = make_new_objects(tp, kafka::offset(0), 1, 10),
    };
    auto reply = initial_manager->replace_objects(std::move(req)).get();
    ASSERT_EQ(reply.ec, l1_rpc::errc::ok);

    // Check that the replacement results in 1 extent.
    validate_metadata(
      tp, kafka::offset(0), kafka::offset(10), exact_next::yes, 1);
}

TEST_F(DbDomainManagerTest, TestBasicRestoreDomain) {
    auto tp = make_tp();

    // Create some data in cloud storage under a manually assigned domain UUID.
    auto restore_uuid = domain_uuid(uuid_t::create());
    const uint64_t db_epoch = 100;
    auto new_objects = make_new_objects(tp, kafka::offset(0), 3, 10);
    auto new_terms = make_terms(tp, kafka::offset(0), model::term_id(1));
    flush_as_manifest(
      &sr->remote.local(),
      bucket_name,
      restore_uuid,
      db_epoch,
      std::move(new_objects),
      std::move(new_terms));

    // Call restore_domain with the UUID where data was written.
    l1_rpc::restore_domain_request req{
      .metastore_partition = model::partition_id(0),
      .new_uuid = restore_uuid,
    };
    auto reply = initial_manager->restore_domain(req).get();
    ASSERT_EQ(reply.ec, l1_rpc::errc::ok);

    // Validate the restored metadata matches what we wrote.
    validate_metadata(tp, kafka::offset(0), kafka::offset(30));

    // Sanity check that restoring again is a no-op.
    reply = initial_manager->restore_domain(req).get();
    ASSERT_EQ(reply.ec, l1_rpc::errc::ok);
    validate_metadata(tp, kafka::offset(0), kafka::offset(30));

    // Now try with some other UUID. Note that restoring from a non-existent
    // manifest isn't an issue; it's more that our current database isn't
    // empty.
    req.new_uuid = domain_uuid(uuid_t::create());
    reply = initial_manager->restore_domain(req).get();
    ASSERT_EQ(reply.ec, l1_rpc::errc::concurrent_requests);
    validate_metadata(tp, kafka::offset(0), kafka::offset(30));
}

TEST_F(DbDomainManagerTest, TestRestoreWithConcurrentReads) {
    auto tp = make_tp();

    // Create some data in cloud storage under a manually assigned domain UUID.
    auto restore_uuid = domain_uuid(uuid_t::create());
    const uint64_t db_epoch = 100;
    auto new_objects = make_new_objects(tp, kafka::offset(0), 3, 10);
    auto new_terms = make_terms(tp, kafka::offset(0), model::term_id(1));
    flush_as_manifest(
      &sr->remote.local(),
      bucket_name,
      restore_uuid,
      db_epoch,
      std::move(new_objects),
      std::move(new_terms));

    // Start a bunch of fibers that repeatedly call read.
    bool restore_done{false};
    size_t num_reads{0};
    std::vector<ss::future<>> reader_futs;
    for (int i = 0; i < 10; ++i) {
        // Runs these loops long enough for some reads to actually happen.
        reader_futs.emplace_back(
          ss::do_until(
            [&restore_done, &num_reads] {
                return restore_done && num_reads >= 10;
            },
            [this, &tp, &num_reads] {
                return initial_manager
                  ->get_extent_metadata(
                    {.tp = tp,
                     .min_offset = kafka::offset(0),
                     .max_offset = kafka::offset::max(),
                     .o = l1_rpc::get_extent_metadata_request::order::forwards,
                     .max_num_extents = std::numeric_limits<size_t>::max()})
                  .then([&num_reads](auto reply) {
                      ++num_reads;
                      if (reply.ec == l1_rpc::errc::ok) {
                          auto num_extents = reply.extents.size();
                          // Readers can see either 0 or 3 extents, depending on
                          // when their read arrives.
                          EXPECT_TRUE(num_extents == 0 || num_extents == 3)
                            << "Unexpected number of extents: " << num_extents;
                      }
                  });
            }));
    }

    // Give the readers a chance to start.
    ss::sleep(10ms).get();

    // Restore the domain while readers are active.
    l1_rpc::restore_domain_request req{
      .metastore_partition = model::partition_id(0),
      .new_uuid = restore_uuid,
    };
    auto reply = initial_manager->restore_domain(req).get();
    ASSERT_EQ(reply.ec, l1_rpc::errc::ok);

    // Give readers a chance to run after restore.
    ss::sleep(10ms).get();

    // Stop the readers and wait for them to finish.
    restore_done = true;
    ss::when_all_succeed(reader_futs.begin(), reader_futs.end()).get();

    // Validate the restored metadata.
    validate_metadata(tp, kafka::offset(0), kafka::offset(30));
}

namespace {

chunked_vector<object_id> put_dummy_objects(
  io& object_io, const chunked_vector<new_object>& new_objects) {
    chunked_vector<object_id> object_ids;
    for (auto& obj : new_objects) {
        object_ids.push_back(obj.oid);
        auto file_res = object_io.create_tmp_file().get();
        EXPECT_TRUE(file_res.has_value());
        auto ostream = file_res.value()->output_stream().get();
        ostream.write("test data").get();
        ostream.close().get();
        ss::abort_source as;

        auto put_res
          = object_io.put_object(obj.oid, file_res->get(), &as).get();
        EXPECT_TRUE(put_res.has_value());
        file_res.value()->remove().get();
    }
    return object_ids;
}

} // namespace

TEST_F(DbDomainManagerTest, TestGarbageCollectionAfterRemoveTopic) {
    cfg.get("cloud_topics_long_term_garbage_collection_interval")
      .set_value(100ms);

    auto tp = make_tp();

    // Create some object metadata and actually create dummy objects for them.
    auto new_objects = make_new_objects(tp, kafka::offset(0), 4321, 10);
    auto object_ids = put_dummy_objects(initial_leader->object_io, new_objects);

    // Verify objects exist in S3 using remote->object_exists.
    for (const auto& oid : object_ids) {
        EXPECT_TRUE(object_exists(oid).get());
    }

    // Add objects to the metastore.
    {
        l1_rpc::add_objects_request req;
        req.new_objects = std::move(new_objects);
        req.new_terms = make_terms(tp, kafka::offset(0), model::term_id(1));
        auto reply = initial_manager->add_objects(std::move(req)).get();
        ASSERT_EQ(reply.ec, l1_rpc::errc::ok);
    }

    // Remove the topic, marking the objects as removable.
    {
        l1_rpc::remove_topics_request req;
        req.topics.push_back(tp.topic_id);
        auto reply = initial_manager->remove_topics(std::move(req)).get();
        ASSERT_EQ(reply.ec, l1_rpc::errc::ok);
        ASSERT_TRUE(reply.not_removed.empty());
    }

    // Allow for some time to GC, but no GC should happen until we flush.
    ss::sleep(1s).get();
    for (const auto& oid : object_ids) {
        EXPECT_TRUE(object_exists(oid).get());
    }

    // Now flush and ensure GC happens.
    ASSERT_EQ(initial_manager->flush_domain({}).get().ec, l1_rpc::errc::ok);

    // Start running GC and wait for all the objects to be removed.
    initial_manager->start();
    RPTEST_REQUIRE_EVENTUALLY(
      30s, [&] { return all_objects_missing(object_ids); });
}

TEST_F(DbDomainManagerTest, TestGetSizeBasic) {
    auto tp = make_tp();
    // Add 3 objects, each with an extent of size 512 bytes.
    {
        l1_rpc::add_objects_request req;
        req.new_objects = make_new_objects(tp, kafka::offset(0), 3, 10);
        req.new_terms = make_terms(tp, kafka::offset(0), model::term_id(1));
        auto reply = initial_manager->add_objects(std::move(req)).get();
        ASSERT_EQ(reply.ec, l1_rpc::errc::ok);
    }

    // Query size - should be 3 * 512 = 1536 bytes.
    l1_rpc::get_size_request size_req{.tp = tp};
    auto size_reply = initial_manager->get_size(std::move(size_req)).get();
    ASSERT_EQ(size_reply.ec, l1_rpc::errc::ok);
    ASSERT_EQ(size_reply.size, 3 * 512);
}

TEST_F(DbDomainManagerTest, TestGetSizeAfterReplace) {
    auto tp = make_tp();
    // Add 5 objects, each with an extent of size 512 bytes.
    for (int i = 0; i < 5; ++i) {
        l1_rpc::add_objects_request req;
        req.new_objects = make_new_objects(tp, kafka::offset(i), 1, 1);
        req.new_terms = make_terms(tp, kafka::offset(i), model::term_id(1));
        auto reply = initial_manager->add_objects(std::move(req)).get();
        ASSERT_EQ(reply.ec, l1_rpc::errc::ok);
    }

    // Initial size should be 5 * 512 = 2560 bytes.
    {
        l1_rpc::get_size_request size_req{.tp = tp};
        auto size_reply = initial_manager->get_size(std::move(size_req)).get();
        ASSERT_EQ(size_reply.ec, l1_rpc::errc::ok);
        ASSERT_EQ(size_reply.size, 5 * 512);
    }

    // Replace all 5 extents with 1 extent (also 512 bytes).
    l1_rpc::replace_objects_request replace_req{
      .metastore_partition = model::partition_id(0),
      .new_objects = make_new_objects(tp, kafka::offset(0), 1, 5),
    };
    auto replace_reply
      = initial_manager->replace_objects(std::move(replace_req)).get();
    ASSERT_EQ(replace_reply.ec, l1_rpc::errc::ok);

    // Size should now be 1 * 512 = 512 bytes.
    {
        l1_rpc::get_size_request size_req{.tp = tp};
        auto size_reply = initial_manager->get_size(std::move(size_req)).get();
        ASSERT_EQ(size_reply.ec, l1_rpc::errc::ok);
        ASSERT_EQ(size_reply.size, 512);
    }
}

TEST_F(DbDomainManagerTest, TestGetSizeMissingPartition) {
    auto tp = make_tp();
    // Query size for a partition that doesn't exist.
    l1_rpc::get_size_request size_req{.tp = tp};
    auto size_reply = initial_manager->get_size(std::move(size_req)).get();
    ASSERT_EQ(size_reply.ec, l1_rpc::errc::missing_ntp);
}
