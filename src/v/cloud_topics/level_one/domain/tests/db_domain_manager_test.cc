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
#include "cloud_topics/level_one/common/object_id.h"
#include "cloud_topics/level_one/domain/db_domain_manager.h"
#include "cloud_topics/level_one/metastore/lsm/stm.h"
#include "cloud_topics/level_one/metastore/rpc_types.h"
#include "cloud_topics/level_one/metastore/state_update.h"
#include "config/node_config.h"
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
      , staging_directory(staging_path.data()) {}

    // Open a new db_domain_manager for the current term. Previous managers are
    // retained in the list too, to validate that their usage fails.
    db_domain_manager* open_manager() {
        auto mgr = std::make_unique<db_domain_manager>(
          stm_ptr->raft()->confirmed_term(),
          stm_ptr,
          staging_directory.get_path(),
          remote,
          bucket);
        mgr->start();
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

} // namespace

class DbDomainManagerTest
  : public raft::raft_fixture
  , public s3_imposter_fixture {
public:
    static constexpr auto num_nodes = 3;
    using opt_ref = std::optional<std::reference_wrapper<domain_manager_node>>;

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

        initial_manager = initial_leader->open_manager();
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

    std::array<std::unique_ptr<domain_manager_node>, num_nodes> dm_nodes;
    scoped_config cfg;
    std::unique_ptr<cloud_io::scoped_remote> sr;

    // Initial leader and manager on that leader.
    domain_manager_node* initial_leader{nullptr};
    db_domain_manager* initial_manager{nullptr};
};

TEST_F(DbDomainManagerTest, TestConcurrentUpdates) {
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
        leader_node.open_manager();
    }
    done = true;
    ss::when_all_succeed(std::move(futs)).get();
    EXPECT_FALSE(HasFailure());

    // NOTE: expected_add_next may not necessarily be the next offset --
    // adder_loop isn't very strict with its accounting.
    validate_metadata(tp, kafka::offset(0), expected_add_next, exact_next::no);
}

TEST_F(DbDomainManagerTest, TestUpdatesWithDroppedAppends) {
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
        leader_node.open_manager();
    }
    done = true;
    ss::when_all_succeed(std::move(futs)).get();
    EXPECT_FALSE(HasFailure());

    // NOTE: expected_add_next may not necessarily be the next offset --
    // adder_loop isn't very strict with its accounting.
    validate_metadata(tp, kafka::offset(0), expected_add_next, exact_next::no);
}

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
