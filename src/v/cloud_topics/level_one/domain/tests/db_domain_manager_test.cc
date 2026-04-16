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
#include "cloud_topics/level_one/common/file_io.h"
#include "cloud_topics/level_one/common/object_id.h"
#include "cloud_topics/level_one/common/object_utils.h"
#include "cloud_topics/level_one/domain/db_domain_manager.h"
#include "cloud_topics/level_one/domain/domain_manager_probe.h"
#include "cloud_topics/level_one/metastore/domain_uuid.h"
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
          &object_io,
          ss::default_scheduling_group(),
          &probe);
        if (start_gc) {
            mgr->start();
        }
        auto* ptr = mgr.get();
        managers.push_back(std::move(mgr));
        if (managers.size() > 3) {
            inactive_managers.push_back(std::move(managers.front()));
            managers.pop_front();
        }
        return ptr;
    }

    // Stop all managers, ignoring errors during teardown.
    ss::future<> stop_managers() {
        for (auto& mgr : managers) {
            try {
                co_await mgr->stop_and_wait();
            } catch (...) {
                // Ignore errors during teardown.
                auto ex = std::current_exception();
                vlog(dm_test_log.info, "Manager shutdown error: {}", ex);
            }
        }
        for (auto& mgr : inactive_managers) {
            try {
                co_await mgr->stop_and_wait();
            } catch (...) {
                // Ignore errors during teardown.
                auto ex = std::current_exception();
                vlog(
                  dm_test_log.info, "Inactive manager shutdown error: {}", ex);
            }
        }
    }

    ss::shared_ptr<stm> stm_ptr;
    cloud_io::remote* remote;
    const cloud_storage_clients::bucket_name& bucket;
    temporary_dir staging_directory;
    file_io object_io;
    domain_manager_probe probe;

    // Active managers on this node. These managers may be operated on by
    // callers. Not all of these managers are expected to actually work, e.g.
    // managers from previous terms likely won't work. We keep around multiple
    // to validate concurrency, but not too many (hence inactive_managers) to
    // not make these tests too stressful.
    //
    // It is expected that the last manager in this list is typically
    // functional.
    std::list<std::unique_ptr<db_domain_manager>> managers;

    // Inactive managers, left around to destruct at the end of the test.
    std::list<std::unique_ptr<db_domain_manager>> inactive_managers;
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

chunked_vector<new_object> make_new_objects_with_ids(
  const model::topic_id_partition& tp,
  kafka::offset start_offset,
  size_t offsets_per_object,
  const chunked_vector<object_id>& object_ids) {
    chunked_vector<new_object> objects;
    objects.reserve(object_ids.size());

    auto next_offset = start_offset;
    for (const auto& oid : object_ids) {
        auto base = next_offset;
        auto last = kafka::offset(next_offset() + offsets_per_object - 1);
        auto obj = make_new_object(tp, base, last);
        obj.oid = oid;
        objects.push_back(std::move(obj));
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
      domain_cloud_prefix(uuid)};
    temporary_dir tmp("test");
    auto cloud_db = lsm::database::open(
                      {.database_epoch = db_epoch},
                      lsm::io::persistence{
                        .data = lsm::io::open_cloud_data_persistence(
                                  tmp.get_path(),
                                  remote,
                                  bucket,
                                  domain_prefix,
                                  ss::sstring(uuid()))
                                  .get(),
                        .metadata = lsm::io::open_cloud_metadata_persistence(
                                      remote, bucket, domain_prefix)
                                      .get()})
                      .get();

    // Pre-register the object IDs before building the add_objects rows.
    preregister_objects_db_update prereg_update;
    prereg_update.registered_at = model::timestamp::now();
    for (const auto& obj : new_objects) {
        prereg_update.object_ids.push_back(obj.oid);
    }
    {
        auto prereg_reader = state_reader(cloud_db.create_snapshot());
        chunked_vector<write_batch_row> prereg_rows;
        auto prereg_res
          = prereg_update.build_rows(prereg_reader, prereg_rows).get();
        ASSERT_TRUE(prereg_res.has_value()) << prereg_res.error();
        auto wb = cloud_db.create_write_batch();
        for (auto& r : prereg_rows) {
            wb.put(r.key, std::move(r.value), lsm::sequence_number{1});
        }
        cloud_db.apply(std::move(wb)).get();
    }

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
        wb.put(r.key, std::move(r.value), lsm::sequence_number{2});
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
                auto prereg = co_await mgr->preregister_objects({
                  .metastore_partition = model::partition_id(0),
                  .count = 1,
                });
                if (prereg.ec != l1_rpc::errc::ok) {
                    continue;
                }
                l1_rpc::add_objects_request req;
                req.new_objects = make_new_objects_with_ids(
                  tp, expected_next, 1, prereg.object_ids);
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
                auto prereg = co_await mgr->preregister_objects({
                  .metastore_partition = model::partition_id(0),
                  .count = 1,
                });
                if (prereg.ec != l1_rpc::errc::ok) {
                    continue;
                }
                l1_rpc::replace_objects_request req{
                  .metastore_partition = model::partition_id(0),
                  .new_objects = make_new_objects_with_ids(
                    tp, offset_to_replace, 1, prereg.object_ids),
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
            co_await random_sleep_ms(1000);
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

    // Preregisters objects and adds them via add_objects.
    void add_preregistered_objects(
      const model::topic_id_partition& tp,
      kafka::offset start_offset,
      size_t count,
      size_t offsets_per_object,
      model::term_id term) {
        auto prereg_reply = initial_manager
                              ->preregister_objects({
                                .metastore_partition = model::partition_id(0),
                                .count = static_cast<uint32_t>(count),
                              })
                              .get();
        ASSERT_EQ(prereg_reply.ec, l1_rpc::errc::ok);
        l1_rpc::add_objects_request req;
        req.new_objects = make_new_objects_with_ids(
          tp, start_offset, offsets_per_object, prereg_reply.object_ids);
        req.new_terms = make_terms(tp, start_offset, term);
        auto reply = initial_manager->add_objects(std::move(req)).get();
        ASSERT_EQ(reply.ec, l1_rpc::errc::ok);
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
        }
        if (args.with_flush_loop) {
            for (int i = 0; i < 2; ++i) {
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
    cfg.get("cloud_topics_metastore_replication_timeout_ms")
      .set_value(std::chrono::milliseconds(10s));
    cfg.get("cloud_topics_metastore_lsm_apply_timeout_ms")
      .set_value(std::chrono::milliseconds(30s));
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
        auto prereg_reply = initial_manager
                              ->preregister_objects({
                                .metastore_partition = model::partition_id(0),
                                .count = 3,
                              })
                              .get();
        ASSERT_EQ(prereg_reply.ec, l1_rpc::errc::ok);
        l1_rpc::add_objects_request req;
        req.new_objects = make_new_objects_with_ids(
          tp, kafka::offset(0), 10, prereg_reply.object_ids);
        req.new_terms = make_terms(tp, kafka::offset(0), model::term_id(1));
        auto reply = initial_manager->add_objects(std::move(req)).get();
        ASSERT_EQ(reply.ec, l1_rpc::errc::ok);
    }
    // Add [30, 59].
    {
        auto prereg_reply = initial_manager
                              ->preregister_objects({
                                .metastore_partition = model::partition_id(0),
                                .count = 3,
                              })
                              .get();
        ASSERT_EQ(prereg_reply.ec, l1_rpc::errc::ok);
        l1_rpc::add_objects_request req;
        req.new_objects = make_new_objects_with_ids(
          tp, kafka::offset(30), 10, prereg_reply.object_ids);
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
        auto prereg_reply = initial_manager
                              ->preregister_objects({
                                .metastore_partition = model::partition_id(0),
                                .count = 1,
                              })
                              .get();
        ASSERT_EQ(prereg_reply.ec, l1_rpc::errc::ok);
        l1_rpc::add_objects_request req;
        req.new_objects = make_new_objects_with_ids(
          tp, kafka::offset(i), 1, prereg_reply.object_ids);
        req.new_terms = make_terms(tp, kafka::offset(i), model::term_id(1));
        auto reply = initial_manager->add_objects(std::move(req)).get();
        ASSERT_EQ(reply.ec, l1_rpc::errc::ok);
    }
    validate_metadata(
      tp, kafka::offset(0), kafka::offset(10), exact_next::yes, 10);
    // Replace [0, 9] with one object.
    auto prereg_reply = initial_manager
                          ->preregister_objects({
                            .metastore_partition = model::partition_id(0),
                            .count = 1,
                          })
                          .get();
    ASSERT_EQ(prereg_reply.ec, l1_rpc::errc::ok);
    l1_rpc::replace_objects_request req{
      .metastore_partition = model::partition_id(0),
      .new_objects = make_new_objects_with_ids(
        tp, kafka::offset(0), 10, prereg_reply.object_ids),
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
    cfg.get("cloud_topics_long_term_file_deletion_delay").set_value(0ms);

    auto tp = make_tp();

    // Create some object metadata and actually create dummy objects for them.
    auto prereg_reply = initial_manager
                          ->preregister_objects({
                            .metastore_partition = model::partition_id(0),
                            .count = 1234,
                          })
                          .get();
    ASSERT_EQ(prereg_reply.ec, l1_rpc::errc::ok);
    auto new_objects = make_new_objects_with_ids(
      tp, kafka::offset(0), 10, prereg_reply.object_ids);
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

    // Remove the topic, retrying as needed since batching may return
    // not_removed when extent counts exceed the batch limit.
    {
        chunked_vector<model::topic_id> remaining;
        remaining.push_back(tp.topic_id);
        while (!remaining.empty()) {
            l1_rpc::remove_topics_request req;
            req.topics = std::move(remaining);
            auto reply = initial_manager->remove_topics(std::move(req)).get();
            ASSERT_EQ(reply.ec, l1_rpc::errc::ok);
            remaining = std::move(reply.not_removed);
        }
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

TEST_F(DbDomainManagerTest, TestGarbageCollectionDeletionDelay) {
    cfg.get("cloud_topics_long_term_garbage_collection_interval")
      .set_value(100ms);
    cfg.get("cloud_topics_long_term_file_deletion_delay").set_value(5000ms);

    auto tp = make_tp();

    auto prereg_reply = initial_manager
                          ->preregister_objects({
                            .metastore_partition = model::partition_id(0),
                            .count = 3,
                          })
                          .get();
    ASSERT_EQ(prereg_reply.ec, l1_rpc::errc::ok);
    auto new_objects = make_new_objects_with_ids(
      tp, kafka::offset(0), 10, prereg_reply.object_ids);
    auto object_ids = put_dummy_objects(initial_leader->object_io, new_objects);

    for (const auto& oid : object_ids) {
        ASSERT_TRUE(object_exists(oid).get());
    }

    // Add and remove objects so the metastore thinks they are eligible for
    // removal.
    {
        l1_rpc::add_objects_request req;
        req.new_objects = std::move(new_objects);
        req.new_terms = make_terms(tp, kafka::offset(0), model::term_id(1));
        auto reply = initial_manager->add_objects(std::move(req)).get();
        ASSERT_EQ(reply.ec, l1_rpc::errc::ok);
    }
    {
        l1_rpc::remove_topics_request req;
        req.topics.push_back(tp.topic_id);
        auto reply = initial_manager->remove_topics(std::move(req)).get();
        ASSERT_EQ(reply.ec, l1_rpc::errc::ok);
        ASSERT_TRUE(reply.not_removed.empty());
    }

    ASSERT_EQ(initial_manager->flush_domain({}).get().ec, l1_rpc::errc::ok);

    // Start GC. The deletion delay should prevent immediate removal.
    initial_manager->start();

    // Verify objects survive several GC cycles while the delay hasn't elapsed.
    ss::sleep(1s).get();
    for (const auto& oid : object_ids) {
        EXPECT_TRUE(object_exists(oid).get());
    }

    // Objects should eventually be deleted once the delay elapses.
    RPTEST_REQUIRE_EVENTUALLY(
      30s, [&] { return all_objects_missing(object_ids); });
}

TEST_F(DbDomainManagerTest, TestGetSizeBasic) {
    auto tp = make_tp();
    // Add 3 objects, each with an extent of size 512 bytes.
    {
        auto prereg_reply = initial_manager
                              ->preregister_objects({
                                .metastore_partition = model::partition_id(0),
                                .count = 3,
                              })
                              .get();
        ASSERT_EQ(prereg_reply.ec, l1_rpc::errc::ok);
        l1_rpc::add_objects_request req;
        req.new_objects = make_new_objects_with_ids(
          tp, kafka::offset(0), 10, prereg_reply.object_ids);
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
        auto prereg_reply = initial_manager
                              ->preregister_objects({
                                .metastore_partition = model::partition_id(0),
                                .count = 1,
                              })
                              .get();
        ASSERT_EQ(prereg_reply.ec, l1_rpc::errc::ok);
        l1_rpc::add_objects_request req;
        req.new_objects = make_new_objects_with_ids(
          tp, kafka::offset(i), 1, prereg_reply.object_ids);
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
    auto replace_prereg_reply = initial_manager
                                  ->preregister_objects({
                                    .metastore_partition = model::partition_id(
                                      0),
                                    .count = 1,
                                  })
                                  .get();
    ASSERT_EQ(replace_prereg_reply.ec, l1_rpc::errc::ok);
    l1_rpc::replace_objects_request replace_req{
      .metastore_partition = model::partition_id(0),
      .new_objects = make_new_objects_with_ids(
        tp, kafka::offset(0), 5, replace_prereg_reply.object_ids),
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

TEST_F(DbDomainManagerTest, TestPreregisteredObjectExpiry) {
    cfg.get("cloud_topics_preregistered_object_ttl").set_value(1ms);
    cfg.get("cloud_topics_long_term_garbage_collection_interval")
      .set_value(100ms);
    cfg.get("cloud_topics_long_term_file_deletion_delay").set_value(0ms);

    auto tp = make_tp();
    auto prereg_reply = initial_manager
                          ->preregister_objects({
                            .metastore_partition = model::partition_id(0),
                            .count = 1,
                          })
                          .get();
    ASSERT_EQ(prereg_reply.ec, l1_rpc::errc::ok);

    // Upload dummy objects to cloud storage so GC can physically delete them.
    auto new_objects = make_new_objects_with_ids(
      tp, kafka::offset(0), 1, prereg_reply.object_ids);
    auto object_ids = put_dummy_objects(initial_leader->object_io, new_objects);
    for (const auto& oid : object_ids) {
        ASSERT_TRUE(object_exists(oid).get());
    }

    initial_manager->start();

    // Eventually a flush should happen after expiry and the objects become
    // eligible for removal.
    RPTEST_REQUIRE_EVENTUALLY(30s, [&](this auto) -> ss::future<bool> {
        co_await initial_manager->flush_domain({});

        co_return co_await all_objects_missing(object_ids);
    });
}

TEST_F(DbDomainManagerTest, TestSetStartOffsetBatchedExtentRemoval) {
    auto tp = make_tp();

    // Add 2500 extents (note, internally we remove 1000 at a time).
    ASSERT_NO_FATAL_FAILURE(add_preregistered_objects(
      tp, kafka::offset(0), 2500, 1, model::term_id(1)));

    // Each call does one batch. The first calls should return has_more=true,
    // and the final call has_more=false once the target is reached.
    int rounds = 0;
    bool has_more = true;
    while (has_more) {
        l1_rpc::set_start_offset_request set_req{
          .tp = tp,
          .start_offset = kafka::offset(2500),
        };
        auto set_reply
          = initial_manager->set_start_offset(std::move(set_req)).get();
        ASSERT_EQ(set_reply.ec, l1_rpc::errc::ok);
        has_more = set_reply.has_more;
        ++rounds;
    }
    // 2500 extents at 1000 per batch = 3 rounds.
    ASSERT_EQ(rounds, 3);

    l1_rpc::get_offsets_request verify_req{.tp = tp};
    auto verify_reply
      = initial_manager->get_offsets(std::move(verify_req)).get();
    ASSERT_EQ(verify_reply.ec, l1_rpc::errc::ok);
    ASSERT_EQ(verify_reply.start_offset, kafka::offset(2500));
    ASSERT_EQ(verify_reply.next_offset, kafka::offset(2500));

    l1_rpc::get_size_request size_req{.tp = tp};
    auto size_reply = initial_manager->get_size(std::move(size_req)).get();
    ASSERT_EQ(size_reply.ec, l1_rpc::errc::ok);
    ASSERT_EQ(size_reply.size, 0);

    // Sanity check: the term for the next offset should still be valid.
    l1_rpc::get_term_for_offset_request term_req{
      .tp = tp,
      .offset = kafka::offset(2500),
    };
    auto term_reply
      = initial_manager->get_term_for_offset(std::move(term_req)).get();
    ASSERT_EQ(term_reply.ec, l1_rpc::errc::ok);
    ASSERT_EQ(term_reply.term, model::term_id(1));
}

// Regression test: when the batch boundary falls in the middle of an extent,
// set_start_offset must not delete that extent.
TEST_F(DbDomainManagerTest, TestSetStartOffsetMidExtent) {
    auto tp = make_tp();

    // 1001 extents of 10 offsets each: [0,9], [10,19], ..., [10000,10009].
    // Enough to exceed the 1000-extent batch limit.
    ASSERT_NO_FATAL_FAILURE(add_preregistered_objects(
      tp, kafka::offset(0), 1001, 10, model::term_id(1)));

    // Target offset 9995 lands in the middle of extent [9990, 9999].
    // The batch scans 1000 extents, ending at [9990,9999]. Without
    // clamping, the intermediate offset would be next_offset(9999)=10000,
    // which would incorrectly delete extent [9990,9999].
    // Retry until all batches are processed.
    bool has_more = true;
    while (has_more) {
        l1_rpc::set_start_offset_request set_req{
          .tp = tp,
          .start_offset = kafka::offset(9995),
        };
        auto set_reply
          = initial_manager->set_start_offset(std::move(set_req)).get();
        ASSERT_EQ(set_reply.ec, l1_rpc::errc::ok);
        has_more = set_reply.has_more;
    }

    l1_rpc::get_offsets_request verify_req{.tp = tp};
    auto verify_reply
      = initial_manager->get_offsets(std::move(verify_req)).get();
    ASSERT_EQ(verify_reply.ec, l1_rpc::errc::ok);
    ASSERT_EQ(verify_reply.start_offset, kafka::offset(9995));
    ASSERT_EQ(verify_reply.next_offset, kafka::offset(10010));
}

TEST_F(DbDomainManagerTest, TestRemoveTopicsBatchedExtentRemoval) {
    auto small_tp = make_tp();
    auto large_tp = make_tp();

    // Add small partition.
    ASSERT_NO_FATAL_FAILURE(add_preregistered_objects(
      small_tp, kafka::offset(0), 50, 1, model::term_id(1)));

    // Add large partition.
    ASSERT_NO_FATAL_FAILURE(add_preregistered_objects(
      large_tp, kafka::offset(0), 2500, 1, model::term_id(1)));

    // Remove both topics, retrying as needed since batching may return
    // not_removed topics when extent counts exceed the batch limit.
    chunked_vector<model::topic_id> remaining;
    remaining.push_back(small_tp.topic_id);
    remaining.push_back(large_tp.topic_id);
    while (!remaining.empty()) {
        l1_rpc::remove_topics_request remove_req;
        remove_req.topics = std::move(remaining);
        auto remove_reply
          = initial_manager->remove_topics(std::move(remove_req)).get();
        ASSERT_EQ(remove_reply.ec, l1_rpc::errc::ok);
        remaining = std::move(remove_reply.not_removed);
    }

    // Verify both partitions no longer exist.
    {
        l1_rpc::get_offsets_request req{.tp = small_tp};
        auto reply = initial_manager->get_offsets(std::move(req)).get();
        ASSERT_EQ(reply.ec, l1_rpc::errc::missing_ntp);
    }
    {
        l1_rpc::get_offsets_request req{.tp = large_tp};
        auto reply = initial_manager->get_offsets(std::move(req)).get();
        ASSERT_EQ(reply.ec, l1_rpc::errc::missing_ntp);
    }
}
