/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/common/fake_io.h"
#include "cloud_topics/level_one/metastore/garbage_collector.h"
#include "cloud_topics/level_one/metastore/simple_stm.h"
#include "cloud_topics/level_one/metastore/state_update.h"
#include "config/property.h"
#include "gtest/gtest.h"
#include "model/fundamental.h"
#include "raft/tests/raft_fixture.h"
#include "storage/record_batch_builder.h"

using namespace cloud_topics::l1;

namespace {
using o = kafka::offset;
using ts = model::timestamp;
ss::abort_source never_abort;
ss::logger test_log("gc-test");
} // namespace

class GarbageCollectorTest : public raft::stm_raft_fixture<simple_stm> {
public:
    stm_shptrs_t create_stms(
      raft::state_machine_manager_builder& builder,
      raft::raft_node_instance& node) override {
        return builder.create_stm<simple_stm>(
          test_log, node.raft().get(), config::mock_binding(5s));
    }

    model::topic_id_partition make_tp(int i) {
        return model::topic_id_partition::from(
          fmt::format("deadbeef-aaaa-0000-0000-000000000000/{}", i));
    }

    ss::future<>
    add_objects(simple_stm* stm, int partitions_count, int last_offset) {
        chunked_vector<new_object> new_objects;
        term_state_update_t terms;

        for (int i = 0; i < partitions_count; ++i) {
            auto tp = make_tp(i);
            auto oid = create_object_id();

            new_object obj;
            obj.oid = oid;
            obj.footer_pos = 1000;
            obj.object_size = 1500;
            obj.extent_metas[tp.topic_id][tp.partition] = new_object::metadata{
              .base_offset = o{0},
              .last_offset = o{last_offset},
              .max_timestamp = ts{10000},
              .filepos = 0,
              .len = 500,
            };
            new_objects.push_back(std::move(obj));
            terms[tp].emplace_back(
              term_start{.term_id = model::term_id{0}, .start_offset = o{0}});

            auto file_res = co_await _io.create_tmp_file();
            ASSERT_TRUE_CORO(file_res.has_value());
            auto ostream = co_await (*file_res)->output_stream();
            co_await ostream.close();
            auto put_res = co_await _io.put_object(
              oid, file_res->get(), &never_abort);
            ASSERT_TRUE_CORO(put_res.has_value());
            co_await file_res->get()->remove();
        }
        auto sync_res = co_await stm->sync(10s);
        ASSERT_TRUE_CORO(sync_res.has_value());

        {
            preregister_objects_update prereg;
            prereg.registered_at = model::timestamp::now();
            for (const auto& o : new_objects) {
                prereg.object_ids.push_back(o.oid);
            }
            storage::record_batch_builder prereg_builder(
              model::record_batch_type::l1_stm, model::offset{0});
            prereg_builder.add_raw_kv(
              serde::to_iobuf(preregister_objects_update::key),
              serde::to_iobuf(std::move(prereg)));
            auto prereg_repl = co_await stm->replicate_and_wait(
              sync_res.value(), std::move(prereg_builder).build(), never_abort);
            ASSERT_TRUE_CORO(prereg_repl.has_value());
            sync_res = co_await stm->sync(10s);
            ASSERT_TRUE_CORO(sync_res.has_value());
        }

        auto update_res = add_objects_update::build(
          stm->state(), std::move(new_objects), std::move(terms));
        ASSERT_TRUE_CORO(update_res.has_value());
        storage::record_batch_builder builder(
          model::record_batch_type::l1_stm, model::offset{0});
        builder.add_raw_kv(
          serde::to_iobuf(add_objects_update::key),
          serde::to_iobuf(std::move(update_res.value())));

        auto repl_res = co_await stm->replicate_and_wait(
          sync_res.value(), std::move(builder).build(), never_abort);
        ASSERT_TRUE_CORO(repl_res.has_value());
    }

    ss::future<> set_start_offset(
      simple_stm* stm, model::topic_id_partition tp, kafka::offset new_start) {
        auto sync_res = co_await stm->sync(10s);
        ASSERT_TRUE_CORO(sync_res.has_value());

        auto update_res = set_start_offset_update::build(
          stm->state(), tp, new_start);
        ASSERT_TRUE_CORO(update_res.has_value());
        storage::record_batch_builder builder(
          model::record_batch_type::l1_stm, model::offset{0});
        builder.add_raw_kv(
          serde::to_iobuf(set_start_offset_update::key),
          serde::to_iobuf(std::move(update_res.value())));

        auto repl_res = co_await stm->replicate_and_wait(
          sync_res.value(), std::move(builder).build(), never_abort);
        ASSERT_TRUE_CORO(repl_res.has_value());
    }

    size_t count_objects() { return _io.list_objects().size(); }

    fake_io _io;
};

TEST_F(GarbageCollectorTest, TestGarbageCollectBasic) {
    initialize_state_machines(1).get();
    wait_for_leader(5s).get();
    auto stm = get_stm<0>(*nodes().begin()->second);

    constexpr auto partitions_count = 10;
    add_objects(stm.get(), partitions_count, 999).get();
    ASSERT_EQ(partitions_count, count_objects());

    // Fully remove all objects.
    for (int i = 0; i < partitions_count; ++i) {
        set_start_offset(stm.get(), make_tp(i), o{1000}).get();
    }

    size_t objects_before = stm->state().objects.size();
    EXPECT_EQ(partitions_count, objects_before);

    // When we garbage collect, all objects get removed.
    garbage_collector gc(stm.get(), &_io);
    auto gc_res = gc.remove_unreferenced_objects(&never_abort).get();
    ASSERT_TRUE(gc_res.has_value());

    EXPECT_EQ(0, stm->state().objects.size());
    ASSERT_EQ(0, count_objects());
}

TEST_F(GarbageCollectorTest, TestGarbageCollectPartiallyRemovedObjects) {
    initialize_state_machines(1).get();
    wait_for_leader(5s).get();
    auto stm = get_stm<0>(*nodes().begin()->second);

    constexpr auto partitions_count = 10;
    add_objects(stm.get(), partitions_count, 999).get();

    // Fully remove data from first 5 partitions (should be GC'd)
    for (int i = 0; i < 5; ++i) {
        set_start_offset(stm.get(), make_tp(i), o{1000}).get();
    }

    // Partially remove data from next 5 partitions (should NOT be GC'd)
    for (int i = 5; i < partitions_count; ++i) {
        set_start_offset(stm.get(), make_tp(i), o{500}).get();
    }
    ASSERT_EQ(partitions_count, count_objects());

    // Run garbage collector
    garbage_collector gc(stm.get(), &_io);
    auto gc_res = gc.remove_unreferenced_objects(&never_abort).get();
    ASSERT_TRUE(gc_res.has_value());

    // The 5 partially removed objects should still be in state
    EXPECT_EQ(5, stm->state().objects.size());
    ASSERT_EQ(5, count_objects());
}

// A fake_io wrapper that fails on delete_objects
class failing_io : public io {
public:
    explicit failing_io(io* underlying)
      : underlying_(underlying) {}

    ss::future<std::expected<std::unique_ptr<staging_file>, errc>>
    create_tmp_file() override {
        return underlying_->create_tmp_file();
    }

    ss::future<std::expected<void, errc>>
    put_object(object_id id, staging_file* f, ss::abort_source* as) override {
        return underlying_->put_object(id, f, as);
    }

    ss::future<std::expected<ss::input_stream<char>, errc>>
    read_object(object_extent ext, ss::abort_source* as) override {
        return underlying_->read_object(ext, as);
    }

    ss::future<std::expected<void, errc>>
    delete_objects(chunked_vector<object_id>, ss::abort_source*) override {
        return ss::make_ready_future<std::expected<void, errc>>(
          std::unexpected(errc::cloud_op_error));
    }

    ss::future<std::expected<cloud_storage_clients::multipart_upload_ref, errc>>
    create_multipart_upload(
      object_id id, size_t part_size, ss::abort_source* as) override {
        return underlying_->create_multipart_upload(id, part_size, as);
    }

private:
    io* underlying_;
};

TEST_F(GarbageCollectorTest, TestGarbageCollectIODeleteFailure) {
    initialize_state_machines(1).get();
    wait_for_leader(5s).get();
    auto stm = get_stm<0>(*nodes().begin()->second);

    constexpr auto partitions_count = 10;
    add_objects(stm.get(), partitions_count, 999).get();

    // Fully remove data from all partitions
    for (int i = 0; i < partitions_count; ++i) {
        set_start_offset(stm.get(), make_tp(i), o{1000}).get();
    }

    size_t objects_before_gc = stm->state().objects.size();
    ASSERT_GT(objects_before_gc, 0);

    // Run GC with failing IO
    failing_io fail_io(&_io);
    garbage_collector gc(stm.get(), &fail_io);
    auto gc_res = gc.remove_unreferenced_objects(&never_abort).get();

    // GC should return error
    ASSERT_FALSE(gc_res.has_value());
    EXPECT_EQ("io error", gc_res.error());

    // Objects should still be in state (no state update should have occurred)
    EXPECT_EQ(objects_before_gc, stm->state().objects.size());
}

TEST_F(GarbageCollectorTest, TestNonLeaderFails) {
    initialize_state_machines(3).get();
    auto leader_id = wait_for_leader(5s).get();
    simple_stm* leader_stm = nullptr;
    simple_stm* follower_stm = nullptr;
    for (auto& [id, node] : nodes()) {
        auto stm = get_stm<0>(*node);
        if (id == leader_id) {
            leader_stm = stm.get();
        } else {
            follower_stm = stm.get();
        }
    }
    ASSERT_NE(nullptr, follower_stm);
    ASSERT_NE(nullptr, leader_stm);

    // Set up for GCing some objects.
    constexpr auto partitions_count = 10;
    add_objects(leader_stm, partitions_count, 999).get();
    for (int i = 0; i < partitions_count; ++i) {
        set_start_offset(leader_stm, make_tp(i), o{1000}).get();
    }
    ASSERT_EQ(partitions_count, count_objects());

    // Run on the follower. This should fail and the objects should remain.
    garbage_collector gc(follower_stm, &_io);
    auto gc_res = gc.remove_unreferenced_objects(&never_abort).get();
    ASSERT_FALSE(gc_res.has_value());
    ASSERT_EQ(partitions_count, count_objects());
}
