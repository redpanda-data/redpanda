/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/tests/manual_fixture.h"
#include "cloud_storage/tests/produce_utils.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "kafka/server/tests/list_offsets_utils.h"
#include "kafka/server/tests/produce_consume_utils.h"
#include "model/fundamental.h"
#include "redpanda/tests/fixture.h"
#include "storage/disk_log_impl.h"
#include "test_utils/async.h"

#include <seastar/core/io_priority_class.hh>
#include <seastar/core/lowres_clock.hh>

#include <boost/algorithm/string/predicate.hpp>

#include <iterator>

static ss::logger fuzz_log("e2e_fuzz_test");
namespace {
const model::topic topic_name("tapioca");
const model::partition_id pid(0);
model::ntp ntp(model::kafka_namespace, topic_name, pid());

// Like `% mod` but returns at least one.
void mod_at_least_one(int* r, int mod) {
    if (mod <= 1) {
        mod = 2;
    }
    auto rn = *r;
    *r = 1 + rn % (mod - 1);
}

enum class op_type {
    min = 0,
    produce = 0,     // (num_records, cardinality)
    local_roll,      // (node_idx,    unused
    local_gc,        // (node_idx,    num_segments)
    local_gc_one,    // (node_idx,    unused)
    local_compact,   // (node_idx,    unused)
    upload_segments, // (unused,      unused)
    upload_manifest, // (unused,      unused)
    switch_leader,   // (node_idx,    unused)
    max = switch_leader,
};

const char* op_names[]{
  "op_type::produce",
  "op_type::local_roll",
  "op_type::local_gc",
  "op_type::local_gc_one",
  "op_type::local_compact",
  "op_type::upload_segments",
  "op_type::upload_manifest",
  "op_type::switch_leader",
};

struct test_op {
    op_type type{op_type::min};
    int arg1{-1};
    int arg2{-1};

    ss::sstring to_string() const {
        return ssx::sformat(
          "{{ {}, {}, {} }}", op_names[static_cast<int>(type)], arg1, arg2);
    }
};

const std::vector<std::pair<op_type, int>> op_weights{
  {op_type::produce, 5},
  {op_type::local_roll, 2},
  {op_type::local_gc, 2},
  {op_type::local_gc_one, 3},
  {op_type::local_compact, 3},
  {op_type::upload_segments, 3},
  {op_type::upload_manifest, 5},
  {op_type::switch_leader, 2},
};

test_op random_operation() {
    static const int weight_total = [&] {
        int total = 0;
        for (const auto& [_, weight] : op_weights) {
            total += weight;
        }
        return total;
    }();
    auto r = random_generators::get_int(0, weight_total);
    int sum = 0;
    op_type type{op_type::max};
    for (const auto& [op, weight] : op_weights) {
        sum += weight;
        if (sum > r) {
            type = op;
            break;
        }
    }
    auto arg1 = random_generators::get_int(0, 100);
    auto arg2 = random_generators::get_int(0, 100);
    return {type, arg1, arg2};
}

std::vector<test_op> generate_workload(size_t count) {
    std::vector<test_op> ret;
    ret.reserve(count);
    bool replicated_in_term = true;
    const int num_nodes = 2;
    const int max_records = 10;
    const int max_cardinality = 5;
    int cur_leader_idx = 0;
    int num_records = 0;
    int num_records_uploaded = 0;
    int num_records_in_manifest = 0;

    // The number of expected local segments per node. We shouldn't try to GC
    // more than this number of segments.
    std::vector<int> num_local_segs_per_node(num_nodes);

    // Whether the active segment on each node has any offsets, in which case
    // we are eligible to roll the segment.
    std::vector<bool> offsets_in_active_seg_per_node(num_nodes);
    auto set_offsets_in_active_seg_all_nodes = [&] {
        for (int i = 0; i < num_nodes; i++) {
            offsets_in_active_seg_per_node[i] = true;
        }
    };
    // Do some best effort cleanup of randomly generated operations by
    // enforcing some rough invariants for scheduled operations (e.g. only
    // roll the local log when there's data to roll).
    while (ret.size() < count) {
        auto op = random_operation();
        int node_idx = op.arg1 % num_nodes;
        auto& num_local_segs = num_local_segs_per_node[node_idx];
        auto active_segment_non_empty
          = offsets_in_active_seg_per_node[node_idx];
        switch (op.type) {
        case op_type::produce:
            replicated_in_term = true;
            set_offsets_in_active_seg_all_nodes();
            mod_at_least_one(&op.arg1, max_records);
            mod_at_least_one(&op.arg2, max_cardinality);
            num_records += op.arg1;
            break;

        case op_type::local_roll:
            // Only roll if we've added to the log since last roll.
            if (active_segment_non_empty) {
                continue;
            }
            num_local_segs++;
            offsets_in_active_seg_per_node[node_idx] = false;
            op.arg1 = node_idx;
            op.arg2 = -1;
            break;

        case op_type::local_gc:
            if (num_local_segs == 0) {
                // Nothing to GC.
                continue;
            }
            op.arg1 = node_idx;
            mod_at_least_one(&op.arg2, num_local_segs);
            break;

        case op_type::local_gc_one:
            if (num_local_segs == 0) {
                // Nothing to GC.
                continue;
            }
            num_local_segs -= 1;
            op.arg1 = node_idx;
            op.arg2 = -1;
            break;

        case op_type::local_compact:
            if (num_local_segs == 0) {
                // Nothing to compact.
                continue;
            }
            op.arg1 = node_idx;
            op.arg2 = -1;
            break;

        // TODO(awong): split into compacted and non compacted.
        case op_type::upload_segments:
            if (num_records == num_records_uploaded) {
                continue;
            }
            // Rough approximation that uploading uploads everything.
            num_records_uploaded = num_records;
            replicated_in_term = true;
            set_offsets_in_active_seg_all_nodes();
            op.arg1 = -1;
            op.arg2 = -1;
            break;

        case op_type::upload_manifest:
            // Only upload manifest if we've uploaded since last time.
            if (num_records_in_manifest == num_records_uploaded) {
                continue;
            }
            num_records_in_manifest = num_records_uploaded;
            set_offsets_in_active_seg_all_nodes();
            replicated_in_term = true;
            op.arg1 = -1;
            op.arg2 = -1;
            break;

        case op_type::switch_leader:
            if (!replicated_in_term) {
                // Not a hard requirement, but make sure each term does
                // something.
                continue;
            }
            for (int i = 0; i < num_nodes; i++) {
                if (!offsets_in_active_seg_per_node[i]) {
                    // No new segment if active segment is empty.
                    continue;
                }
                // Leadership adds a new segment.
                offsets_in_active_seg_per_node[i] = false;
                num_local_segs_per_node[i]++;
            }
            replicated_in_term = false;
            if (cur_leader_idx == node_idx) {
                op.arg1 = (node_idx + 1) % num_nodes;
            } else {
                op.arg1 = node_idx;
            }
            op.arg2 = -1;
            cur_leader_idx = op.arg1;
            break;
        };
        ret.emplace_back(op);
    }
    return ret;
}

} // namespace

class cloud_storage_e2e_fuzz_test
  : public cloud_storage_manual_multinode_test_base {
public:
    cloud_storage_e2e_fuzz_test()
      : cloud_storage_manual_multinode_test_base(true) {}

    ss::future<> produce(int num_records, int cardinality);
    ss::future<> local_roll(int node_idx);
    ss::future<> local_gc(int node_idx, int num_segments);
    ss::future<> local_compact(int node_idx);
    ss::future<> upload_segments();
    ss::future<> upload_manifest();
    ss::future<> switch_leader(int node_idx);

    ss::future<> run_workload(std::vector<test_op> ops);
    ss::future<> validate_consume();

    // Set up the second fixture and index them in 'fixtures' based on who is
    // the first partition leader.
    void initialize_fixtures() {
        vassert(second_fixture == nullptr, "Fixture already initialized");
        config::shard_local_cfg().enable_leader_balancer.set_value(false);
        second_fixture = start_second_fixture();
        tests::cooperative_spin_wait_with_timeout(3s, [this] {
            return app.controller->get_members_table().local().node_ids().size()
                   == 2;
        }).get();
        cluster::topic_properties props;
        props.shadow_indexing = model::shadow_indexing_mode::full;
        props.cleanup_policy_bitflags
          = model::cleanup_policy_bitflags::compaction
            | model::cleanup_policy_bitflags::deletion;
        add_topic({model::kafka_namespace, topic_name}, 1, props, 2).get();
        boost_require_eventually(15s, [&] {
            cluster::partition* prt_a
              = app.partition_manager.local().get(ntp).get();
            cluster::partition* prt_b
              = second_fixture->app.partition_manager.local().get(ntp).get();
            if (!prt_a || !prt_b) {
                return false;
            }
            if (prt_a->is_leader()) {
                fixtures.emplace_back(this);
                fixtures.emplace_back(second_fixture.get());
                return true;
            }
            if (prt_b->is_leader()) {
                fixtures.emplace_back(second_fixture.get());
                fixtures.emplace_back(this);
                return true;
            }
            return false;
        });
        for (auto* f : fixtures) {
            produce_transports.emplace_back(f->make_kafka_client().get());
            produce_transports.back().start().get();
            consume_transports.emplace_back(f->make_kafka_client().get());
            consume_transports.back().start().get();
            list_transports.emplace_back(f->make_kafka_client().get());
            list_transports.back().start().get();
        }
    }

private:
    int cur_leader_idx{0};
    std::unique_ptr<redpanda_thread_fixture> second_fixture;
    std::vector<redpanda_thread_fixture*> fixtures;
    std::vector<tests::kafka_produce_transport> produce_transports;
    std::vector<tests::kafka_consume_transport> consume_transports;
    std::vector<tests::kafka_list_offsets_transport> list_transports;
};

ss::future<>
cloud_storage_e2e_fuzz_test::produce(int num_records, int cardinality) {
    auto& producer = produce_transports[cur_leader_idx];
    for (int i = 0; i < num_records; i++) {
        co_await producer.produce_to_partition(
          topic_name,
          model::partition_id(0),
          tests::kv_t::sequence(i % cardinality, 1));
    }
}

ss::future<> cloud_storage_e2e_fuzz_test::local_roll(int node_idx) {
    auto* prt
      = fixtures[node_idx]->app.partition_manager.local().get(ntp).get();
    auto* log = dynamic_cast<storage::disk_log_impl*>(prt->log().get());
    co_await log->flush();
    co_await log->force_roll(ss::default_priority_class());
}

ss::future<>
cloud_storage_e2e_fuzz_test::local_gc(int node_idx, int num_segments) {
    auto* prt
      = fixtures[node_idx]->app.partition_manager.local().get(ntp).get();
    auto* log = dynamic_cast<storage::disk_log_impl*>(prt->log().get());
    co_await log->flush();
    size_t total_size = 0;
    size_t size_to_remove = 0;
    for (int i = 0; i < log->segment_count(); i++) {
        const auto size = log->segments()[i]->size_bytes();
        if (i < num_segments) {
            size_to_remove += size;
        }
        total_size += size;
    }
    auto start_time = ss::lowres_clock::now();
    size_t size_to_retain = total_size - size_to_remove;
    storage::gc_config cfg(model::timestamp::min(), size_to_retain);
    co_await log->gc(cfg);
    try {
        // Ignore timeouts here: it's possible that running GC didn't actually
        // schedule anything to be deleted.
        co_await tests::cooperative_spin_wait_with_timeout(
          10s, [prt, start_time] {
              return prt->log_eviction_stm()->last_event_processed_at()
                     > start_time;
          });
    } catch (...) {
    }
}

ss::future<> cloud_storage_e2e_fuzz_test::local_compact(int node_idx) {
    ss::abort_source as;
    auto* prt
      = fixtures[node_idx]->app.partition_manager.local().get(ntp).get();
    storage::housekeeping_config compact_all_cfg(
      model::timestamp::min(),
      std::nullopt,
      prt->archival_meta_stm()->max_collectible_offset(),
      ss::default_priority_class(),
      as);
    auto* log = dynamic_cast<storage::disk_log_impl*>(prt->log().get());
    co_await log->housekeeping(compact_all_cfg);
}

ss::future<> cloud_storage_e2e_fuzz_test::upload_segments() {
    auto* prt
      = fixtures[cur_leader_idx]->app.partition_manager.local().get(ntp).get();
    auto& archiver = prt->archiver()->get();
    BOOST_REQUIRE(co_await archiver.sync_for_tests());
    auto res = co_await archiver.upload_next_candidates();
    BOOST_REQUIRE_EQUAL(0, res.compacted_upload_result.num_failed);
    BOOST_REQUIRE_EQUAL(0, res.non_compacted_upload_result.num_failed);
}

ss::future<> cloud_storage_e2e_fuzz_test::upload_manifest() {
    auto* prt
      = fixtures[cur_leader_idx]->app.partition_manager.local().get(ntp).get();
    auto& archiver = prt->archiver()->get();

    BOOST_REQUIRE(co_await archiver.sync_for_tests());
    auto res = co_await archiver.upload_manifest("test");
    BOOST_REQUIRE_EQUAL(cloud_storage::upload_result::success, res);
    co_await archiver.flush_manifest_clean_offset();
}

ss::future<> cloud_storage_e2e_fuzz_test::switch_leader(int node_idx) {
    auto* prt_l
      = fixtures[cur_leader_idx]->app.partition_manager.local().get(ntp).get();
    auto* prt_f
      = fixtures[node_idx]->app.partition_manager.local().get(ntp).get();
    cluster::transfer_leadership_request transfer_req{
      .group = prt_l->raft()->group(),
      .target = prt_f->raft()->self().id(),
    };
    auto transfer_resp = co_await prt_l->raft()->transfer_leadership(
      transfer_req);
    BOOST_REQUIRE(transfer_resp.success);
    co_await tests::cooperative_spin_wait_with_timeout(
      3s, [&] { return prt_f->is_leader(); });
    cur_leader_idx = node_idx;
}

ss::future<> cloud_storage_e2e_fuzz_test::validate_consume() {
    auto& lister = list_transports[cur_leader_idx];
    auto start_offset = co_await lister.start_offset_for_partition(
      topic_name, model::partition_id(0));
    auto hwm = co_await lister.high_watermark_for_partition(
      topic_name, model::partition_id(0));

    auto& consumer = consume_transports[cur_leader_idx];
    for (auto i = start_offset(); i < hwm(); i++) {
        vlog(fuzz_log.debug, "Consuming from offset {}", i);
        co_await consumer.consume_from_partition(
          topic_name, model::partition_id(0), model::offset(i));
    }
}

ss::future<>
cloud_storage_e2e_fuzz_test::run_workload(std::vector<test_op> ops) {
    for (const auto& op : ops) {
        switch (op.type) {
        case op_type::produce:
            vlog(fuzz_log.info, "Running produce({}, {})", op.arg1, op.arg2);
            co_await produce(op.arg1, op.arg2);
            break;
        case op_type::local_roll:
            vlog(fuzz_log.info, "Running local_roll({})", op.arg1);
            co_await local_roll(op.arg1);
            break;
        case op_type::local_gc:
            vlog(fuzz_log.info, "Running local_gc({}, {})", op.arg1, op.arg2);
            co_await local_gc(op.arg1, op.arg2);
            break;
        case op_type::local_gc_one:
            vlog(fuzz_log.info, "Running local_gc({}, 1)", op.arg1);
            co_await local_gc(op.arg1, 1);
            break;
        case op_type::local_compact:
            vlog(fuzz_log.info, "Running local_compact({})", op.arg1);
            co_await local_compact(op.arg1);
            break;
        case op_type::upload_segments:
            vlog(fuzz_log.info, "Running upload_segments()");
            co_await upload_segments();
            break;
        case op_type::upload_manifest:
            vlog(fuzz_log.info, "Running upload_manifest()");
            co_await upload_manifest();
            break;
        case op_type::switch_leader:
            vlog(fuzz_log.info, "Running switch_leader({})", op.arg1);
            co_await switch_leader(op.arg1);
            break;
        }
        co_await validate_consume();
    }
}

FIXTURE_TEST(e2e_fuzz, cloud_storage_e2e_fuzz_test) {
    auto ops = generate_workload(100);
    for (const auto& op : ops) {
        vlog(fuzz_log.info, "  {},", op.to_string());
    }
    initialize_fixtures();
    run_workload(std::move(ops)).get();
}
