// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/log_eviction_stm.h"
#include "config/configuration.h"
#include "raft/tests/raft_fixture.h"
#include "test_utils/async.h"

#include <seastar/core/abort_source.hh>

#include <gtest/gtest.h>

#include <cstddef>
struct manual_deletion_fixture : public raft::raft_fixture {
    static model::record_batch
    make_batches_with_timestamp(model::timestamp ts) {
        storage::record_batch_builder builder(
          model::record_batch_type::raft_data, model::offset(0));

        builder.add_raw_kv(iobuf::from("key"), iobuf::from("value"));
        auto batch = std::move(builder).build();

        batch.header().first_timestamp = ts;
        batch.header().max_timestamp = ts;

        return batch;
    }

    ss::future<> init_and_start_node(model::node_id id) {
        auto& n = node(id);
        raft::state_machine_manager_builder stm_mgr_builder;
        n.initialise(all_vnodes()).get();
        cluster::log_eviction_stm_factory f(n.get_kvstore());
        f.create(
          stm_mgr_builder,
          n.raft().get(),
          cluster::stm_instance_config{nullptr});
        return n.start(std::move(stm_mgr_builder));
    }

    void prepare_raft_group() {
        config::shard_local_cfg().log_segment_size_min.set_value(128_KiB);
        for (size_t id = 0; id < 3; ++id) {
            add_node(
              model::node_id(static_cast<int32_t>(id)), model::revision_id{0});
        }

        for (auto& [id, _] : nodes()) {
            init_and_start_node(id).get();
        }

        auto leader_id = wait_for_leader(10s).get();
        auto first_ts = model::timestamp::now();
        for (int i = 0; i < 5; i++) {
            node(leader_id)
              .raft()
              ->replicate(
                make_batches(
                  100,
                  [first_ts](auto) {
                      return make_batches_with_timestamp(first_ts);
                  }),
                raft::replicate_options(raft::consistency_level::quorum_ack))
              .get();
            node(leader_id).raft()->step_down("test").get();
            leader_id = wait_for_leader(10s).get();
        }

        retention_timestamp = model::to_timestamp(
          model::timestamp_clock::now() + 2s);
        ss::sleep(5s).get(); // wait to ensure broker_timestamp is different
        auto second_ts = model::timestamp(first_ts() + 200000);
        node(leader_id)
          .raft()
          ->replicate(
            make_batches(
              100,
              [second_ts](auto) {
                  return make_batches_with_timestamp(second_ts);
              }),
            raft::replicate_options(raft::consistency_level::quorum_ack))
          .get();

        wait_for_committed_offset(node(leader_id).raft()->dirty_offset(), 10s)
          .get();
    }
    ss::future<bool> do_execute_housekeeping() {
        for (auto& [_, n] : nodes()) {
            co_await n->raft()->log()->housekeeping(
              storage::housekeeping_config(
                retention_timestamp,
                100_MiB,
                model::offset::max(),
                model::offset::max(),
                model::offset::max(),
                std::nullopt,
                std::nullopt,
                std::chrono::milliseconds{0},
                as,
                storage::ntp_sanitizer_config{.sanitize_only = true}));

            if (n->raft()->log()->offsets().start_offset <= model::offset(0)) {
                co_return false;
            }
        }
        co_return true;
    }
    void apply_retention_policy() {
        tests::cooperative_spin_wait_with_timeout(5s, [this] {
            return do_execute_housekeeping();
        }).get();
    }

    void
    remove_data(const absl::flat_hash_set<model::node_id>& nodes_to_delete) {
        std::vector<std::filesystem::path> to_delete;
        to_delete.reserve(nodes_to_delete.size());

        // disable and remove data
        for (auto id : nodes_to_delete) {
            to_delete.push_back(
              std::filesystem::path(
                node(id).raft()->log()->config().topic_directory()));
        }
        for (auto id : nodes_to_delete) {
            stop_node(id).get();
        }
        for (auto& path : to_delete) {
            std::filesystem::remove_all(path);
        }
        // enable back
        for (auto id : nodes_to_delete) {
            add_node(id, model::revision_id(0));
        }
        for (auto id : nodes_to_delete) {
            init_and_start_node(id).get();
        }
    }

    void remove_all_data() { remove_data(all_ids()); }

    model::timestamp retention_timestamp;
    ss::abort_source as;
};

TEST_F(
  manual_deletion_fixture, test_collected_log_recovery_admin_deletion_all) {
    prepare_raft_group();
    apply_retention_policy();

    // simulate admin deleting log folders. For more details look here:
    //
    // https://github.com/redpanda-data/redpanda/issues/321

    remove_all_data();
    auto leader_id = wait_for_leader(10s).get();

    wait_for_committed_offset(node(leader_id).raft()->dirty_offset(), 30s)
      .get();
};

TEST_F(
  manual_deletion_fixture, test_collected_log_recovery_admin_deletion_one) {
    prepare_raft_group();
    apply_retention_policy();

    // simulate admin deleting log folders. For more details look here:
    //
    // https://github.com/redpanda-data/redpanda/issues/321

    remove_data({model::node_id(1)});
    auto leader_id = wait_for_leader(10s).get();

    wait_for_committed_offset(node(leader_id).raft()->dirty_offset(), 30s)
      .get();
}
