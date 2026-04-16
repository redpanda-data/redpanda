/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "cluster/state_machine_registry.h"
#include "container/chunked_hash_map.h"
#include "container/chunked_vector.h"
#include "kafka/server/group_data_parser.h"
#include "raft/persisted_stm.h"
#include "serde/rw/envelope.h"
#include "serde/rw/map.h"
#include "serde/rw/pair.h"
#include "serde/rw/set.h"

namespace kafka {

class group_tx_tracker_stm final
  : public raft::persisted_stm<>
  , public group_data_parser<group_tx_tracker_stm> {
public:
    static constexpr std::string_view name = "group_tx_tracker_stm";
    struct producer_tx_state
      : serde::envelope<
          producer_tx_state,
          serde::version<0>,
          serde::compat_version<0>> {
        model::record_batch_type fence_type;
        model::offset begin_offset;
        model::timestamp batch_ts;
        model::timeout_clock::duration timeout;

        bool expired_deprecated_fence_tx() const;

        auto serde_fields() {
            return std::tie(fence_type, begin_offset, batch_ts, timeout);
        }
    };
    struct per_group_state
      : serde::envelope<
          per_group_state,
          serde::version<1>,
          serde::compat_version<0>> {
        per_group_state() = default;

        void maybe_add_tx_begin(
          const kafka::group_id&,
          model::record_batch_type fence_type,
          model::producer_identity pid,
          model::offset offset,
          model::timestamp begin_ts,
          model::timeout_clock::duration tx_timeout);

        absl::btree_set<model::offset> begin_offsets;

        // deprecated
        absl::btree_map<model::producer_identity, model::offset>
          producer_to_begin_deprecated;

        absl::btree_map<model::producer_identity, producer_tx_state>
          producer_states;

        void gc_expired_tx_fence_transactions();

        auto serde_fields() {
            return std::tie(
              begin_offsets, producer_to_begin_deprecated, producer_states);
        }
    };
    using all_txs_t = absl::btree_map<kafka::group_id, per_group_state>;

    group_tx_tracker_stm(
      ss::logger&, raft::consensus*, ss::sharded<features::feature_table>&);

    storage::stm_type type() override {
        return storage::stm_type::consumer_offsets_transactional;
    }

    ss::future<chunked_vector<model::tx_range>>
    aborted_tx_ranges(model::offset, model::offset) override {
        // Instead of tracking aborted transactions, group partitions rely on a
        // different approach. When a group transaction is committed, the data
        // to be committed is converted into regular offset data batches. This
        // conversion happens atomically along with writing a commit marker.
        // This eliminates the need to track completed transactional batches and
        // they are unconditionally omitted in the compaction pass.
        return ss::make_ready_future<chunked_vector<model::tx_range>>();
    }

    ss::future<> do_apply(const model::record_batch&) override;

    model::offset max_removable_local_log_offset() override;

    ss::future<raft::local_snapshot_applied>
    apply_local_snapshot(raft::stm_snapshot_header, iobuf&& bytes) override;

    ss::future<raft::stm_snapshot>
      take_local_snapshot(ssx::semaphore_units) override;

    ss::future<> apply_raft_snapshot(const iobuf&) final;
    ss::future<iobuf> take_raft_snapshot(model::offset) final;

    ss::future<> handle_raft_data(model::record_batch);
    ss::future<> handle_tx_offsets(
      model::record_batch_header, kafka::group_tx::offsets_metadata);
    ss::future<> handle_fence_v0(
      model::record_batch_header, kafka::group_tx::fence_metadata_v0);
    ss::future<> handle_fence_v1(
      model::record_batch_header, kafka::group_tx::fence_metadata_v1);
    ss::future<>
      handle_fence(model::record_batch_header, kafka::group_tx::fence_metadata);
    ss::future<>
      handle_abort(model::record_batch_header, kafka::group_tx::abort_metadata);
    ss::future<> handle_commit(
      model::record_batch_header, kafka::group_tx::commit_metadata);
    ss::future<> handle_version_fence(features::feature_table::version_fence);
    void handle_group_block(kafka::group_block gb);
    group_block_info_map& group_blocks();
    const group_block_info_map& group_blocks() const;

    ss::future<> stop() final;

    const all_txs_t& inflight_transactions() const { return _all_txs; }

    raft::stm_initial_recovery_policy
    get_initial_recovery_policy() const final {
        return raft::stm_initial_recovery_policy::read_everything;
    }

private:
    static constexpr int8_t supported_local_snapshot_version = 2;
    struct snapshot
      : serde::envelope<snapshot, serde::version<2>, serde::compat_version<0>> {
        all_txs_t transactions;

        // legacy for version 1 RP to decode version 2+ snapshots
        chunked_vector<kafka::group_id> blocked_groups;

        chunked_vector<group_block_info_map::value_type> group_blocks;

        auto serde_fields() {
            return std::tie(transactions, blocked_groups, group_blocks);
        }

        friend bool operator==(const snapshot&, const snapshot&) = default;
    };

    void handle_group_metadata(group_metadata_kv);

    ss::future<> gc_expired_tx_fence_transactions();

    void maybe_add_tx_begin_offset(
      model::record_batch_type fence_type,
      kafka::group_id,
      model::producer_identity,
      model::offset,
      model::timestamp begin_ts,
      model::timeout_clock::duration tx_timeout);

    void maybe_end_tx(kafka::group_id, model::producer_identity, model::offset);

    all_txs_t _all_txs;
    group_block_info_map _group_blocks;

    ss::sharded<features::feature_table>& _feature_table;
    ss::abort_source _as;
    static constexpr ss::lowres_clock::duration tx_fence_gc_frequency{1h};
    ss::timer<ss::lowres_clock> _stale_tx_fence_gc_timer;
};

class group_tx_tracker_stm_factory : public cluster::state_machine_factory {
public:
    explicit group_tx_tracker_stm_factory(
      ss::sharded<features::feature_table>&);
    bool is_applicable_for(const storage::ntp_config&) const final;
    void create(
      raft::state_machine_manager_builder&,
      raft::consensus*,
      const cluster::stm_instance_config&) final;

private:
    ss::sharded<features::feature_table>& _feature_table;
};

} // namespace kafka
