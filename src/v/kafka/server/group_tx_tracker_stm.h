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
#include "kafka/server/group_data_parser.h"
#include "raft/persisted_stm.h"
#include "serde/rw/envelope.h"
#include "serde/rw/map.h"
#include "serde/rw/set.h"

namespace kafka {

class group_tx_tracker_stm final
  : public raft::persisted_stm<>
  , public group_data_parser<group_tx_tracker_stm> {
public:
    static constexpr std::string_view name = "group_tx_tracker_stm";

    group_tx_tracker_stm(ss::logger&, raft::consensus*);

    storage::stm_type type() override {
        return storage::stm_type::consumer_offsets_transactional;
    }

    ss::future<fragmented_vector<model::tx_range>>
    aborted_tx_ranges(model::offset, model::offset) override {
        // Instead of tracking aborted transactions, group partitions rely on a
        // different approach. When a group transaction is committed, the data
        // to be committed is converted into regular offset data batches. This
        // conversion happens atomically along with writing a commit marker.
        // This eliminates the need to track completed transactional batches and
        // they are unconditionally omitted in the compaction pass.
        return ss::make_ready_future<fragmented_vector<model::tx_range>>();
    }

    ss::future<> do_apply(const model::record_batch&) override;

    model::offset max_collectible_offset() override;

    ss::future<>
    apply_local_snapshot(raft::stm_snapshot_header, iobuf&& bytes) override;

    ss::future<raft::stm_snapshot>
      take_local_snapshot(ssx::semaphore_units) override;

    ss::future<> apply_raft_snapshot(const iobuf&) final;
    ss::future<iobuf> take_snapshot(model::offset) final;

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

private:
    struct per_group_state
      : serde::envelope<
          per_group_state,
          serde::version<0>,
          serde::compat_version<0>> {
        per_group_state() = default;

        per_group_state(model::producer_identity pid, model::offset offset) {
            maybe_add_tx_begin(pid, offset);
        }

        void
        maybe_add_tx_begin(model::producer_identity pid, model::offset offset);

        absl::btree_set<model::offset> begin_offsets;

        absl::btree_map<model::producer_identity, model::offset>
          producer_to_begin;

        auto serde_fields() {
            return std::tie(begin_offsets, producer_to_begin);
        }
    };
    using all_txs_t = absl::btree_map<kafka::group_id, per_group_state>;
    struct snapshot
      : serde::envelope<snapshot, serde::version<0>, serde::compat_version<0>> {
        all_txs_t transactions;

        auto serde_fields() { return std::tie(transactions); }
    };

    void maybe_add_tx_begin_offset(
      kafka::group_id, model::producer_identity, model::offset);

    void maybe_end_tx(kafka::group_id, model::producer_identity);

    all_txs_t _all_txs;
};

class group_tx_tracker_stm_factory : public cluster::state_machine_factory {
public:
    group_tx_tracker_stm_factory() = default;
    bool is_applicable_for(const storage::ntp_config&) const final;
    void create(raft::state_machine_manager_builder&, raft::consensus*) final;
};

} // namespace kafka
