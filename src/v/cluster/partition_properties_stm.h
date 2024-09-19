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
#include "container/fragmented_vector.h"
#include "model/fundamental.h"
#include "raft/persisted_stm.h"
#include "serde/envelope.h"

namespace cluster {

struct partition_properties_stm_accessor;
class partition_properties_stm
  : public raft::persisted_stm<raft::kvstore_backed_stm_snapshot> {
    friend struct partition_properties_stm_accessor;

public:
    using writes_disabled = ss::bool_class<struct writes_disabled_tag>;
    static constexpr std::string_view name = "partition_properties_stm";

    partition_properties_stm(
      raft::consensus* raft,
      ss::logger& logger,
      storage::kvstore& kvstore,
      config::binding<std::chrono::milliseconds> sync_timeout);

    ss::future<iobuf> take_snapshot(model::offset) final;

    // Updates partition properties to disable writes
    ss::future<std::error_code> disable_writes();
    // Updates partition properties to enable writes
    ss::future<std::error_code> enable_writes();
    // Waits for the state to be up to date and returns an up to date state of
    // write disabled property, this method may return an error and is only
    // intended to be called on the current leader.
    ss::future<result<writes_disabled>> sync_writes_disabled();
    // Returns a current value of the writes disabled property.
    writes_disabled are_writes_disabled() const;

protected:
    ss::future<>
    apply_local_snapshot(raft::stm_snapshot_header, iobuf&&) override;

    ss::future<raft::stm_snapshot>
    take_local_snapshot(ssx::semaphore_units apply_units) override;

private:
    struct update_writes_disabled_cmd
      : serde::envelope<
          update_writes_disabled_cmd,
          serde::version<0>,
          serde::compat_version<0>> {
        writes_disabled writes_disabled = writes_disabled::no;

        auto serde_fields() { return std::tie(writes_disabled); }
        friend bool operator==(
          const update_writes_disabled_cmd&, const update_writes_disabled_cmd&)
          = default;
    };

    struct state_snapshot
      : serde::envelope<
          state_snapshot,
          serde::version<0>,
          serde::compat_version<0>> {
        writes_disabled writes_disabled;
        model::offset update_offset;

        auto serde_fields() { return std::tie(writes_disabled, update_offset); }
        friend bool operator==(const state_snapshot&, const state_snapshot&)
          = default;
    };

    struct raft_snapshot
      : serde::
          envelope<raft_snapshot, serde::version<0>, serde::compat_version<0>> {
        writes_disabled writes_disabled = writes_disabled::no;

        auto serde_fields() { return std::tie(writes_disabled); }

        friend bool operator==(const raft_snapshot&, const raft_snapshot&)
          = default;
    };

    struct local_snapshot
      : serde::envelope<
          local_snapshot,
          serde::version<0>,
          serde::compat_version<0>> {
        chunked_vector<state_snapshot> state_updates;

        auto serde_fields() { return std::tie(state_updates); }
        friend bool
        operator==(const local_snapshot& lhs, const local_snapshot& rhs) {
            return std::equal(
              lhs.state_updates.begin(),
              lhs.state_updates.end(),
              rhs.state_updates.begin());
        }
    };

    friend std::ostream& operator<<(std::ostream&, const raft_snapshot&);
    friend std::ostream&
    operator<<(std::ostream&, const update_writes_disabled_cmd&);
    friend std::ostream& operator<<(std::ostream&, const local_snapshot&);
    friend std::ostream& operator<<(std::ostream&, const state_snapshot&);

    enum class operation_type {
        update_writes_disabled = 0,
    };

    ss::future<> do_apply(const model::record_batch&) final;
    ss::future<> apply_raft_snapshot(const iobuf&) final;
    void apply_record(model::record, model::offset);

    static model::record_batch
      make_update_partitions_batch(update_writes_disabled_cmd);

    ss::future<std::error_code> replicate_properties_update(
      model::timeout_clock::duration timeout,
      update_writes_disabled_cmd command);

    config::binding<std::chrono::milliseconds> _sync_timeout;

    // an optimization for taking snapshots at arbitrary offsets, the map
    // contains a snapshot of properties every time they change, this
    // structure is intended to be very small as the partition properties
    // are intermittent events, the updates are ordered by offsets and they are
    // cleaned when the local snapshot is taken.
    chunked_vector<state_snapshot> _state_snapshots;
};

class partition_properties_stm_factory : public state_machine_factory {
public:
    partition_properties_stm_factory(
      storage::kvstore& kvstore,
      config::binding<std::chrono::milliseconds> sync_timeout);

    bool is_applicable_for(const storage::ntp_config& cfg) const final;

    void create(
      raft::state_machine_manager_builder& builder,
      raft::consensus* raft) final;

private:
    storage::kvstore& _kvstore;
    config::binding<std::chrono::milliseconds> _sync_timeout;
};

} // namespace cluster
