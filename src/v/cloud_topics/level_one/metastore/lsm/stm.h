/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "cloud_topics/level_one/metastore/lsm/state.h"
#include "cluster/state_machine_registry.h"
#include "model/fundamental.h"
#include "raft/persisted_stm.h"

namespace cloud_topics::l1 {

using metastore_stm_base = raft::persisted_stm_no_snapshot_at_offset<>;

// A replicated state machine that backs an LSM database on object storage.
// This state machine applies optimistic concurrency control to ensure the
// updates go to proper database. Any additional concurrency control (e.g.
// locking to ensure application integrity) is left to callers.
class stm final : public metastore_stm_base {
public:
    static constexpr std::string_view name = "l1_lsm_stm";
    enum class errc {
        not_leader,
        raft_error,
        shutting_down,
    };

    explicit stm(
      ss::logger&, raft::consensus*, config::binding<std::chrono::seconds>);
    raft::consensus* raft() { return _raft; }

    // Syncs the STM such that we're guaranteed that it has applied all records
    // from the previous terms. Calling does _not_ ensure that all records from
    // the current term have been applied. But it does establish for new
    // leaders that they are up-to-date.
    //
    // Returns the current term.
    ss::future<std::expected<model::term_id, errc>> sync(
      model::timeout_clock::duration timeout,
      std::optional<std::reference_wrapper<ss::abort_source>> as
      = std::nullopt);

    // Replicates the given batch and waits for it to finish replicating.
    // Success here does not guarantee that the replicated operation succeeded
    // in updating the STM -- only that the apply was attempted.
    ss::future<std::expected<model::offset, errc>> replicate_and_wait(
      model::term_id, model::record_batch batch, ss::abort_source&);

    const lsm_state& state() const { return state_; }
    lsm_state& mutable_state() { return state_; }

    raft::stm_initial_recovery_policy
    get_initial_recovery_policy() const final {
        return raft::stm_initial_recovery_policy::read_everything;
    }

protected:
    ss::future<> stop() override;

    ss::future<lsm_stm_snapshot> make_snapshot();

    ss::future<> do_apply(const model::record_batch&) override;

    model::offset max_removable_local_log_offset() override;

    ss::future<raft::local_snapshot_applied>
    apply_local_snapshot(raft::stm_snapshot_header, iobuf&& bytes) override;

    ss::future<raft::stm_snapshot>
      take_local_snapshot(ssx::semaphore_units) override;

    ss::future<> apply_raft_snapshot(const iobuf&) final;

    ss::future<iobuf> take_raft_snapshot() final;

private:
    void rearm_snapshot_timer();
    void write_snapshot_async();
    ss::future<> maybe_write_snapshot();

    // The deterministic state managed by this STM.
    lsm_state state_;
    config::binding<std::chrono::seconds> snapshot_delay_secs_;
    ss::timer<ss::lowres_clock> snapshot_timer_;
};

class lsm_stm_factory : public cluster::state_machine_factory {
public:
    lsm_stm_factory() = default;
    bool is_applicable_for(const storage::ntp_config&) const final;
    void create(
      raft::state_machine_manager_builder&,
      raft::consensus*,
      const cluster::stm_instance_config&) final;
};

} // namespace cloud_topics::l1
