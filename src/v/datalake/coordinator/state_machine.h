/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once
#include "cluster/state_machine_registry.h"
#include "datalake/coordinator/state.h"
#include "datalake/coordinator/types.h"
#include "raft/persisted_stm.h"

namespace datalake::coordinator {

class coordinator_stm final : public raft::persisted_stm<> {
public:
    static constexpr std::string_view name = "datalake_coordinator_stm";
    enum class errc {
        not_leader,
        apply_error,
        raft_error,
        shutting_down,
    };

    explicit coordinator_stm(ss::logger&, raft::consensus*);
    raft::consensus* raft() { return _raft; }

    // Syncs the STM such that we're guaranteed that it has applied all records
    // from the previous terms. Calling does _not_ ensure that all records from
    // the current term have been applied. But it does establish for new
    // leaders that they are up-to-date.
    //
    // Returns the current term.
    ss::future<checked<model::term_id, errc>>
    sync(model::timeout_clock::duration timeout);

    // Replicates the given batch and waits for it to finish replicating.
    // Success here does not guarantee that the replicated operation succeeded
    // in updating the STM -- only that the apply was attempted.
    ss::future<checked<std::nullopt_t, errc>> replicate_and_wait(
      model::term_id, model::record_batch batch, ss::abort_source&);

    const topics_state& state() const { return state_; }

protected:
    ss::future<> do_apply(const model::record_batch&) override;

    model::offset max_collectible_offset() override;

    ss::future<>
    apply_local_snapshot(raft::stm_snapshot_header, iobuf&& bytes) override;

    ss::future<raft::stm_snapshot>
      take_local_snapshot(ssx::semaphore_units) override;

    ss::future<> apply_raft_snapshot(const iobuf&) final;

    ss::future<iobuf> take_snapshot(model::offset) final;

private:
    // The deterministic state managed by this STM.
    topics_state state_;
};
class stm_factory : public cluster::state_machine_factory {
public:
    stm_factory() = default;
    bool is_applicable_for(const storage::ntp_config&) const final;
    void create(raft::state_machine_manager_builder&, raft::consensus*) final;
};
} // namespace datalake::coordinator
