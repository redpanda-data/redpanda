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

#include "cloud_topics/dl_stm/command_builder.h"
#include "cloud_topics/dl_stm/commands.h"
#include "cloud_topics/dl_stm/dl_stm_state.h"
#include "cloud_topics/types.h"
#include "cluster/state_machine_registry.h"
#include "model/fundamental.h"
#include "model/timestamp.h"
#include "raft/persisted_stm.h"
#include "raft/state_machine_manager.h"

#include <seastar/core/abort_source.hh>

#include <absl/container/btree_map.h>

namespace experimental::cloud_topics {

class dl_stm final : public raft::persisted_stm<> {
public:
    static constexpr const char* name = "dl_stm";
    friend class dl_stm_command_builder;

    dl_stm(ss::logger&, raft::consensus*);

    ss::future<> stop() override;
    model::offset max_collectible_offset() override;

    /// Find overlays by offset
    std::optional<dl_overlay> lower_bound(kafka::offset o) const noexcept;
    std::optional<kafka::offset>
    get_term_last_offset(model::term_id t) const noexcept;
    kafka::offset get_last_reconciled() const noexcept;

    ss::future<bool> replicate(model::term_id term, command_builder&& builder);

private:
    ss::future<bool> replicate(model::term_id term, model::record_batch rb);

    /// Apply record batches
    ss::future<> do_apply(const model::record_batch& batch) override;

    ss::future<> apply_raft_snapshot(const iobuf&) override;

    ss::future<>
    apply_local_snapshot(raft::stm_snapshot_header, iobuf&&) override;
    ss::future<raft::stm_snapshot>
    take_local_snapshot(ssx::semaphore_units u) override;
    ss::future<iobuf> take_snapshot(model::offset) override;

    ss::gate _gate;
    ss::abort_source _as;
    dl_stm_state _state;
};

class dl_stm_factory : public cluster::state_machine_factory {
public:
    dl_stm_factory() = default;

    bool is_applicable_for(const storage::ntp_config& ntp_cfg) const final;

    bool is_shadow_topic(const storage::ntp_config& ntp_cfg) const;

    void create(
      raft::state_machine_manager_builder& builder,
      raft::consensus* raft) final;
};

} // namespace experimental::cloud_topics
