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
#include "datalake/coordinator/types.h"
#include "raft/persisted_stm.h"

namespace datalake::coordinator {

class coordinator_stm final : public raft::persisted_stm<> {
public:
    static constexpr std::string_view name = "datalake_coordinator_stm";

    explicit coordinator_stm(ss::logger&, raft::consensus*);

    ss::future<> do_apply(const model::record_batch&) override;

    model::offset max_collectible_offset() override;

    ss::future<>
    apply_local_snapshot(raft::stm_snapshot_header, iobuf&& bytes) override;

    ss::future<raft::stm_snapshot>
      take_local_snapshot(ssx::semaphore_units) override;

    ss::future<> apply_raft_snapshot(const iobuf&) final;

    ss::future<iobuf> take_snapshot(model::offset) final;

    ss::future<add_translated_data_files_reply>
      add_translated_data_file(add_translated_data_files_request);

    ss::future<fetch_latest_data_file_reply>
      fetch_latest_data_file(fetch_latest_data_file_request);

private:
};
class stm_factory : public cluster::state_machine_factory {
public:
    stm_factory() = default;
    bool is_applicable_for(const storage::ntp_config&) const final;
    void create(raft::state_machine_manager_builder&, raft::consensus*) final;
};
} // namespace datalake::coordinator
