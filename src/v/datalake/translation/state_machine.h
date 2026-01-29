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
#include "model/fundamental.h"
#include "model/timestamp.h"
#include "raft/persisted_stm.h"

namespace datalake::translation {

/// Tracks the progress of datalake translation and clamps the collectible
/// offset to ensure the data is not GC-ed before translation is finished.
class translation_stm final : public raft::persisted_stm<> {
public:
    static constexpr std::string_view name = "datalake_translation_stm";
    using base = raft::persisted_stm<>;

    explicit translation_stm(ss::logger&, raft::consensus*);

    ss::future<> stop() override;

    ss::future<> do_apply(const model::record_batch&) override;
    model::offset max_removable_local_log_offset() override;
    std::optional<kafka::offset> lowest_pinned_data_offset() const override;
    ss::future<raft::local_snapshot_applied>
    apply_local_snapshot(raft::stm_snapshot_header, iobuf&& bytes) override;

    ss::future<raft::stm_snapshot>
      take_local_snapshot(ssx::semaphore_units) override;

    ss::future<> apply_raft_snapshot(const iobuf&) final;
    ss::future<iobuf> take_raft_snapshot(model::offset) final;

    raft::consensus* raft() const { return _raft; }

    // wait until at least `offset` is translated
    ss::future<> wait_translated(
      model::offset offset,
      model::timeout_clock::time_point,
      std::optional<std::reference_wrapper<ss::abort_source>> as
      = std::nullopt);

    ss::future<std::optional<kafka::offset>>
    highest_translated_offset(model::timeout_clock::duration timeout);

    /**
     * last_translated_timestamp - Approximate timestamp of the last record
     * translated. Can be used to approximate translation lag. By design,
     * should underestimate the system time at which highest_translated_offset
     * became available for translation. This allows us to *overestimate* lag.
     */
    ss::future<std::optional<model::timestamp>>
    last_translated_timestamp(model::timeout_clock::duration timeout);

    /**
     * Cached versions of highest_translated_offset method. These methods will
     * not call state machine sync, risking to have the stale value if the
     * leadership changed.
     */
    kafka::offset cached_highest_translated_offset() const {
        return _highest_translated_offset;
    }

    /**
     * Cached versions of last_translated_timestamp method. These methods will
     * not call state machine sync, risking to have the stale value if the
     * leadership changed.
     */
    std::optional<model::timestamp> cached_last_translated_timestamp() const {
        return _last_translated_timestamp;
    }

    ss::future<std::error_code> reset_highest_translated_offset(
      kafka::offset new_translated_offset,
      std::optional<model::timestamp> new_translated_timestamp,
      model::term_id term,
      model::timeout_clock::duration timeout,
      ss::abort_source&);

    raft::stm_initial_recovery_policy
    get_initial_recovery_policy() const final {
        return raft::stm_initial_recovery_policy::skip_to_end;
    }

private:
    struct snapshot
      : serde::envelope<snapshot, serde::version<1>, serde::compat_version<0>> {
        kafka::offset highest_translated_offset;
        std::optional<model::timestamp> last_translated_timestamp;
        auto serde_fields() {
            return std::tie(
              highest_translated_offset, last_translated_timestamp);
        }
    };

    ss::future<> wait_translated(
      kafka::offset,
      model::timeout_clock::time_point,
      std::optional<std::reference_wrapper<ss::abort_source>> as
      = std::nullopt);

    void update_highest_translated_offset(kafka::offset new_offset);

    // The offset one below the starting point of the next translation.
    // When this is kafka::offset::min() (the default), this indicates that it
    // is not initialized.
    kafka::offset _highest_translated_offset{};

    // approximate system time at which _highest_translated_offset became
    // available for translation.
    std::optional<model::timestamp> _last_translated_timestamp{};
    raft::offset_monitor<kafka::offset> _waiters_for_translated;
};

class stm_factory : public cluster::state_machine_factory {
public:
    explicit stm_factory(bool is_iceberg_enabled);
    bool is_applicable_for(const storage::ntp_config&) const final;
    void create(
      raft::state_machine_manager_builder&,
      raft::consensus*,
      const cluster::stm_instance_config&) final;

private:
    bool _iceberg_enabled;
};

} // namespace datalake::translation
