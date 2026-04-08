/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "cloud_topics/level_one/metastore/domain_uuid.h"
#include "cluster/state_machine_registry.h"
#include "lsm/lsm.h"
#include "model/fundamental.h"
#include "raft/persisted_stm.h"
#include "serde/envelope.h"
#include "utils/detailed_error.h"

namespace cloud_topics::read_replica {

// State replicated through the STM to allow callers to serve the Kafka API.
struct state
  : public serde::envelope<state, serde::version<0>, serde::compat_version<0>> {
    // The domain UUID for the domain that hosts this partition in the source
    // cluster.
    l1::domain_uuid domain{};

    // The seqno from the source database at which we last synced. The fields
    // that are used to serve the synchronous bits of the Kafka protocol were
    // read at this seqno. This can be used to ensure that asynchronous bits of
    // the Kafka protocol are read at or later than this seqno.
    std::optional<lsm::sequence_number> seqno{std::nullopt};

    // The start offset of the partition (inclusive).
    kafka::offset start_offset{0};

    // One past the last offset.
    // NOTE: this is the high watermark.
    kafka::offset next_offset{0};

    // The term corresponding to next_offset on the source cluster. This is NOT
    // the term of this STM's Raft group.
    model::term_id latest_term{0};

    auto serde_fields() {
        return std::tie(domain, seqno, start_offset, next_offset, latest_term);
    }

    fmt::iterator format_to(fmt::iterator it) const;

    friend bool operator==(const state&, const state&) = default;
};

enum class update_key : uint8_t {
    update_metadata = 0,
};

struct update_metadata_update
  : public serde::envelope<
      update_metadata_update,
      serde::version<0>,
      serde::compat_version<0>> {
    l1::domain_uuid domain;
    std::optional<lsm::sequence_number> seqno;

    kafka::offset start_offset;
    kafka::offset next_offset;
    model::term_id latest_term;

    auto serde_fields() {
        return std::tie(domain, seqno, start_offset, next_offset, latest_term);
    }

    fmt::iterator format_to(fmt::iterator it) const;

    friend bool operator==(
      const update_metadata_update&, const update_metadata_update&) = default;

    bool can_apply(const state&) const;
};

struct stm_snapshot
  : public serde::
      envelope<stm_snapshot, serde::version<0>, serde::compat_version<0>> {
    state st;
    auto serde_fields() { return std::tie(st); }
};

using stm_base = raft::persisted_stm_no_snapshot_at_offset<>;

// A replicated state machine that tracks metadata for a read replica partition.
// The metadata is synchronized from the source cluster's cloud database by the
// leader and replicated to followers through Raft.
class stm final : public stm_base {
public:
    static constexpr std::string_view name = "read_replica_stm";

    enum class errc {
        // This replica is not the leader.
        not_leader,
        // An error occurred during Raft operations.
        raft_error,
        // The system is shutting down.
        shutting_down,
    };
    using error = detailed_error<errc>;

    explicit stm(ss::logger&, raft::consensus*);

    raft::consensus* raft() { return _raft; }

    // Syncs the STM such that we're guaranteed that it has applied all records
    // from previous terms. This establishes for new leaders that they are
    // up-to-date with the state from prior leaders.
    //
    // Returns the current term on success.
    ss::future<std::expected<model::term_id, error>> sync(
      model::timeout_clock::duration timeout,
      std::optional<std::reference_wrapper<ss::abort_source>> as
      = std::nullopt);

    // Accessors for current state.
    const state& get_state() const { return state_; }
    l1::domain_uuid domain() const { return state_.domain; }
    bool has_domain() const { return state_.domain != l1::domain_uuid{}; }

    raft::stm_initial_recovery_policy
    get_initial_recovery_policy() const final {
        return raft::stm_initial_recovery_policy::read_everything;
    }

    /// Serializes the update into a record batch and replicates it through
    /// Raft. Waits for the batch to be applied before returning. The update
    /// is applied only if can_apply() passes during do_apply(); callers
    /// should check can_apply() beforehand to avoid unnecessary replication.
    ss::future<std::expected<model::offset, error>>
    update(model::term_id term, update_metadata_update, ss::abort_source& as);

protected:
    ss::future<> do_apply(const model::record_batch&) override;

    ss::future<raft::local_snapshot_applied>
    apply_local_snapshot(raft::stm_snapshot_header, iobuf&& bytes) override;

    ss::future<raft::stm_snapshot>
      take_local_snapshot(ssx::semaphore_units) override;

    ss::future<> apply_raft_snapshot(const iobuf&) final;

    ss::future<iobuf> take_raft_snapshot() final;

private:
    // Replicates the given batch and waits for it to finish applying.
    // Success indicates the batch was replicated and applied, but does not
    // guarantee the update semantically succeeded (e.g., if it was rejected
    // due to stale data).
    ss::future<std::expected<model::offset, error>> replicate_and_wait(
      model::term_id term, model::record_batch batch, ss::abort_source& as);

    // The deterministic state managed by this STM.
    state state_;
};

// Factory for creating read_replica STMs.
class stm_factory : public cluster::state_machine_factory {
public:
    stm_factory() = default;
    bool is_applicable_for(const storage::ntp_config&) const final;
    void create(
      raft::state_machine_manager_builder&,
      raft::consensus*,
      const cluster::stm_instance_config&) final;
};

} // namespace cloud_topics::read_replica

template<>
struct fmt::formatter<cloud_topics::read_replica::stm::errc> final
  : public fmt::formatter<std::string_view> {
    fmt::appender format(
      const cloud_topics::read_replica::stm::errc& e,
      fmt::format_context& ctx) const;
};

template<>
struct fmt::formatter<cloud_topics::read_replica::update_key> final
  : public fmt::formatter<std::string_view> {
    fmt::appender format(
      const cloud_topics::read_replica::update_key& k,
      fmt::format_context& ctx) const;
};
