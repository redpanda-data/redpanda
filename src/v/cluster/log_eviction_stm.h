/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "base/outcome.h"
#include "base/seastarx.h"
#include "cluster/state_machine_registry.h"
#include "model/fundamental.h"
#include "raft/fwd.h"
#include "raft/persisted_stm.h"
#include "storage/kvstore.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/queue.hh>
#include <seastar/util/log.hh>

namespace cluster {

class consensus;

/**
 * Responsible for taking snapshots triggered by underlying log segments
 * eviction.
 *
 * The process goes like this: storage layer will send a "deletion notification"
 * - a request to evict log up to a certain offset. log_eviction_stm will then
 * adjust that offset with _stm_manager->max_collectible_offset(), write the
 * raft snapshot and notify the storage layer that log eviction can safely
 * proceed up to the adjusted offset.
 *
 * This class also initiates and responds to delete-records events. Call
 * truncate() pushes a special prefix_truncate batch onto the log for which this
 * stm will be searching for. Upon processing of this record a new snapshot will
 * be written which may also trigger deletion of data on disk.
 */
class log_eviction_stm
  : public raft::persisted_stm<raft::kvstore_backed_stm_snapshot> {
public:
    static constexpr std::string_view name = "log_eviction_stm";

    using offset_result = result<model::offset, std::error_code>;
    log_eviction_stm(raft::consensus*, ss::logger&, storage::kvstore&);

    ss::future<> start() override;

    ss::future<> stop() override;

    /// Truncate local log
    ///
    /// This method doesn't immediately delete the entries of the log below the
    /// requested offset but pushes a special record batch onto the log which
    /// when read by brokers, will invoke a routine that will perform the
    /// deletion
    ///
    /// Returns the offset at which the operation was replicated.
    ss::future<offset_result> truncate(
      model::offset rp_start_offset,
      kafka::offset kafka_start_offset,
      ss::lowres_clock::time_point deadline,
      std::optional<std::reference_wrapper<ss::abort_source>> as
      = std::nullopt);

    /// Return the offset up to which the storage layer would like to
    /// prefix truncate the log, if any.
    std::optional<model::offset> eviction_requested_offset() const {
        const auto requested_eviction_offset = std::max(
          _delete_records_eviction_offset, _storage_eviction_offset);
        if (!requested_eviction_offset) {
            return std::nullopt;
        }
        return requested_eviction_offset;
    }

    /// This class drives eviction, it works differently then other stms in this
    /// regard
    ///
    /// Override to ensure it never unnecessarily waits
    ss::future<> ensure_local_snapshot_exists(model::offset) override;

    /// The actual start offset of the log with the delta factored in
    model::offset effective_start_offset() const;

    /// Ensure followers have processed up until the most recent known version
    /// of the batch representing the start offset
    /// This only returns the start override, if one exists. It does not take
    /// into account local storage, and may not even point to an offset that
    /// exists in local storage (e.g. if we have locally truncated).
    ///
    /// If `kafka::offset{}` is returned and archival storage is enabled for the
    /// given ntp then the caller should fall back on the archival stm to check
    /// if a start offset override exists and if so what its value is.
    ss::future<result<kafka::offset, std::error_code>>
    sync_kafka_start_offset_override(model::timeout_clock::duration timeout);

    /// If `kafka::offset{}` is returned and archival storage is enabled for the
    /// given ntp then the caller should fall back on the archival stm to check
    /// if a start offset override exists and if so what its value is.
    kafka::offset kafka_start_offset_override();

    ss::future<iobuf> take_snapshot(model::offset) final { co_return iobuf{}; }

protected:
    ss::future<>
    apply_local_snapshot(raft::stm_snapshot_header, iobuf&&) override;

    ss::future<raft::stm_snapshot>
    take_local_snapshot(ssx::semaphore_units apply_units) override;

    virtual ss::future<model::offset> storage_eviction_event();

private:
    using base_t = raft::persisted_stm<raft::kvstore_backed_stm_snapshot>;
    void increment_start_offset(model::offset);
    bool should_process_evict(model::offset);

    ss::future<> monitor_log_eviction();
    ss::future<> do_write_raft_snapshot(model::offset);
    ss::future<> handle_log_eviction_events();
    ss::future<> do_apply(const model::record_batch&) final;
    ss::future<> apply_raft_snapshot(const iobuf&) final;

    ss::future<offset_result> replicate_command(
      model::record_batch batch,
      ss::lowres_clock::time_point deadline,
      std::optional<std::reference_wrapper<ss::abort_source>> as);

private:
    ss::abort_source _as;

    // Offset we are able to truncate based on local retention policy, as
    // signaled by the storage layer. This value is not maintained via the
    // persisted_stm and may be different across replicas.
    model::offset _storage_eviction_offset;

    // Offset corresponding to a delete-records request from the user. This
    // value is maintained via the persisted_stm and is identical on every
    // replica.
    model::offset _delete_records_eviction_offset;

    // Should be signaled every time either of the above offsets are updated.
    ss::condition_variable _has_pending_truncation;

    // Kafka offset of the last `prefix_truncate_record` applied to this stm.
    kafka::offset _cached_kafka_start_offset_override;
};

class log_eviction_stm_factory : public state_machine_factory {
public:
    explicit log_eviction_stm_factory(storage::kvstore&);

    bool is_applicable_for(const storage::ntp_config& cfg) const final;

    void create(
      raft::state_machine_manager_builder& builder,
      raft::consensus* raft) final;

private:
    storage::kvstore& _kvstore;
};

} // namespace cluster
