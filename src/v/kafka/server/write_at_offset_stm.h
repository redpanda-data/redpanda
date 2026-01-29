/*
 * Copyright 2025 Redpanda Data, Inc.
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
#include "raft/persisted_stm.h"
#include "raft/replicate.h"

namespace kafka {

/**
 * The write at offset state machine allows caller to replicate the record batch
 * at a specific Kafka offset. The replication is only successful if the batch
 * offset after replication matches the expected offset. Otherwise the
 * `replicate` call will return an error. In case of an error it is the caller
 * responsibility to reconcile with the log and retry replication of the batch
 * with an expected offset.
 */
class write_at_offset_stm
  : public raft::persisted_stm<raft::kvstore_backed_stm_snapshot> {
public:
    static constexpr std::string_view name = "write_at_offset";
    /**
     * State machine specific error codes.
     */
    enum class errc : int8_t {
        success = 0,
        invalid_offset,
        replicate_exception,
        invalid_batch_type,
        invalid_input,
        not_leader,
        invalid_truncation_offset,
    };
    struct errc_category final : public std::error_category {
        const char* name() const noexcept final;

        std::string message(int c) const final;
    };

    const std::error_category& error_category() noexcept;

    std::error_code make_error_code(errc e) noexcept;

    write_at_offset_stm(
      raft::consensus* raft,
      ss::logger& logger,
      storage::kvstore& kvstore,
      std::vector<model::record_batch_type> offset_translated_batches);
    /**
     * Method that replicates a vector of record batches only if the
     * kafka::offset of each batch is going to be exactly the expected
     * offset.
     *
     * The method supports replicating gaps in the logs, in this case the
     * prev_log_offset parameter must be present. If the `prev_log_offset` is
     * not present it is equivalent for it to be equal to (first
     * expected_base_offset
     * - 1).
     *
     * If prev_log_offset is present the state machine will match the expected
     * prev_log_offset with the tracked offset and fill any gaps between
     * consecutive batches and within the provided batch sequence.
     * Gaps are filled between (prev_log_offset, first_expected_base_offset)
     * and between any non-consecutive expected offsets in the batch vector.
     *
     * Example:
     *
     * The original log contains the following batches
     * (denoted as [base_offset, end_offset])
     *
     *
     * [0,4][gap][10,13][gap][20,22]
     *
     * In order to replicate the sequence the following replicate call
     * is required:
     *
     * replicate(
     *   batches=[[0, 4], [10, 13], [20, 22]],
     *   expected_base_offsets=[0, 10, 20],
     *   prev_log_offset=std::nullopt
     * )
     *
     * This will replicate:
     * - [0, 4] at offset 0
     * - fill gap from 5-9
     * - [10, 13] at offset 10
     * - fill gap from 14-19
     * - [20, 22] at offset 20
     *
     * Alternatively, to replicate with a gap from a previous log position:
     *
     * replicate(
     *   batches=[[10, 13]],
     *   expected_base_offsets=[10],
     *   prev_log_offset=4
     * )
     *
     * This fills the gap [5, 9]  and replicates [10, 13] at offset 10.
     *
     * The method guarantees the replication in the call order. There can be
     * more than one replicate request in flight at the same time.
     */
    raft::replicate_stages replicate(
      chunked_vector<model::record_batch>,
      chunked_vector<kafka::offset> expected_base_offsets,
      std::optional<kafka::offset> prev_log_offset,
      model::timeout_clock::duration timeout,
      std::optional<std::reference_wrapper<ss::abort_source>> as
      = std::nullopt);

    /**
     * Ensures that the log is truncatable up to (inclusive) new_start_offset
     * - 1. This method will replicate any missing ghost batches to fill the gap
     * if necessary. Effective hwm after the method returns is atleast
     * new_start_offset, this guarantees everything before new_start_offset can
     * be safely truncated.
     */
    ss::future<errc> ensure_truncatable(
      kafka::offset new_start_offset,
      model::timeout_clock::duration timeout,
      std::optional<std::reference_wrapper<ss::abort_source>> as
      = std::nullopt);

    ss::future<iobuf> take_raft_snapshot(model::offset) final;
    ss::future<> apply_raft_snapshot(const iobuf&) final;

    raft::stm_initial_recovery_policy
    get_initial_recovery_policy() const final {
        return raft::stm_initial_recovery_policy::skip_to_end;
    }
    /**
     * Returns the effective last offset as perceived by the state machine. This
     * function calls `persisted_stm::sync()` to make sure the returned offset
     * is up to date.
     *
     * Callers are supposed to maintain the lifetime of the STM until the
     * returned future is ready.
     *
     * NOTE: the resulting offset can move backward if the inflight replicate
     * request fails
     *
     */
    ss::future<result<kafka::offset>>
    get_expected_last_offset(model::timeout_clock::duration sync_timeout);

private:
    friend class write_at_offset_stm_accessor;
    ss::future<> do_apply(const model::record_batch& b) final;

    ss::future<raft::local_snapshot_applied>
    apply_local_snapshot(raft::stm_snapshot_header, iobuf&&) final;

    ss::future<raft::stm_snapshot>
      take_local_snapshot(ssx::semaphore_units) final;
    bool is_offset_translated_batch(const model::record_batch& batch) const;

    ss::future<result<raft::replicate_result>> do_replicate(
      chunked_vector<model::record_batch>,
      chunked_vector<kafka::offset> expected_base_offsets,
      std::optional<kafka::offset> prev_log_offset,
      model::timeout_clock::duration timeout,
      std::optional<std::reference_wrapper<ss::abort_source>> as,
      ss::promise<> enqueued_promise);

    raft::replicate_stages try_replicate_in_stages(
      chunked_vector<model::record_batch>,
      std::optional<std::reference_wrapper<ss::abort_source>> as);

    kafka::offset expected_last_offset() const;

    bool inflight_last_offset_needs_reset(model::term_id insync_term) const {
        return _inflight_last_offset.has_value()
               && _inflight_last_offset->in_sync_term == insync_term;
    }

    std::vector<model::record_batch_type> _offset_translated_batches;
    kafka::offset _last_offset{};
    struct term_offset {
        kafka::offset offset;
        model::term_id in_sync_term;
    };
    friend std::ostream& operator<<(std::ostream&, const term_offset&);

    std::optional<term_offset> _inflight_last_offset;
    ssx::mutex _sync_lock;
};

class write_at_offset_stm_factory : public cluster::state_machine_factory {
public:
    write_at_offset_stm_factory(
      storage::kvstore& kvstore,
      std::vector<model::record_batch_type> offset_translated_batches);

    bool is_applicable_for(const storage::ntp_config& cfg) const final;

    void create(
      raft::state_machine_manager_builder& builder,
      raft::consensus* raft,
      const cluster::stm_instance_config& cfg) final;

private:
    storage::kvstore& _kvstore;
    std::vector<model::record_batch_type> _offset_translated_batches;
};

} // namespace kafka

template<>
struct fmt::formatter<kafka::write_at_offset_stm::errc>
  : fmt::formatter<std::string_view> {
    auto format(
      const kafka::write_at_offset_stm::errc&, fmt::format_context& ctx) const
      -> decltype(ctx.out());
};
