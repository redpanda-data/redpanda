/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "cloud_storage/types.h"
#include "kafka/protocol/errors.h"
#include "kafka/server/partition_proxy.h"
#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "raft/replicate.h"

#include <seastar/core/coroutine.hh>

#include <boost/numeric/conversion/cast.hpp>

#include <optional>
#include <system_error>

namespace kafka {

class replicated_partition final : public kafka::partition_proxy::impl {
public:
    explicit replicated_partition(
      ss::lw_shared_ptr<cluster::partition> p) noexcept;

    const model::ntp& ntp() const final;

    ss::future<result<model::offset, error_code>>
    sync_effective_start(model::timeout_clock::duration timeout) final;

    model::offset start_offset() const final;

    model::offset high_watermark() const final;

    /**
     * According to Kafka protocol semantics a log_end_offset is an offset that
     * is assigned to the next record produced to a log
     */
    model::offset log_end_offset() const;

    model::offset leader_high_watermark() const;

    checked<model::offset, error_code> last_stable_offset() const final;

    bool is_leader() const final;

    ss::future<error_code>
      prefix_truncate(model::offset, ss::lowres_clock::time_point) final;

    ss::future<std::error_code> linearizable_barrier() final;

    ss::future<std::optional<storage::timequery_result>>
    timequery(storage::timequery_config cfg) final;

    ss::future<result<model::offset>>
      replicate(model::record_batch_reader, raft::replicate_options) final;

    raft::replicate_stages replicate(
      model::batch_identity,
      model::record_batch_reader&&,
      raft::replicate_options) final;

    ss::future<storage::translating_reader> make_reader(
      storage::log_reader_config cfg,
      std::optional<model::timeout_clock::time_point>) final;

    ss::future<std::vector<model::tx_range>> aborted_transactions(
      model::offset base,
      model::offset last,
      ss::lw_shared_ptr<const storage::offset_translator_state>) final;

    cluster::partition_probe& probe() final;

    ss::future<std::optional<model::offset>>
      get_leader_epoch_last_offset(kafka::leader_epoch) const final;

    /**
     * A leader epoch is used by Kafka clients to determine if a replica is up
     * to date with the leader and to detect truncation.
     *
     * The leader epoch differs from Raft term as the term is updated when
     * leader election starts. Whereas the leader epoch is updated after the
     * state of the replica is determined. Therefore the leader epoch uses
     * confirmed term instead of the simple term which is incremented every time
     * the leader election starts.
     */
    kafka::leader_epoch leader_epoch() const final;

    ss::future<error_code> validate_fetch_offset(
      model::offset, bool, model::timeout_clock::time_point) final;

    result<partition_info> get_partition_info() const final;

private:
    // Returns the Kafka offset corresponding to the lowest offset in the
    // log, including local and cloud storage. Doesn't take into account any
    // start offset overrides (see start_offset()).
    model::offset partition_kafka_start_offset() const;

    model::offset kafka_start_offset_with_override(
      model::offset start_kafka_offset_override) const;

    // Returns the highest offset in the given term, without considering
    // overrides of the starting offset.
    ss::future<std::optional<model::offset>>
      get_leader_epoch_last_offset_unbounded(kafka::leader_epoch) const;

    ss::future<std::vector<model::tx_range>> aborted_transactions_local(
      cloud_storage::offset_range,
      ss::lw_shared_ptr<const storage::offset_translator_state>);

    ss::future<std::vector<model::tx_range>> aborted_transactions_remote(
      cloud_storage::offset_range offsets,
      ss::lw_shared_ptr<const storage::offset_translator_state> ot_state);

    bool may_read_from_cloud(kafka::offset);

    ss::lw_shared_ptr<cluster::partition> _partition;
    ss::lw_shared_ptr<const storage::offset_translator_state> _translator;
};

} // namespace kafka
