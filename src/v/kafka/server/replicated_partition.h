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

#include "cluster/members_table.h"
#include "cluster/partition.h"
#include "cluster/partition_probe.h"
#include "kafka/protocol/errors.h"
#include "kafka/server/partition_proxy.h"
#include "kafka/types.h"
#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "raft/errc.h"
#include "raft/types.h"

#include <seastar/core/coroutine.hh>

#include <boost/numeric/conversion/cast.hpp>

#include <memory>
#include <optional>
#include <system_error>

namespace kafka {

class replicated_partition final : public kafka::partition_proxy::impl {
public:
    explicit replicated_partition(
      ss::lw_shared_ptr<cluster::partition> p) noexcept;

    const model::ntp& ntp() const final { return _partition->ntp(); }

    ss::future<result<model::offset, error_code>>
    sync_effective_start(model::timeout_clock::duration timeout) final {
        auto synced_start_offset_override
          = co_await _partition->sync_kafka_start_offset_override(timeout);
        if (synced_start_offset_override.has_failure()) {
            auto err = synced_start_offset_override.error();
            auto error_code = error_code::unknown_server_error;
            if (err.category() == cluster::error_category()) {
                switch (cluster::errc(err.value())) {
                    /**
                     * In the case of timeout and shutting down errors return
                     * not_leader_for_partition error to force clients retry.
                     */
                case cluster::errc::shutting_down:
                case cluster::errc::not_leader:
                case cluster::errc::timeout:
                    error_code = error_code::not_leader_for_partition;
                    break;
                default:
                    error_code = error_code::unknown_server_error;
                }
            }
            co_return error_code;
        }
        co_return kafka_start_offset_with_override(
          synced_start_offset_override.value());
    }

    model::offset start_offset() const final {
        const auto start_offset_override
          = _partition->kafka_start_offset_override();
        if (!start_offset_override.has_value()) {
            return partition_kafka_start_offset();
        }
        return kafka_start_offset_with_override(start_offset_override.value());
    }

    model::offset high_watermark() const final {
        if (_partition->is_read_replica_mode_enabled()) {
            if (_partition->cloud_data_available()) {
                return _partition->next_cloud_offset();
            } else {
                return model::offset(0);
            }
        }
        return _translator->from_log_offset(_partition->high_watermark());
    }
    /**
     * According to Kafka protocol semantics a log_end_offset is an offset that
     * is assigned to the next record produced to a log
     */
    model::offset log_end_offset() const {
        if (_partition->is_read_replica_mode_enabled()) {
            if (_partition->cloud_data_available()) {
                return model::next_offset(_partition->next_cloud_offset());
            } else {
                return model::offset(0);
            }
        }
        /**
         * If a local log is empty we return start offset as this is the offset
         * assigned to the next batch produced to the log.
         */
        if (_partition->dirty_offset() < _partition->raft_start_offset()) {
            return _translator->from_log_offset(
              _partition->raft_start_offset());
        }
        /**
         * By default we return a dirty_offset + 1
         */
        return model::next_offset(
          _translator->from_log_offset(_partition->dirty_offset()));
    }

    model::offset leader_high_watermark() const {
        if (_partition->is_read_replica_mode_enabled()) {
            return high_watermark();
        }
        return _translator->from_log_offset(
          _partition->leader_high_watermark());
    }

    checked<model::offset, error_code> last_stable_offset() const final {
        if (_partition->is_read_replica_mode_enabled()) {
            if (_partition->cloud_data_available()) {
                // There is no difference between HWM and LO in this mode
                return _partition->next_cloud_offset();
            } else {
                return model::offset(0);
            }
        }
        auto maybe_lso = _partition->last_stable_offset();
        if (maybe_lso == model::invalid_lso) {
            return error_code::offset_not_available;
        }
        return _translator->from_log_offset(maybe_lso);
    }

    bool is_elected_leader() const final {
        return _partition->is_elected_leader();
    }

    bool is_leader() const final { return _partition->is_leader(); }

    ss::future<error_code>
      prefix_truncate(model::offset, ss::lowres_clock::time_point) final;

    ss::future<std::error_code> linearizable_barrier() final {
        auto r = co_await _partition->linearizable_barrier();
        if (r) {
            co_return raft::errc::success;
        }
        co_return r.error();
    }

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

    ss::future<std::vector<cluster::rm_stm::tx_range>> aborted_transactions(
      model::offset base,
      model::offset last,
      ss::lw_shared_ptr<const storage::offset_translator_state>) final;

    cluster::partition_probe& probe() final { return _partition->probe(); }

    ss::future<std::optional<model::offset>>
      get_leader_epoch_last_offset(kafka::leader_epoch) const final;

    kafka::leader_epoch leader_epoch() const final {
        return leader_epoch_from_term(_partition->term());
    }

    ss::future<error_code> validate_fetch_offset(
      model::offset, bool, model::timeout_clock::time_point) final;

    result<partition_info> get_partition_info() const final;

private:
    // Returns the Kafka offset corresponding to the lowest offset in the
    // log, including local and cloud storage. Doesn't take into account any
    // start offset overrides (see start_offset()).
    model::offset partition_kafka_start_offset() const {
        if (
          _partition->is_read_replica_mode_enabled()
          && _partition->cloud_data_available()) {
            // Always assume remote read in this case.
            return _partition->start_cloud_offset();
        }

        auto local_kafka_start_offset = _translator->from_log_offset(
          _partition->raft_start_offset());
        if (
          _partition->is_remote_fetch_enabled()
          && _partition->cloud_data_available()
          && (_partition->start_cloud_offset() < local_kafka_start_offset)) {
            return _partition->start_cloud_offset();
        }
        return local_kafka_start_offset;
    }

    model::offset kafka_start_offset_with_override(
      model::offset start_kafka_offset_override) const {
        if (start_kafka_offset_override == model::offset{}) {
            return partition_kafka_start_offset();
        }
        if (_partition->is_read_replica_mode_enabled()) {
            // The start override may fall ahead of the HWM since read replicas
            // compute HWM based on uploaded segments, and the override may
            // appear in the manifest before uploading corresponding segments.
            // Clamp down to the HWM.
            const auto hwm = high_watermark();
            if (hwm <= start_kafka_offset_override) {
                return hwm;
            }
        }
        return std::max(
          partition_kafka_start_offset(), start_kafka_offset_override);
    }

    // Returns the highest offset in the given term, without considering
    // overrides of the starting offset.
    ss::future<std::optional<model::offset>>
      get_leader_epoch_last_offset_unbounded(kafka::leader_epoch) const;

    ss::future<std::vector<cluster::rm_stm::tx_range>>
      aborted_transactions_local(
        cloud_storage::offset_range,
        ss::lw_shared_ptr<const storage::offset_translator_state>);

    ss::future<std::vector<cluster::rm_stm::tx_range>>
    aborted_transactions_remote(
      cloud_storage::offset_range offsets,
      ss::lw_shared_ptr<const storage::offset_translator_state> ot_state);

    bool may_read_from_cloud(kafka::offset);

    ss::lw_shared_ptr<cluster::partition> _partition;
    ss::lw_shared_ptr<const storage::offset_translator_state> _translator;
};

} // namespace kafka
